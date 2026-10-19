// Copyright 2026 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, LazyLock};

use anyhow::Context;
use futures::future::{Either, Shared, ready};
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use parking_lot::Mutex;
use prometheus::{IntCounterVec, register_int_counter_vec_with_registry};
use risingwave_common::bitmap::Bitmap;
use risingwave_common::monitor::GLOBAL_METRICS_REGISTRY;
use risingwave_common::util::panic::FutureCatchUnwindExt;
use risingwave_hummock_sdk::HummockSstableObjectId;
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use risingwave_pb::id::TableId;
use tokio::sync::{Semaphore, oneshot, watch};

use self::PinCacheRefillAttemptOutcome::{
    AlreadyPublished, CapacityRejected, NotOwned, Obsolete, Published,
};
use super::PinCacheRefillObject;
use crate::hummock::pin_cache::{PinCacheDownloadError, PinCacheReadHandle, PinCacheRefillToken};
use crate::hummock::refill_locality::{
    RefillOwnership, block_vnode_range, vnode_range_overlaps_bitmap,
};
use crate::hummock::{Sstable, SstableStoreRef};
use crate::monitor::StoreLocalStatistic;

/// Ownership rejection can retire only the publication that was checked. A stale backend
/// token says nothing about ownership of the currently published route.
enum PinCacheRefillAttemptOutcome {
    Published,
    AlreadyPublished,
    Obsolete,
    CapacityRejected,
    NotOwned(Option<PinCacheReadHandle>),
}

async fn refill_pin_cache_object(
    store: &SstableStoreRef,
    info: &SstableInfo,
    ownership: &HashMap<TableId, Bitmap>,
    token: PinCacheRefillToken,
) -> anyhow::Result<PinCacheRefillAttemptOutcome> {
    let Some(cache) = store.pin_cache() else {
        return Ok(PinCacheRefillAttemptOutcome::Obsolete);
    };
    // Work may have been revoked while waiting for the executor's permit. Skip its metadata
    // read as well as its download. Publication also rejects revocation during the download.
    if cache.prepare_refill(token.object_id()) != Some(token) {
        return Ok(PinCacheRefillAttemptOutcome::Obsolete);
    }
    let route = cache.get(token.object_id());
    let mut stats = StoreLocalStatistic::default();
    let sst = store.sstable(info, &mut stats).await;
    stats.discard();
    let sst = sst.context("pin refill ownership metadata")?;
    if !owns_object(&sst, ownership) {
        return Ok(PinCacheRefillAttemptOutcome::NotOwned(route));
    }
    drop(sst);
    // Ownership validation may await remote metadata. Do not start a whole-SST transfer for
    // work revoked during that wait; only the final publication check belongs to the backend.
    if cache.prepare_refill(token.object_id()) != Some(token) {
        return Ok(PinCacheRefillAttemptOutcome::Obsolete);
    }
    // Check the current route after ownership validation. The captured route above is only
    // for withdrawing that specific publication if the ownership check rejects it.
    if cache.get(token.object_id()).is_some() {
        return Ok(PinCacheRefillAttemptOutcome::AlreadyPublished);
    }
    let download = match cache
        .download(
            token.object_id(),
            info.file_size,
            store.store(),
            store.get_sst_data_path(token.object_id()),
        )
        .await
    {
        Ok(download) => download,
        Err(PinCacheDownloadError::CapacityRejected) => {
            return Ok(PinCacheRefillAttemptOutcome::CapacityRejected);
        }
        Err(PinCacheDownloadError::Io(error)) => {
            return Err(error).context("pin refill object copy");
        }
    };
    Ok(if cache.publish(token, download) {
        PinCacheRefillAttemptOutcome::Published
    } else {
        PinCacheRefillAttemptOutcome::Obsolete
    })
}

/// The executor has already restricted ownership to this object's admitted tables.
/// A matching block admits the complete physical object.
fn owns_object(sst: &Sstable, ownership: &HashMap<TableId, Bitmap>) -> bool {
    sst.meta
        .block_metas
        .iter()
        .enumerate()
        .any(|(index, block)| {
            let table = block.table_id();
            ownership.get(&table).is_some_and(|bitmap| {
                vnode_range_overlaps_bitmap(block_vnode_range(sst, index), bitmap)
            })
        })
}

static REFILL_OUTCOMES: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec_with_registry!(
        "pin_cache_refill_total",
        "Pin whole-object refill attempt outcomes",
        &["result"],
        &GLOBAL_METRICS_REGISTRY
    )
    .unwrap()
});

fn attempt_result_label<E, P>(
    result: &Result<Result<PinCacheRefillAttemptOutcome, E>, P>,
) -> &'static str {
    match result {
        Ok(Ok(Published)) => "published",
        Ok(Ok(AlreadyPublished)) => "already_published",
        Ok(Ok(CapacityRejected)) => "capacity_rejected",
        Ok(Ok(Obsolete | NotOwned(_))) => "obsolete",
        Ok(Err(_)) => "error",
        Err(_) => "panic",
    }
}

type Completion = Shared<oneshot::Receiver<bool>>;

struct Work {
    info: SstableInfo,
    cache_token: PinCacheRefillToken,
    ownership: Arc<HashMap<TableId, Bitmap>>,
    completion: Completion,
    completion_sender: oneshot::Sender<bool>,
}

#[derive(Default)]
struct State {
    objects: HashMap<HummockSstableObjectId, Work>,
    pending: BTreeSet<HummockSstableObjectId>,
    stopped: bool,
}

impl State {
    fn remove(&mut self, object: HummockSstableObjectId) -> Option<Work> {
        self.pending.remove(&object);
        self.objects.remove(&object)
    }

    fn insert(&mut self, object: HummockSstableObjectId, work: Work) {
        self.remove(object);
        self.pending.insert(object);
        self.objects.insert(object, work);
    }
}

/// Attempts each submitted object once under a shared concurrency limit.
/// Version deadlines only drop waiters; the worker owns unfinished copies.
/// Errors and capacity rejection finish the work without background retries.
pub(crate) struct PinCacheRefillExecutor {
    // Lock order: executor state -> PinCache shard. PinCache never calls back into this lock.
    // Keep admission checks and route revocation atomic; never hold state across await.
    state: Arc<Mutex<State>>,
    wake: watch::Sender<()>,
    store: SstableStoreRef,
}

impl PinCacheRefillExecutor {
    pub(crate) fn new(store: SstableStoreRef, concurrency: Arc<Semaphore>) -> Self {
        for result in [
            "published",
            "already_published",
            "capacity_rejected",
            "obsolete",
            "error",
            "panic",
        ] {
            let _ = REFILL_OUTCOMES.with_label_values(&[result]);
        }
        let state = Arc::new(Mutex::new(State::default()));
        let (wake, receiver) = watch::channel(());
        let limit = concurrency.available_permits().max(1);
        // Capture cleanup before spawning, so even an unpolled driver cancels its waiters.
        let cleanup = scopeguard::guard(state.clone(), |state| {
            let mut state = state.lock();
            state.stopped = true;
            state.objects.clear();
            state.pending.clear();
        });
        let worker_store = store.clone();
        let worker_state = state.clone();
        tokio::spawn(async move {
            let _cleanup = cleanup;
            Self::run(worker_store, worker_state, receiver, concurrency, limit).await;
        });
        Self { state, wake, store }
    }

    /// Submits one object after the gate has finalized its admitted tables and registered
    /// backend membership. Unregistered or currently unowned objects need no work.
    /// Equivalent live submissions share the same attempt result.
    /// Submission is synchronous; the returned future only waits for the first result.
    /// True includes published/cached and obsolete/skipped work, not a cache-hit guarantee.
    /// False means the attempt failed or was cancelled. Failures are not retried automatically.
    /// Dropping the future does not cancel work; the caller owns aggregation and deadlines.
    pub(crate) fn submit(
        &self,
        candidate: PinCacheRefillObject,
        ownership: RefillOwnership<'_>,
    ) -> impl Future<Output = bool> + Send + 'static + use<> {
        let Some(cache) = self.store.pin_cache() else {
            return Either::Left(ready(true));
        };
        let PinCacheRefillObject {
            source_sst: info,
            admitted_tables,
        } = candidate;
        let object = info.object_id;
        // Materialize current ownership before locking executor state.
        let ownership: HashMap<_, _> = admitted_tables
            .iter()
            .filter_map(|table| {
                ownership
                    .owned_vnodes(table)
                    .map(|bitmap| (*table, bitmap.into_owned()))
            })
            .collect();
        let ownership = Arc::new(ownership);
        let mut state = self.state.lock();
        if state.stopped {
            return Either::Left(ready(false));
        }
        if ownership.is_empty() {
            return Either::Left(ready(true));
        }
        let Some(current_token) = cache.prepare_refill(object) else {
            return Either::Left(ready(true));
        };
        let completion = if let Some(work) = state.objects.get(&object)
            && work.cache_token == current_token
            && work.info == info
            && work.ownership == ownership
        {
            work.completion.clone()
        } else {
            // A new ownership/metadata generation must not publish an older in-flight token.
            cache.revoke_refill(object);
            let Some(cache_token) = cache.prepare_refill(object) else {
                state.remove(object);
                return Either::Left(ready(true));
            };
            let (sender, receiver) = oneshot::channel();
            let completion = receiver.shared();
            state.insert(
                object,
                Work {
                    info,
                    cache_token,
                    ownership,
                    completion: completion.clone(),
                    completion_sender: sender,
                },
            );
            drop(state);
            self.wake.send_replace(());
            completion
        };
        Either::Right(async move { matches!(completion.await, Ok(true)) })
    }

    /// Withdraws backend membership before dropping work and completing its waiters.
    /// Running attempts retain their physical slots until they observe the revoked token.
    pub(crate) fn unregister_objects(
        &self,
        objects: impl IntoIterator<Item = HummockSstableObjectId>,
    ) {
        let objects: Vec<_> = objects.into_iter().collect();
        if objects.is_empty() {
            return;
        }
        if let Some(cache) = self.store.pin_cache() {
            cache.unregister_objects(objects.iter().copied());
        }
        let mut state = self.state.lock();
        for object in objects {
            state.remove(object);
        }
        drop(state);
        self.wake.send_replace(());
    }

    /// Cancels work whose captured policy/ownership is no longer current.
    /// Unchanged work continues. Updates never start new attempts or sweep published files.
    pub(crate) fn cancel_changed_work(
        &self,
        ownership: RefillOwnership<'_>,
        pinned_tables: &HashSet<TableId>,
    ) {
        let Some(cache) = self.store.pin_cache() else {
            return;
        };
        let mut state = self.state.lock();
        let State {
            objects, pending, ..
        } = &mut *state;
        objects.retain(|object, work| {
            // Stale work cannot revoke another admission's token.
            if cache.prepare_refill(*object) != Some(work.cache_token) {
                return false;
            }
            let unchanged = work.ownership.iter().all(|(table, bitmap)| {
                pinned_tables.contains(table)
                    && ownership.owned_vnodes(table).as_deref() == Some(bitmap)
            });
            if !unchanged {
                cache.revoke_refill(*object);
            }
            unchanged
        });
        pending.retain(|object| objects.contains_key(object));
        drop(state);
        self.wake.send_replace(());
    }

    async fn run(
        store: SstableStoreRef,
        state: Arc<Mutex<State>>,
        mut receiver: watch::Receiver<()>,
        concurrency: Arc<Semaphore>,
        limit: usize,
    ) {
        let mut tasks = FuturesUnordered::new();
        // This worker owns per-object download serialization, including across admission changes.
        // A replacement Work may already be queued while its revoked attempt is still doing I/O.
        let mut running = HashSet::new();
        loop {
            let jobs = {
                let mut state = state.lock();
                // Replacements wait for their old physical attempt, while unrelated objects
                // may use the remaining slots. Keep blocked replacements in pending.
                let jobs: Vec<_> = state
                    .pending
                    .iter()
                    .filter(|object| !running.contains(*object))
                    .take(limit - running.len())
                    .map(|object| {
                        let work = &state.objects[object];
                        (
                            *object,
                            work.info.clone(),
                            work.ownership.clone(),
                            work.cache_token,
                        )
                    })
                    .collect();
                for (object, ..) in &jobs {
                    state.pending.remove(object);
                }
                jobs
            };
            for (object, info, ownership, cache_token) in jobs {
                running.insert(object);
                let store = store.clone();
                let concurrency = concurrency.clone();
                tasks.push(async move {
                    // A panicking I/O task is a failed admission, not a permanently lost waiter.
                    let result = AssertUnwindSafe(async {
                        let _permit = concurrency
                            .acquire_owned()
                            .await
                            .expect("refill concurrency stays open");
                        refill_pin_cache_object(&store, &info, &ownership, cache_token).await
                    })
                    .rw_catch_unwind()
                    .await;
                    (object, cache_token, result)
                });
            }
            tokio::select! {
                change = receiver.changed() => if change.is_err() { break; },
                Some((object, cache_token, result)) = tasks.next(), if !tasks.is_empty() => {
                    running.remove(&object);
                    REFILL_OUTCOMES
                        .with_label_values(&[attempt_result_label(&result)])
                        .inc();
                    let mut state = state.lock();
                    if state.objects.get(&object).is_some_and(|work| work.cache_token == cache_token) {
                        let work = state.remove(object).unwrap();
                        let ready = match result {
                            Ok(Ok(Published | AlreadyPublished | Obsolete)) => true,
                            Ok(Ok(NotOwned(route))) => {
                                // Both the admission and the publication must still match.
                                if let Some(route) = route {
                                    route.invalidate();
                                }
                                true
                            },
                            Ok(Ok(CapacityRejected)) => {
                                // Cache pressure must not keep a refill admission alive. Report
                                // fallback to the version gate and let version application retire
                                // old files; a later explicit submission may attempt this object again.
                                false
                            },
                            Ok(Err(error)) => {
                                tracing::warn!(
                                    object_id = object.as_raw_id(),
                                    admission = ?cache_token,
                                    object_size = work.info.file_size,
                                    error = %format_args!("{error:#}"),
                                    "pin refill failed; using remote fallback"
                                );
                                false
                            },
                            Err(_) => {
                                tracing::warn!(
                                    object_id = object.as_raw_id(),
                                    admission = ?cache_token,
                                    object_size = work.info.file_size,
                                    "pin refill panicked; using remote fallback"
                                );
                                false
                            }
                        };
                        let _ = work.completion_sender.send(ready);
                    }
                    drop(state);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
