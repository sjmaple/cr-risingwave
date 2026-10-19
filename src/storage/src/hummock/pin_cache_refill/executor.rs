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

use std::collections::{HashMap, HashSet};
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, LazyLock};

use anyhow::Context;
use futures::StreamExt;
use futures::future::{Either, ready};
use futures::stream::FuturesUnordered;
use parking_lot::Mutex;
use prometheus::{IntCounterVec, register_int_counter_vec_with_registry};
use risingwave_common::monitor::GLOBAL_METRICS_REGISTRY;
use risingwave_common::util::panic::FutureCatchUnwindExt;
use risingwave_hummock_sdk::HummockSstableObjectId;
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use risingwave_pb::id::TableId;
use tokio::sync::{Semaphore, mpsc, oneshot};

use self::PinCacheRefillAttemptOutcome::{CapacityRejected, Obsolete, Published};
use super::PinCacheRefillObject;
use crate::hummock::SstableStoreRef;
use crate::hummock::pin_cache::{PinCacheDownloadError, PinCacheRefillToken};

enum PinCacheRefillAttemptOutcome {
    Published,
    Obsolete,
    CapacityRejected,
}

async fn refill_pin_cache_object(
    store: &SstableStoreRef,
    info: &SstableInfo,
    token: PinCacheRefillToken,
) -> anyhow::Result<PinCacheRefillAttemptOutcome> {
    let Some(cache) = store.pin_cache() else {
        return Ok(PinCacheRefillAttemptOutcome::Obsolete);
    };
    // Admission may have been revoked while waiting for the data permit.
    if cache.prepare_refill(token.object_id()) != Some(token) {
        return Ok(PinCacheRefillAttemptOutcome::Obsolete);
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
        Ok(Ok(CapacityRejected)) => "capacity_rejected",
        Ok(Ok(Obsolete)) => "obsolete",
        Ok(Err(_)) => "error",
        Err(_) => "panic",
    }
}

struct Work {
    info: SstableInfo,
    admitted_tables: HashSet<TableId>,
    cache_token: PinCacheRefillToken,
    completion: Option<oneshot::Sender<bool>>,
}

/// Downloads each admitted object once under the shared data concurrency limit.
/// The gate loads metadata and checks ownership before submitting a download.
/// Dropping a waiter leaves its copy running; errors finish without background retries.
pub(crate) struct PinCacheRefillExecutor {
    // Lock order: executor work -> PinCache shard. Never hold work across await.
    // Cancelled entries remain until their physical attempt exits, preventing overlapping copies.
    work: Arc<Mutex<HashMap<HummockSstableObjectId, Work>>>,
    jobs: mpsc::UnboundedSender<HummockSstableObjectId>,
    store: SstableStoreRef,
}

impl PinCacheRefillExecutor {
    pub(crate) fn new(store: SstableStoreRef, concurrency: Arc<Semaphore>) -> Self {
        for result in [
            "published",
            "capacity_rejected",
            "obsolete",
            "error",
            "panic",
        ] {
            let _ = REFILL_OUTCOMES.with_label_values(&[result]);
        }
        let work = Arc::new(Mutex::new(HashMap::new()));
        let (jobs, receiver) = mpsc::unbounded_channel();
        // Capture cleanup before spawning, so even an unpolled driver cancels its waiters.
        let cleanup_work = work.clone();
        let mut receiver = scopeguard::guard(receiver, move |mut receiver| {
            receiver.close();
            cleanup_work.lock().clear();
        });
        let worker_store = store.clone();
        let worker_work = work.clone();
        tokio::spawn(async move {
            Self::run(worker_store, worker_work, &mut receiver, concurrency).await;
        });
        Self { work, jobs, store }
    }

    /// Submits a gate-deduplicated object after metadata and ownership admission.
    /// The token must have been captured before asynchronous preparation; do not recapture it.
    /// Submission is synchronous; the owned future only waits for its result.
    /// True includes published/cached and obsolete/skipped work, not a cache-hit guarantee.
    /// False means failure or cancellation, including a duplicate whose previous physical
    /// attempt has not exited. Duplicates never replace or restart work.
    /// Dropping the future does not cancel the copy; failures are not retried automatically.
    pub(crate) fn submit(
        &self,
        candidate: &PinCacheRefillObject,
        cache_token: PinCacheRefillToken,
    ) -> impl Future<Output = bool> + Send + 'static + use<> {
        let Some(cache) = self.store.pin_cache() else {
            return Either::Left(ready(true));
        };
        let object = candidate.source_sst.object_id;
        let mut work = self.work.lock();
        if self.jobs.is_closed() || work.contains_key(&object) {
            return Either::Left(ready(false));
        }
        if candidate.admitted_tables.is_empty() || cache.prepare_refill(object) != Some(cache_token)
        {
            return Either::Left(ready(true));
        }
        // Only uncached objects need an attempt. Gate admission does not revalidate or
        // withdraw existing files; this executor serializes the only runtime publisher.
        if cache.get(object).is_some() {
            return Either::Left(ready(true));
        }
        let (sender, receiver) = oneshot::channel();
        work.insert(
            object,
            Work {
                info: candidate.source_sst.clone(),
                admitted_tables: candidate.admitted_tables.clone(),
                cache_token,
                completion: Some(sender),
            },
        );
        if self.jobs.send(object).is_err() {
            work.remove(&object);
        }
        Either::Right(async move { matches!(receiver.await, Ok(true)) })
    }

    /// Withdraws backend membership before completing waiters. Physical attempts retain
    /// their entries until they exit; an old token can never publish after unregistration.
    pub(crate) fn unregister_objects(
        &self,
        objects: impl IntoIterator<Item = HummockSstableObjectId>,
    ) {
        let objects: Vec<_> = objects.into_iter().collect();
        if let Some(cache) = self.store.pin_cache() {
            cache.unregister_objects(objects.iter().copied());
        }
        let mut work = self.work.lock();
        for object in objects {
            if let Some(work) = work.get_mut(&object) {
                work.completion.take();
            }
        }
    }

    /// The gate identifies changed tables from its authoritative policy/ownership maps.
    /// Cancel only dependent downloads; updates never submit jobs or sweep published files.
    pub(crate) fn cancel_tables(&self, changed_tables: &HashSet<TableId>) {
        let Some(cache) = self.store.pin_cache() else {
            return;
        };
        let mut work = self.work.lock();
        for (object, work) in work.iter_mut() {
            if work.completion.is_none() || work.admitted_tables.is_disjoint(changed_tables) {
                continue;
            }
            // Stale work cannot revoke another registration's token.
            if cache.prepare_refill(*object) != Some(work.cache_token) {
                work.completion.take();
                continue;
            }
            cache.revoke_refill(*object);
            work.completion.take();
        }
    }

    async fn run(
        store: SstableStoreRef,
        work: Arc<Mutex<HashMap<HummockSstableObjectId, Work>>>,
        receiver: &mut mpsc::UnboundedReceiver<HummockSstableObjectId>,
        concurrency: Arc<Semaphore>,
    ) {
        let mut tasks = FuturesUnordered::new();
        loop {
            tokio::select! {
                job = receiver.recv() => {
                    let Some(object) = job else {
                        break;
                    };
                    let (info, token) = {
                        let work = work.lock();
                        let work = &work[&object];
                        (
                            work.info.clone(),
                            work.cache_token,
                        )
                    };
                    let store = store.clone();
                    let concurrency = concurrency.clone();
                    tasks.push(async move {
                        let result = AssertUnwindSafe(async {
                            let _permit = concurrency
                                .acquire_owned()
                                .await
                                .expect("refill concurrency stays open");
                            refill_pin_cache_object(&store, &info, token).await
                        })
                        .rw_catch_unwind()
                        .await;
                        (object, result)
                    });
                }
                Some((object, result)) = tasks.next(), if !tasks.is_empty() => {
                    REFILL_OUTCOMES
                        .with_label_values(&[attempt_result_label(&result)])
                        .inc();
                    let work = work.lock().remove(&object).unwrap();
                    let Some(completion) = work.completion else {
                        continue;
                    };
                    let ready = match result {
                        Ok(Ok(Published | Obsolete)) => true,
                        Ok(Ok(CapacityRejected)) => false,
                        Ok(Err(error)) => {
                            tracing::warn!(
                                object_id = object.as_raw_id(),
                                error = %format_args!("{error:#}"),
                                "pin refill failed; using remote fallback"
                            );
                            false
                        },
                        Err(_) => {
                            tracing::warn!(
                                object_id = object.as_raw_id(),
                                "pin refill panicked; using remote fallback"
                            );
                            false
                        }
                    };
                    let _ = completion.send(ready);
                }
            }
        }
    }

    #[cfg(test)]
    pub(in crate::hummock) fn has_pending_for_test(&self, object: HummockSstableObjectId) -> bool {
        self.work.lock().contains_key(&object)
    }
}

#[cfg(test)]
mod tests;
