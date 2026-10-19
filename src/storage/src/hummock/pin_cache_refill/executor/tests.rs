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

use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use futures::FutureExt;
use futures::task::{ArcWake, waker_ref};
use risingwave_common::hash::VirtualNode;
use risingwave_common::util::epoch::test_epoch;
use risingwave_hummock_sdk::EpochWithGap;
use risingwave_hummock_sdk::compaction_group::group_split::split_sst_with_table_ids;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::SstDeltaInfo;
use risingwave_hummock_sdk::key::{FullKey, UserKey, prefix_slice_with_vnode};
use risingwave_hummock_sdk::sstable_info::SstableInfoInner;
use tokio::sync::mpsc;

use super::*;
use crate::hummock::TableHolder;
use crate::hummock::iterator::test_utils::mock_sstable_store;
use crate::hummock::local_version::pinned_version::PinnedVersion;
use crate::hummock::pin_cache::PinCache;
use crate::hummock::pin_cache::test_utils::in_memory_object_store;
use crate::hummock::pin_cache_refill::tests::{intra_level_delta, version_with_ssts};
use crate::hummock::pin_cache_refill::{PinCacheRefillPlan, PinCacheRefillPlanner};
use crate::hummock::test_utils::{default_builder_opt_for_test, gen_test_sstable_with_table_ids};
use crate::hummock::value::HummockValue;

// Fixture setup only: registration and publication remain explicit in each test.
async fn with_pin_cache(store: SstableStoreRef, capacity: u64) -> (SstableStoreRef, Arc<PinCache>) {
    let cache = PinCache::new(in_memory_object_store(), capacity, 1, [])
        .await
        .unwrap();
    let store = Arc::new(
        Arc::into_inner(store)
            .unwrap()
            .with_pin_cache(cache.clone()),
    );
    (store, cache)
}

// Sorted rows define the physical table/vnode layout; one entry per block exposes boundaries.
async fn sst_with_vnodes(
    store: &SstableStoreRef,
    object: u64,
    rows: &[(TableId, usize)],
) -> (TableHolder, SstableInfo) {
    let mut options = default_builder_opt_for_test();
    options.block_capacity = 1;
    let mut tables: Vec<_> = rows.iter().map(|(table, _)| table.as_raw_id()).collect();
    tables.dedup();
    gen_test_sstable_with_table_ids(
        options,
        object,
        rows.iter().map(|&(table, vnode)| {
            (
                FullKey {
                    user_key: UserKey::for_test(
                        table,
                        prefix_slice_with_vnode(VirtualNode::from_index(vnode), b"key"),
                    ),
                    epoch_with_gap: EpochWithGap::new_from_epoch(test_epoch(1)),
                },
                HummockValue::put(Bytes::from_static(b"value")),
            )
        }),
        store.clone(),
        tables,
    )
    .await
}

// Keep asynchronous admission assertions bounded without hiding their expected outcome.
async fn wait(waiter: impl Future<Output = bool>) -> bool {
    tokio::time::timeout(Duration::from_secs(1), waiter)
        .await
        .unwrap()
}

async fn wait_finished(executor: &PinCacheRefillExecutor) {
    tokio::time::timeout(Duration::from_secs(1), async {
        while !executor.work.lock().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_policy_and_ownership_changes_cancel_without_restarting() {
    enum Change {
        Unrelated,
        Policy,
        Ownership,
    }
    for change in [Change::Unrelated, Change::Policy, Change::Ownership] {
        let tables = [TableId::from(233), TableId::from(234)];
        let store = mock_sstable_store().await;
        let (_, info) = sst_with_vnodes(&store, 874, &[(tables[0], 0), (tables[1], 0)]).await;
        let object = info.object_id;
        let (store, cache) = with_pin_cache(store, u64::MAX).await;
        let concurrency = Arc::new(Semaphore::new(1));
        let executor = PinCacheRefillExecutor::new(store, concurrency.clone());
        cache.register_objects([object]);
        let permit = concurrency.acquire().await.unwrap();
        let ownership = HashMap::from([
            (tables[0], Bitmap::ones(VirtualNode::COUNT_FOR_TEST)),
            (
                tables[1],
                Bitmap::from_indices(VirtualNode::COUNT_FOR_TEST, [255]),
            ),
        ]);
        let ownership_view = RefillOwnership {
            streaming: Some(&ownership),
            serving: None,
        };
        let candidate = PinCacheRefillObject {
            source_sst: info,
            admitted_tables: tables.into(),
        };
        let waiter = executor.submit(candidate.clone(), ownership_view);
        let token = cache.prepare_refill(object);
        let mut next_ownership = ownership.clone();
        let mut next_pins = HashSet::from(tables);
        match change {
            Change::Unrelated => {
                let other = TableId::from(999);
                next_ownership.insert(other, Bitmap::ones(VirtualNode::COUNT_FOR_TEST));
                next_pins.insert(other);
            }
            Change::Policy => {
                next_pins.remove(&tables[0]);
            }
            Change::Ownership => {
                next_ownership.insert(
                    tables[0],
                    Bitmap::from_indices(VirtualNode::COUNT_FOR_TEST, [255]),
                );
            }
        }
        executor.cancel_changed_work(
            RefillOwnership {
                streaming: Some(&next_ownership),
                serving: None,
            },
            &next_pins,
        );
        // Restoring policy/ownership must not implicitly restart cancelled work.
        executor.cancel_changed_work(ownership_view, &tables.into());
        assert!(cache.is_registered(object), "B retains physical membership");
        if matches!(change, Change::Unrelated) {
            assert_eq!(cache.prepare_refill(object), token);
            drop(permit);
            assert!(wait(waiter).await);
        } else {
            assert_ne!(cache.prepare_refill(object), token);
            assert!(!wait(waiter).await);
            {
                let work = executor.work.lock();
                assert!(work[&object].completion.is_none());
            }
            assert!(cache.get(object).is_none());
            drop(permit);
            wait_finished(&executor).await;
            // A later explicit submission may start a fresh attempt.
            assert!(wait(executor.submit(candidate, ownership_view)).await);
        }
        assert!(cache.get(object).is_some());
    }
}

#[tokio::test]
async fn test_pin_refill_capacity_and_cached_submission() {
    for fits in [true, false] {
        let table = TableId::from(233);
        let store = mock_sstable_store().await;
        let (_, info) = sst_with_vnodes(&store, 873, &[(table, 0), (table, 128)]).await;
        let object = info.object_id;
        // Exact fit versus one byte short exercises physical file capacity accounting.
        let (store, cache) = with_pin_cache(store, info.file_size - u64::from(!fits)).await;
        cache.register_objects([object]);
        let candidate = PinCacheRefillObject {
            source_sst: info,
            admitted_tables: [table].into(),
        };
        let ownership = [(table, Bitmap::ones(VirtualNode::COUNT_FOR_TEST))].into();
        let ownership_view = RefillOwnership {
            streaming: Some(&ownership),
            serving: None,
        };
        let executor = PinCacheRefillExecutor::new(store.clone(), Arc::new(Semaphore::new(1)));
        assert_eq!(
            wait(executor.submit(candidate.clone(), ownership_view)).await,
            fits
        );
        assert_eq!(cache.get(object).is_some(), fits);
        assert!(cache.is_registered(object));
        if fits {
            let path = store.get_sst_data_path(object);
            let bytes = store.store().read(&path, ..).await.unwrap();
            store.store().delete(&path).await.unwrap();
            store.clear_meta_cache().await.unwrap();
            let token = cache.prepare_refill(object);
            // A defensive repeat admission skips the cached file, even when its new
            // nonempty ownership would reject it. This is not a gate replay fixture.
            let ownership = [(
                table,
                Bitmap::from_indices(VirtualNode::COUNT_FOR_TEST, [255]),
            )]
            .into();
            assert_eq!(
                executor
                    .submit(
                        candidate,
                        RefillOwnership {
                            streaming: Some(&ownership),
                            serving: None,
                        }
                    )
                    .now_or_never(),
                Some(true)
            );
            assert_eq!(cache.prepare_refill(object), token);
            assert_eq!(cache.get(object).unwrap().read(..).await.unwrap(), bytes);
        }
        assert!(executor.work.lock().is_empty());
    }
}

#[tokio::test]
async fn test_failed_attempt_finishes_and_explicit_resubmission_can_succeed() {
    let table = TableId::from(233);
    let store = mock_sstable_store().await;
    let (_, info) = sst_with_vnodes(&store, 870, &[(table, 0)]).await;
    let object = info.object_id;
    let path = store.get_sst_data_path(object);
    let bytes = store.store().read(&path, ..).await.unwrap();
    store.store().delete(&path).await.unwrap();
    let (store, cache) = with_pin_cache(store, u64::MAX).await;
    cache.register_objects([object]);
    let executor = PinCacheRefillExecutor::new(store.clone(), Arc::new(Semaphore::new(1)));
    let candidate = PinCacheRefillObject {
        source_sst: info,
        admitted_tables: [table].into(),
    };
    let ownership = [(table, Bitmap::ones(VirtualNode::COUNT_FOR_TEST))].into();
    let ownership_view = RefillOwnership {
        streaming: Some(&ownership),
        serving: None,
    };
    // Revocation must skip I/O even when the remote file is unavailable.
    let revoked = cache.prepare_refill(object).unwrap();
    cache.revoke_refill(object);
    assert!(matches!(
        refill_pin_cache_object(&store, &candidate.source_sst, &ownership, revoked).await,
        Ok(Obsolete)
    ));
    let first = executor.submit(candidate.clone(), ownership_view);
    let duplicate = executor.submit(candidate.clone(), ownership_view);
    assert!(!wait(first).await);
    assert!(!wait(duplicate).await);
    assert!(
        executor.work.lock().is_empty(),
        "failure leaves no retry debt"
    );
    store.store().upload(&path, bytes).await.unwrap();
    assert!(
        cache.get(object).is_none(),
        "restoring the remote file does not submit work"
    );
    assert!(wait(executor.submit(candidate, ownership_view)).await);
    assert!(cache.get(object).is_some());
}

#[tokio::test]
async fn test_cancelled_attempt_preserves_new_registration_without_restarting() {
    let table = TableId::default();
    let (store, cache) = with_pin_cache(mock_sstable_store().await, u64::MAX).await;
    let (_, info) = sst_with_vnodes(&store, 872, &[(table, 0)]).await;
    let object = info.object_id;
    cache.register_objects([object]);
    let concurrency = Arc::new(Semaphore::new(1));
    let executor = PinCacheRefillExecutor::new(store, concurrency.clone());
    let permit = concurrency.acquire().await.unwrap();
    let candidate = PinCacheRefillObject {
        source_sst: info,
        admitted_tables: [table].into(),
    };
    let ownership = [(table, Bitmap::ones(VirtualNode::COUNT_FOR_TEST))].into();
    let ownership_view = RefillOwnership {
        streaming: Some(&ownership),
        serving: None,
    };
    let waiter = executor.submit(candidate.clone(), ownership_view);
    executor.unregister_objects([object]);
    cache.register_objects([object]);
    let token = cache.prepare_refill(object);
    executor.cancel_changed_work(ownership_view, &candidate.admitted_tables);
    assert_eq!(cache.prepare_refill(object), token);
    assert!(!wait(waiter).await);
    assert!(
        !wait(executor.submit(candidate.clone(), ownership_view)).await,
        "a cancelled physical attempt still prevents an overlapping copy"
    );
    drop(permit);
    wait_finished(&executor).await;
    assert!(
        cache.get(object).is_none(),
        "cancellation never queues a replacement"
    );
    assert!(wait(executor.submit(candidate, ownership_view)).await);
}

#[cfg(not(madsim))]
#[test]
fn test_driver_shutdown_finishes_pending_and_future_submissions() {
    for (poll_driver, drop_owner) in [(false, false), (true, false), (true, true)] {
        let table = TableId::default();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let store = runtime.block_on(async {
            let (store, _) = with_pin_cache(mock_sstable_store().await, u64::MAX).await;
            store
        });
        let candidate = PinCacheRefillObject {
            source_sst: SstableInfoInner {
                object_id: 880.into(),
                file_size: 1,
                table_ids: vec![table],
                ..Default::default()
            }
            .into(),
            admitted_tables: [table].into(),
        };
        let ownership = [(table, Bitmap::ones(VirtualNode::COUNT_FOR_TEST))].into();
        let ownership_view = RefillOwnership {
            streaming: Some(&ownership),
            serving: None,
        };
        let (executor, waiter) = {
            let _entered = runtime.enter();
            let executor = PinCacheRefillExecutor::new(store, Arc::new(Semaphore::new(0)));
            // Unregistered work is a completed no-op while the driver is live.
            assert!(
                executor
                    .submit(candidate.clone(), ownership_view)
                    .now_or_never()
                    .unwrap()
            );
            assert!(executor.work.lock().is_empty());
            executor
                .store
                .pin_cache()
                .unwrap()
                .register_objects([880.into()]);
            let waiter = executor.submit(candidate.clone(), ownership_view);
            (executor, waiter)
        };
        if poll_driver {
            runtime.block_on(async {
                tokio::task::yield_now().await;
            });
        }
        if drop_owner {
            runtime.block_on(async {
                drop(executor);
                assert!(
                    !wait(waiter).await,
                    "a waiter must not keep its driver alive after the owner exits"
                );
            });
            continue;
        }
        drop(runtime);
        assert!(executor.jobs.is_closed());
        assert!(
            !waiter
                .now_or_never()
                .expect("shutdown must finish pending waiters")
        );
        assert!(
            !executor
                .submit(candidate.clone(), ownership_view)
                .now_or_never()
                .expect("a stopped driver cannot accept work")
        );
        assert!(
            !executor
                .submit(
                    candidate,
                    RefillOwnership {
                        streaming: None,
                        serving: None
                    }
                )
                .now_or_never()
                .expect("shutdown takes precedence over ownership-based skipping")
        );
    }
}

#[tokio::test]
async fn test_pin_executor_uses_shared_budget_and_targeted_removal() {
    let table = TableId::default();
    let (store, cache) = with_pin_cache(mock_sstable_store().await, u64::MAX).await;
    let mut objects = HashMap::new();
    for id in 850..853 {
        let (_, info) = sst_with_vnodes(&store, id, &[(table, 0)]).await;
        objects.insert(
            info.object_id,
            PinCacheRefillObject {
                source_sst: info,
                admitted_tables: [table].into(),
            },
        );
    }
    cache.register_objects(objects.keys().copied());
    let concurrency = Arc::new(Semaphore::new(2));
    let executor = PinCacheRefillExecutor::new(store, concurrency.clone());
    // Occupy the shared data budget with Foyer work while Pin queues its attempts.
    let foyer_permits = concurrency.acquire_many(2).await.unwrap();
    let ownership: HashMap<_, _> = [(table, Bitmap::ones(VirtualNode::COUNT_FOR_TEST))].into();
    let ownership_view = RefillOwnership {
        streaming: Some(&ownership),
        serving: None,
    };
    let mut waiters: HashMap<_, _> = objects
        .into_iter()
        .map(|(object, candidate)| (object, executor.submit(candidate, ownership_view)))
        .collect();
    let (removed, survivor) = ([850.into(), 851.into()], 852.into());
    struct WakeCount(AtomicUsize);
    impl ArcWake for WakeCount {
        fn wake_by_ref(arc_self: &Arc<Self>) {
            arc_self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
    let survivor_wakes = Arc::new(WakeCount(AtomicUsize::new(0)));
    let removed_wakes = Arc::new(WakeCount(AtomicUsize::new(0)));
    let survivor_ticket = waiters.remove(&survivor).unwrap();
    let removed_ticket = waiters.remove(&removed[0]).unwrap();
    tokio::pin!(survivor_ticket, removed_ticket);
    let survivor_waker = waker_ref(&survivor_wakes);
    let removed_waker = waker_ref(&removed_wakes);
    assert!(
        survivor_ticket
            .as_mut()
            .poll(&mut Context::from_waker(&survivor_waker))
            .is_pending()
    );
    assert!(
        removed_ticket
            .as_mut()
            .poll(&mut Context::from_waker(&removed_waker))
            .is_pending()
    );
    let survivor_token = cache.prepare_refill(survivor);
    executor.unregister_objects([]);
    executor.unregister_objects(removed);
    assert!(removed_wakes.0.load(Ordering::Relaxed) > 0);
    assert_eq!(
        survivor_wakes.0.load(Ordering::Relaxed),
        0,
        "one admission must not wake unrelated waiters"
    );
    assert_eq!(
        removed_ticket
            .as_mut()
            .poll(&mut Context::from_waker(&removed_waker)),
        Poll::Ready(false)
    );
    assert!(!cache.is_registered(removed[0]));
    assert!(
        !wait(waiters.remove(&removed[1]).unwrap()).await,
        "withdrawal must finish queued waiters without waiting for a permit"
    );
    assert!(!cache.is_registered(removed[1]));
    assert_eq!(cache.prepare_refill(survivor), survivor_token);
    drop(foyer_permits);
    assert!(
        tokio::time::timeout(Duration::from_secs(1), survivor_ticket)
            .await
            .unwrap()
    );
    assert!(cache.get(survivor).is_some());
    wait_finished(&executor).await;
    assert!(
        executor.work.lock().is_empty(),
        "completed work must not become another live-set"
    );
}

#[tokio::test]
async fn test_pin_plan_unions_logical_admission_without_widening_other_objects() {
    let tables = [TableId::from(233), TableId::from(234)];
    let store = mock_sstable_store().await;
    let mut inserted = Vec::new();
    let mut next_sst_id = 900.into();
    for object in [703, 704] {
        let (_, info) = sst_with_vnodes(&store, object, &tables.map(|table| (table, 0))).await;
        let (a, b) = split_sst_with_table_ids(&info, &mut next_sst_id, 1, 1, vec![tables[1]]);
        inserted.push(a);
        if object == 704 {
            // Retain A as the first physical descriptor, but admit B from a second reference.
            inserted.push(b);
        }
    }
    let target = version_with_ssts(&inserted);
    let mut empty = (*version_with_ssts(&[])).clone();
    empty.state_table_info = target.state_table_info.clone();
    let empty = PinnedVersion::new(empty, mpsc::unbounded_channel().0);
    let (store, cache) = with_pin_cache(store, u64::MAX).await;
    let mut planner = PinCacheRefillPlanner::new(empty.clone());
    let executor = PinCacheRefillExecutor::new(store, Arc::new(Semaphore::new(1)));
    planner.replace_pinned_tables(tables.into());
    let raw_delta = intra_level_delta(&[], &inserted, 0);
    let (refill_plan, membership_changes) = planner.advance_planning_version(
        &[SstDeltaInfo {
            insert_sst_infos: inserted,
            ..Default::default()
        }],
        target,
        Some(&[raw_delta]),
    );
    assert_eq!(
        refill_plan.objects.len(),
        2,
        "logical references share one physical candidate"
    );
    assert!(
        membership_changes
            .inserted
            .iter()
            .all(|object| !cache.is_registered(*object)),
        "planning must not register future objects"
    );
    cache.register_objects(membership_changes.inserted);
    let ownership = [(tables[1], Bitmap::ones(VirtualNode::COUNT_FOR_TEST))].into();
    for object in refill_plan.objects.into_values() {
        // Submission is eager; even its only waiter may be dropped before being polled.
        drop(executor.submit(
            object,
            RefillOwnership {
                streaming: Some(&ownership),
                serving: None,
            },
        ));
    }
    tokio::time::timeout(Duration::from_secs(1), async {
        while !executor.work.lock().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        cache.get(703.into()).is_none(),
        "only A admitted this object"
    );
    assert!(
        cache.get(704.into()).is_some(),
        "B's reference admits this object"
    );
}

#[tokio::test]
async fn test_submission_intersects_projection_admission_and_current_ownership() {
    // Keep A last so its exact last key excludes vnode 255, without a table-switch separator.
    let b = TableId::from(233);
    let a = TableId::from(234);
    for (project_b, admit_b, current_vnode, empty_bitmap) in [
        // B is owned and admitted, but outside the logical projection.
        (false, true, Some(255), false),
        // B is owned and projected, but was not admitted by the plan.
        (true, false, Some(255), false),
        // A is unowned at submission, with an absent or empty bitmap.
        (false, false, None, false),
        (false, false, None, true),
        // A owns the matching vnode at submission.
        (false, false, Some(0), false),
        // A matching block after the first A block must also admit the object.
        (false, false, Some(128), false),
    ] {
        let store = mock_sstable_store().await;
        let (_, info) = sst_with_vnodes(&store, 702, &[(b, 0), (a, 0), (a, 128)]).await;
        let object = info.object_id;
        let (store, cache) = with_pin_cache(store, u64::MAX).await;
        cache.register_objects([object]);
        let executor = PinCacheRefillExecutor::new(store, Arc::new(Semaphore::new(1)));
        let mut projection = info.get_inner();
        if !project_b {
            projection.table_ids = vec![a];
        }
        let mut plan = PinCacheRefillPlan::new(
            &[SstDeltaInfo {
                insert_sst_infos: vec![projection.into()],
                ..Default::default()
            }],
            &if admit_b { [a, b].into() } else { [a].into() },
        );
        let mut ownership = HashMap::from([(b, Bitmap::ones(VirtualNode::COUNT_FOR_TEST))]);
        if let Some(vnode) = current_vnode {
            ownership.insert(
                a,
                Bitmap::from_indices(VirtualNode::COUNT_FOR_TEST, [vnode]),
            );
        } else if empty_bitmap {
            ownership.insert(a, Bitmap::zeros(VirtualNode::COUNT_FOR_TEST));
        }
        let ownership = RefillOwnership {
            streaming: Some(&ownership),
            serving: None,
        };
        assert!(wait(executor.submit(plan.objects.remove(&object).unwrap(), ownership)).await);
        assert_eq!(
            cache.get(object).is_some(),
            matches!(current_vnode, Some(0 | 128))
        );
    }
}
