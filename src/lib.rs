use std::{
    borrow::Cow,
    collections::HashMap,
    future::Future,
    sync::{Arc, Weak},
};

use parking_lot::Mutex as SyncMutex;
use tokio::sync::Mutex;

type SharedMapping<T> = Arc<SyncMutex<HashMap<Cow<'static, str>, BroadcastOnce<T>>>>;

/// SingleFlight represents a class of work and creates a space in which units of work
/// can be executed with duplicate suppression.
#[derive(Debug)]
pub struct SingleFlight<T> {
    mapping: SharedMapping<T>,
}

impl<T> Default for SingleFlight<T> {
    fn default() -> Self {
        Self {
            mapping: Default::default(),
        }
    }
}

// Key is designed to avoid String clone.
enum Key<'a> {
    Static(Cow<'static, str>),
    MaybeBorrowed(Cow<'a, str>),
}

impl<'a> Key<'a> {
    #[inline]
    fn as_str(&'a self) -> &'a str {
        match self {
            Key::Static(cow) => cow.as_ref(),
            Key::MaybeBorrowed(cow) => cow.as_ref(),
        }
    }
}

impl<'a> From<Key<'a>> for Cow<'static, str> {
    fn from(k: Key<'a>) -> Self {
        match k {
            Key::Static(cow) => cow,
            Key::MaybeBorrowed(cow) => Cow::Owned(cow.into_owned()),
        }
    }
}

struct Shared<T> {
    slot: Mutex<Option<T>>,
}

impl<T> Default for Shared<T> {
    fn default() -> Self {
        Self {
            slot: Mutex::new(None),
        }
    }
}

// BroadcastOnce consists of shared slot and notify.
#[derive(Clone)]
struct BroadcastOnce<T> {
    shared: Weak<Shared<T>>,
}

impl<T> BroadcastOnce<T> {
    fn new() -> (Self, Arc<Shared<T>>) {
        let shared = Arc::new(Shared::default());
        (
            Self {
                shared: Arc::downgrade(&shared),
            },
            shared,
        )
    }
}

// After calling BroadcastOnce::waiter we can get a waiter.
// It's in WaitList.
struct BroadcastOnceWaiter<T, F> {
    func: F,
    shared: Arc<Shared<T>>,

    key: Cow<'static, str>,
    mapping: SharedMapping<T>,
}

impl<T> std::fmt::Debug for BroadcastOnce<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BroadcastOnce")
    }
}

#[allow(clippy::type_complexity)]
impl<T> BroadcastOnce<T> {
    fn try_waiter<F>(
        &self,
        func: F,
        key: Cow<'static, str>,
        mapping: SharedMapping<T>,
    ) -> Result<BroadcastOnceWaiter<T, F>, (F, Cow<'static, str>, SharedMapping<T>)> {
        let Some(upgraded) = self.shared.upgrade() else {
            return Err((func, key, mapping));
        };
        Ok(BroadcastOnceWaiter {
            func,
            shared: upgraded,
            key,
            mapping,
        })
    }

    #[inline]
    const fn waiter<F>(
        shared: Arc<Shared<T>>,
        func: F,
        key: Cow<'static, str>,
        mapping: SharedMapping<T>,
    ) -> BroadcastOnceWaiter<T, F> {
        BroadcastOnceWaiter {
            func,
            shared,
            key,
            mapping,
        }
    }
}

// We already in WaitList, so wait will be fine, we won't miss
// anything after Waiter generated.
impl<T, F, Fut> BroadcastOnceWaiter<T, F>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = T>,
    T: Clone,
{
    async fn wait(self) -> T {
        let mut slot = self.shared.slot.lock().await;
        if let Some(value) = (*slot).as_ref() {
            return value.clone();
        }

        let value = (self.func)().await;
        *slot = Some(value.clone());

        self.mapping.lock().remove(&self.key);

        value
    }
}

impl<T> SingleFlight<T> {
    /// Create a new BroadcastOnce to do work with.
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Execute and return the value for a given function, making sure that only one
    /// operation is in-flight at a given moment. If a duplicate call comes in, that caller will
    /// wait until the original call completes and return the same value.
    ///
    /// The key is a Owned key. The performance will be slightly better than `work`.
    pub fn work_with_owned_key<F, Fut>(
        &self,
        key: Cow<'static, str>,
        func: F,
    ) -> impl Future<Output = T>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = T>,
        T: Clone,
    {
        self.work_inner(Key::Static(key), func)
    }

    /// Execute and return the value for a given function, making sure that only one
    /// operation is in-flight at a given moment. If a duplicate call comes in, that caller will
    /// wait until the original call completes and return the same value.
    pub fn work<F, Fut>(&self, key: &str, func: F) -> impl Future<Output = T>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = T>,
        T: Clone,
    {
        self.work_inner(Key::MaybeBorrowed(key.into()), func)
    }

    #[inline]
    fn work_inner<'a, 'b: 'a, F, Fut>(&'a self, key: Key<'b>, func: F) -> impl Future<Output = T>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = T>,
        T: Clone,
    {
        let owned_mapping = self.mapping.clone();
        let mut mapping = self.mapping.lock();
        let val = mapping.get_mut(key.as_str());
        match val {
            Some(call) => {
                let key: Cow<'static, str> = key.into();
                let (func, key, owned_mapping) = match call.try_waiter(func, key, owned_mapping) {
                    Ok(waiter) => return waiter.wait(),
                    Err(fm) => fm,
                };
                let (new_call, shared) = BroadcastOnce::new();
                *call = new_call;
                let waiter = BroadcastOnce::waiter(shared, func, key, owned_mapping);
                waiter.wait()
            }
            None => {
                let key: Cow<'static, str> = key.into();
                let (call, shared) = BroadcastOnce::new();
                mapping.insert(key.clone(), call);
                let waiter = BroadcastOnce::waiter(shared, func, key, owned_mapping);
                waiter.wait()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::atomic::{
            AtomicUsize,
            Ordering::{AcqRel, Acquire},
        },
        time::Duration,
    };

    use futures_util::{stream::FuturesUnordered, StreamExt};

    use super::*;

    #[tokio::test]
    async fn direct_call() {
        let group = SingleFlight::new();
        let result = group
            .work("key", || async {
                tokio::time::sleep(Duration::from_millis(10)).await;
                "Result".to_string()
            })
            .await;
        assert_eq!(result, "Result");
    }

    #[tokio::test]
    async fn parallel_call() {
        let call_counter = AtomicUsize::default();

        let group = SingleFlight::new();
        let futures = FuturesUnordered::new();
        for _ in 0..10 {
            futures.push(group.work("key", || async {
                tokio::time::sleep(Duration::from_millis(100)).await;
                call_counter.fetch_add(1, AcqRel);
                "Result".to_string()
            }));
        }

        assert!(futures.all(|out| async move { out == "Result" }).await);
        assert_eq!(
            call_counter.load(Acquire),
            1,
            "future should only be executed once"
        );
    }

    #[tokio::test]
    async fn parallel_call_seq_await() {
        let call_counter = AtomicUsize::default();

        let group = SingleFlight::new();
        let mut futures = Vec::new();
        for _ in 0..10 {
            futures.push(group.work("key", || async {
                tokio::time::sleep(Duration::from_millis(100)).await;
                call_counter.fetch_add(1, AcqRel);
                "Result".to_string()
            }));
        }

        for fut in futures.into_iter() {
            assert_eq!(fut.await, "Result");
        }
        assert_eq!(
            call_counter.load(Acquire),
            1,
            "future should only be executed once"
        );
    }

    #[tokio::test]
    async fn call_with_static_str_key() {
        let group = SingleFlight::new();
        let result = group
            .work_with_owned_key("key".into(), || async {
                tokio::time::sleep(Duration::from_millis(1)).await;
                "Result".to_string()
            })
            .await;
        assert_eq!(result, "Result");
    }

    #[tokio::test]
    async fn call_with_static_string_key() {
        let group = SingleFlight::new();
        let result = group
            .work_with_owned_key("key".to_string().into(), || async {
                tokio::time::sleep(Duration::from_millis(1)).await;
                "Result".to_string()
            })
            .await;
        assert_eq!(result, "Result");
    }

    #[tokio::test]
    async fn late_wait() {
        let group = SingleFlight::new();
        let fut_early = group.work_with_owned_key("key".into(), || async {
            tokio::time::sleep(Duration::from_millis(20)).await;
            "Result".to_string()
        });
        let fut_late = group.work_with_owned_key("key".into(), || async { panic!("unexpected") });
        assert_eq!(fut_early.await, "Result");
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(fut_late.await, "Result");
    }

    #[tokio::test]
    async fn cancel() {
        let group = SingleFlight::new();

        // the executer cancelled and the other awaiter will create a new future and execute.
        let fut_cancel = group.work_with_owned_key("key".into(), || async {
            tokio::time::sleep(Duration::from_millis(2000)).await;
            "Result1".to_string()
        });
        let _ = tokio::time::timeout(Duration::from_millis(10), fut_cancel).await;
        let fut_late = group.work_with_owned_key("key".into(), || async { "Result2".to_string() });
        assert_eq!(fut_late.await, "Result2");

        // the first executer is slow but not dropped, so the result will be the first ones.
        let begin = tokio::time::Instant::now();
        let fut_1 = group.work_with_owned_key("key".into(), || async {
            tokio::time::sleep(Duration::from_millis(2000)).await;
            "Result1".to_string()
        });
        let fut_2 = group.work_with_owned_key("key".into(), || async { panic!() });
        let (v1, v2) = tokio::join!(fut_1, fut_2);
        assert_eq!(v1, "Result1");
        assert_eq!(v2, "Result1");
        assert!(begin.elapsed() > Duration::from_millis(1500));
    }
}
