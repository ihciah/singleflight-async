use std::{
    borrow::Cow, cell::UnsafeCell, collections::HashMap, future::Future, ops::Deref, sync::Arc,
};

use parking_lot::{RwLock, RwLockUpgradableReadGuard};
use tokio::sync::{futures::Notified, Notify};

/// SingleFlight represents a class of work and creates a space in which units of work
/// can be executed with duplicate suppression.
#[derive(Debug)]
pub struct SingleFlight<T> {
    mapping: Arc<RwLock<HashMap<Cow<'static, str>, BroadcastOnce<T>>>>,
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

impl<'a> Deref for Key<'a> {
    type Target = Cow<'a, str>;

    fn deref(&self) -> &Self::Target {
        match self {
            Key::Static(cow) => cow,
            Key::MaybeBorrowed(cow) => cow,
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

// BroadcastOnce consists of shared slot and notify.
#[derive(Clone)]
struct BroadcastOnce<T> {
    shared: Arc<(UnsafeCell<Option<T>>, Notify)>,
}

// After calling BroadcastOnce::waiter we can get a waiter.
// It's in WaitList.
struct BroadcastOnceWaiter<T> {
    notified: Notified<'static>,
    shared: Arc<(UnsafeCell<Option<T>>, Notify)>,
}

impl<T> Default for BroadcastOnce<T> {
    fn default() -> Self {
        Self {
            shared: Arc::new((UnsafeCell::new(None), Notify::new())),
        }
    }
}

impl<T> std::fmt::Debug for BroadcastOnce<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BroadcastOnce")
    }
}

impl<T> BroadcastOnce<T> {
    fn new() -> Self {
        Self::default()
    }

    fn waiter(&self) -> BroadcastOnceWaiter<T> {
        // Leak Notify to get a Notified<'static>.
        // It's safe since Notify is behind an Arc and we hold a reference.
        let notify = unsafe { &*(&self.shared.1 as *const Notify) };
        BroadcastOnceWaiter {
            notified: notify.notified(),
            shared: self.shared.clone(),
        }
    }

    // Safety: do not call wake multiple times
    unsafe fn wake(&self, value: T) {
        *self.shared.0.get() = Some(value);
        self.shared.1.notify_waiters();
    }
}

// We already in WaitList, so wait will be fine, we won't miss
// anything after Waiter generated.
impl<T> BroadcastOnceWaiter<T> {
    // Safety: first call wake, then call wait
    async unsafe fn wait(self) -> T
    where
        T: Clone,
    {
        self.notified.await;
        (*self.shared.0.get())
            .clone()
            .expect("value not set unexpectedly")
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

    #[allow(clippy::await_holding_lock)]
    #[inline]
    fn work_inner<'a, 'b: 'a, F, Fut>(&'a self, key: Key<'b>, func: F) -> impl Future<Output = T>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = T>,
        T: Clone,
    {
        enum Either<L, R> {
            Left(L),
            Right(R),
        }

        // here the lock does not across await
        let m = self.mapping.upgradable_read();
        let val = m.get(key.deref());
        let either = match val {
            Some(call) => {
                let waiter = call.waiter();
                drop(m);
                Either::Left(waiter)
            }
            None => {
                let key: Cow<'static, str> = key.into();
                let call = BroadcastOnce::new();
                {
                    let mut m = RwLockUpgradableReadGuard::upgrade(m);
                    m.insert(key.clone(), call.clone());
                }
                Either::Right((key, func(), self.mapping.clone(), call))
            }
        };
        async move {
            match either {
                Either::Left(waiter) => unsafe { waiter.wait().await },
                Either::Right((key, fut, mapping, call)) => {
                    let output = fut.await;
                    mapping.write().remove(&key);
                    unsafe { call.wake(output.clone()) };
                    output
                }
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
}
