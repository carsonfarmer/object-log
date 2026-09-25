//! Runs the component's single-threaded WASI futures by polling host resources.
use std::{
    future::Future,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};
use wasi::io::poll::{self, Pollable};

type Slot = Arc<Mutex<Option<Pollable>>>;
static WAITING: Mutex<Vec<(Slot, Waker)>> = Mutex::new(Vec::new());

pub(crate) struct Subscription(Slot);

impl Drop for Subscription {
    fn drop(&mut self) {
        self.0.lock().unwrap().take();
    }
}

pub(crate) fn subscribe(pollable: Pollable, waker: Waker) -> Subscription {
    let slot = Arc::new(Mutex::new(Some(pollable)));
    WAITING.lock().unwrap().push((Arc::clone(&slot), waker));
    Subscription(slot)
}

pub(crate) fn run<T>(future: impl Future<Output = T>) -> T {
    WAITING
        .lock()
        .unwrap()
        .retain(|(slot, _)| slot.lock().unwrap().is_some());
    futures::pin_mut!(future);
    let waker = Waker::noop();
    loop {
        if let Poll::Ready(value) = future.as_mut().poll(&mut Context::from_waker(waker)) {
            return value;
        }
        let slots = std::mem::take(&mut *WAITING.lock().unwrap());
        let active = slots
            .into_iter()
            .filter_map(|(slot, waker)| {
                let pollable = slot.lock().unwrap().take();
                pollable.map(|pollable| (slot, pollable, waker))
            })
            .collect::<Vec<_>>();
        assert!(!active.is_empty(), "pending WASI operation has no pollable");
        let mut ready = vec![false; active.len()];
        for index in poll::poll(
            &active
                .iter()
                .map(|(_, pollable, _)| pollable)
                .collect::<Vec<_>>(),
        ) {
            ready[usize::try_from(index).expect("poll index fits usize")] = true;
        }
        let mut waiting = Vec::new();
        for (is_ready, (slot, pollable, waker)) in ready.into_iter().zip(active) {
            if is_ready {
                waker.wake();
            } else if Arc::strong_count(&slot) > 1 {
                *slot.lock().unwrap() = Some(pollable);
                waiting.push((slot, waker));
            }
        }
        WAITING.lock().unwrap().extend(waiting);
    }
}
