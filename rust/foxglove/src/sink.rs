use std::cell::OnceCell;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use smallvec::SmallVec;

use crate::metadata::Metadata;
use crate::{ChannelId, FoxgloveError, RawChannel};

/// A log message payload shared by reference across every [`Sink`] subscribed to a channel for a
/// single [`RawChannel::log_to_sinks`] call.
///
/// [`RawChannel::log_to_sinks`] iterates its sinks synchronously, on the caller's thread (usually
/// the application's hot logging thread), invoking [`Sink::log_shared`] once per sink with the
/// *same* underlying byte slice. Most sinks only need a borrow of that slice (see
/// [`SharedLogPayload::as_slice`]), but sinks that hand the payload off to another thread (e.g. an
/// async writer task) need an owned, refcounted copy that outlives the call.
///
/// Rather than have every such sink pay for its own `Vec<u8>`/`Bytes` copy of the payload,
/// `SharedLogPayload` lazily materializes a single [`Bytes`] copy the first time
/// [`SharedLogPayload::shared_bytes`] is called, and every subsequent call (from another sink,
/// still within the same `log_to_sinks` invocation) just clones the `Bytes` handle, which is a
/// cheap refcount bump rather than a memcpy.
///
/// This type is not `Sync`/`Send` and is only ever used within the single synchronous loop in
/// `LogSinkSet::for_each`/`for_each_filtered`, so the laziness does not need to be thread-safe.
pub struct SharedLogPayload<'a> {
    data: &'a [u8],
    shared: OnceCell<Bytes>,
}

impl<'a> SharedLogPayload<'a> {
    /// Wraps a borrowed payload slice for fan-out to multiple sinks.
    pub(crate) fn new(data: &'a [u8]) -> Self {
        Self {
            data,
            shared: OnceCell::new(),
        }
    }

    /// Returns the payload as a borrowed slice, with no copying.
    pub fn as_slice(&self) -> &[u8] {
        self.data
    }

    /// Returns an owned, cheaply-cloneable [`Bytes`] view of the payload.
    ///
    /// The first call across all sinks sharing this instance copies the payload once; every other
    /// call (including repeated calls from the same sink) just bumps a refcount.
    pub fn shared_bytes(&self) -> Bytes {
        self.shared
            .get_or_init(|| Bytes::copy_from_slice(self.data))
            .clone()
    }
}

/// Uniquely identifies a [`Sink`] in the context of this program.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SinkId(NonZeroU64);
impl SinkId {
    /// Returns a new SinkId
    pub fn new(id: NonZeroU64) -> Self {
        Self(id)
    }

    /// Allocates the next sink ID.
    pub fn next() -> Self {
        static NEXT_ID: AtomicU64 = AtomicU64::new(1);
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        // SAFETY: NEXT_ID starts at 1 and only increments, so it's never zero
        let non_zero_id = unsafe { NonZeroU64::new_unchecked(id) };
        Self::new(non_zero_id)
    }
}
impl std::fmt::Display for SinkId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<SinkId> for u64 {
    fn from(id: SinkId) -> Self {
        id.0.get()
    }
}

impl From<SinkId> for NonZeroU64 {
    fn from(id: SinkId) -> Self {
        id.0
    }
}

/// A [`Sink`] writes a message from a channel to a destination.
///
/// Sinks are thread-safe and can be shared between threads. Usually you'd use our implementations
/// like [`McapWriter`](crate::McapWriter) or [`WebSocketServer`](crate::WebSocketServer).
#[doc(hidden)]
pub trait Sink: Send + Sync {
    /// Returns the sink's unique ID.
    fn id(&self) -> SinkId;

    /// Writes the message for the channel to the sink.
    ///
    /// Metadata contains optional message metadata that may be used by some sink implementations.
    fn log(
        &self,
        channel: &RawChannel,
        msg: &[u8],
        metadata: &Metadata,
    ) -> Result<(), FoxgloveError>;

    /// Writes the message for the channel to the sink, given a payload shared across all sinks
    /// for this call.
    ///
    /// This exists so that sinks which need to retain an owned copy of the payload past the end
    /// of the call (for example, to hand it off to an async task) can obtain one cheaply via
    /// [`SharedLogPayload::shared_bytes`] without forcing a redundant copy when multiple such
    /// sinks are subscribed to the same channel (e.g. multiple connected WebSocket clients). The
    /// default implementation just forwards to [`Sink::log`], which is correct for sinks that only
    /// need a borrow of the payload for the duration of the call.
    #[doc(hidden)]
    fn log_shared(
        &self,
        channel: &RawChannel,
        msg: &SharedLogPayload<'_>,
        metadata: &Metadata,
    ) -> Result<(), FoxgloveError> {
        self.log(channel, msg.as_slice(), metadata)
    }

    /// Called when new channels are made available within the [`Context`][ctx].
    ///
    /// Sinks can track channels seen, and do new channel-related things the first time they see a
    /// channel, rather than in this method. The choice is up to the implementor.
    ///
    /// When the sink is first registered with a context, this callback is automatically invoked
    /// with each of the channels registered to that context.
    ///
    /// For sinks that manage their channel subscriptions dynamically, note that it is NOT safe to
    /// call [`Context::subscribe_channels`][sub] from the context of this callback. If the sink
    /// wants to subscribe to channels immediately, it may return a list of corresponding channel
    /// IDs.
    ///
    /// For sinks that [auto-subscribe][Sink::auto_subscribe] to all channels, the return value of
    /// this method is ignored.
    ///
    /// [ctx]: crate::Context
    /// [sub]: crate::Context::subscribe_channels
    fn add_channels(&self, _channel: &[&Arc<RawChannel>]) -> Option<Vec<ChannelId>> {
        None
    }

    /// Called when a new channel is made available within the [`Context`][crate::Context].
    ///
    /// See [`Sink::add_channels`] for additional details.
    ///
    /// For sinks that manage their channel subscriptions dynamically, this function may return
    /// true to immediately subscribe to the channel.
    #[doc(hidden)]
    fn add_channel(&self, channel: &Arc<RawChannel>) -> bool {
        self.add_channels(&[channel])
            .is_some_and(|ids| ids.contains(&channel.id()))
    }

    /// Called when a channel is unregistered from the [`Context`][ctx].
    ///
    /// Sinks can clean up any channel-related state they have or take other actions.
    ///
    /// For sinks that manage their channel subscriptions dynamically, it is not necessary to call
    /// [`Context::unsubscribe_channels`][unsub] for this sink; subscriptions for a channel are
    /// automatically removed when that channel is removed.
    ///
    /// [ctx]: crate::Context
    /// [unsub]: crate::Context::unsubscribe_channels
    fn remove_channel(&self, _channel: &RawChannel) {}

    /// Indicates whether this sink automatically subscribes to all channels.
    ///
    /// The default implementation returns true.
    ///
    /// A sink implementation may return false to indicate that it intends to manage its
    /// subscriptions dynamically using [`Sink::add_channel`],
    /// [`Context::subscribe_channels`][sub], and [`Context::unsubscribe_channels`][unsub].
    ///
    /// [sub]: crate::Context::subscribe_channels
    /// [unsub]: crate::Context::unsubscribe_channels
    fn auto_subscribe(&self) -> bool {
        true
    }
}

/// A small group of sinks.
///
/// We use a [`SmallVec`] to improve cache locality and reduce heap allocations when working with a
/// small number of sinks, which is typically the case.
pub(crate) type SmallSinkVec = SmallVec<[Arc<dyn Sink>; 6]>;

#[cfg(test)]
mod tests {
    use super::*;

    /// Demonstrates that `shared_bytes()` only copies the payload once, no matter how many times
    /// it's called (i.e. no matter how many sinks are fanned out to for a given log call): every
    /// call after the first returns a `Bytes` clone that's a refcount bump pointing at the exact
    /// same backing allocation, not a fresh copy.
    #[test]
    fn shared_log_payload_materializes_once() {
        let data = vec![0x42u8; 4096];
        let payload = SharedLogPayload::new(&data);

        let first = payload.shared_bytes();
        assert_eq!(first.as_ref(), data.as_slice());

        // Simulate fanning out to N additional "clients".
        for _ in 0..9 {
            let next = payload.shared_bytes();
            assert_eq!(next.as_ref(), data.as_slice());
            // Same backing allocation as the first materialization: this is a refcount clone, not
            // a memcpy of the payload.
            assert_eq!(next.as_ptr(), first.as_ptr());
        }
    }

    #[test]
    fn shared_log_payload_as_slice_never_copies() {
        let data = vec![0x7fu8; 64];
        let payload = SharedLogPayload::new(&data);
        // Borrowing the slice directly should never require materializing an owned copy.
        assert_eq!(payload.as_slice().as_ptr(), data.as_ptr());
    }
}
