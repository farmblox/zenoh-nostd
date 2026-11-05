use core::fmt::Display;

use zenoh_proto::{
    EitherError, TransportError,
    msgs::{NetworkMessage, NetworkMessageRef, TransportMessageRef},
};

pub trait ZTransportRx {
    fn decode_prefixed(&mut self, read: &[u8]) -> core::result::Result<(), TransportError>;
    fn decode_raw(&mut self, read: &[u8]) -> core::result::Result<(), TransportError>;

    fn decode(&mut self, read: &[u8], prefixed: bool) -> core::result::Result<(), TransportError> {
        if prefixed {
            self.decode_prefixed(read)
        } else {
            self.decode_raw(read)
        }
    }

    fn decode_prefixed_with<E>(
        &mut self,
        read: impl FnMut(&mut [u8]) -> core::result::Result<usize, E>,
    ) -> core::result::Result<(), EitherError<TransportError, E>>
    where
        E: Display;

    fn decode_raw_with<E>(
        &mut self,
        read: impl FnMut(&mut [u8]) -> core::result::Result<usize, E>,
    ) -> core::result::Result<(), EitherError<TransportError, E>>
    where
        E: Display;

    fn decode_with<E>(
        &mut self,
        read: impl FnMut(&mut [u8]) -> core::result::Result<usize, E>,
        prefixed: bool,
    ) -> core::result::Result<(), EitherError<TransportError, E>>
    where
        E: Display,
    {
        if prefixed {
            self.decode_prefixed_with(read)
        } else {
            self.decode_raw_with(read)
        }
    }

    fn decode_prefixed_with_async<E>(
        &mut self,
        read: impl AsyncFnMut(&mut [u8]) -> core::result::Result<usize, E>,
    ) -> impl Future<Output = core::result::Result<(), EitherError<TransportError, E>>>
    where
        E: Display;

    fn decode_raw_with_async<E>(
        &mut self,
        read: impl AsyncFnMut(&mut [u8]) -> core::result::Result<usize, E>,
    ) -> impl Future<Output = core::result::Result<(), EitherError<TransportError, E>>>
    where
        E: Display;

    fn decode_with_async<E>(
        &mut self,
        read: impl AsyncFnMut(&mut [u8]) -> core::result::Result<usize, E>,
        prefixed: bool,
    ) -> impl Future<Output = core::result::Result<(), EitherError<TransportError, E>>>
    where
        E: Display,
    {
        async move {
            if prefixed {
                self.decode_prefixed_with_async(read).await
            } else {
                self.decode_raw_with_async(read).await
            }
        }
    }

    fn flush(&mut self) -> impl Iterator<Item = (NetworkMessage<'_>, &'_ [u8])>;
    /// Discard the current batch and any incomplete fragmented messages.
    fn clear(&mut self);
}

pub trait ZTransportTx {
    fn keepalive(&mut self) -> Result<(), TransportError>;
    fn close(&mut self) -> Result<(), TransportError>;
    fn transport(&mut self, msg: TransportMessageRef<'_>) -> Result<(), TransportError>;

    /// Encode one complete network message, optionally reusing its verified
    /// wire bytes. A full batch must be drained before encoding another message.
    /// Messages beyond the backing buffer's capacity fail before publication.
    fn encode(
        &mut self,
        msg: NetworkMessageRef<'_>,
        bytes: Option<&[u8]>,
    ) -> Result<(), TransportError>;

    /// Return the next batch or fragment. Drain until `None` before reusing the
    /// buffer; each borrow stays valid until the next mutable transport access.
    fn flush(&mut self, prefixed: bool) -> Result<Option<&[u8]>, TransportError>;
    fn clear(&mut self);
}
