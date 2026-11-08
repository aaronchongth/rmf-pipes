//! A pass-through message for service calls: the bytes are already CDR
//! (encoded from a dynamic message), so hiroz must send them as they are and
//! hand back the response's bytes unparsed.
//!
//! hiroz's raw service client is only available with its FFI feature; this lets
//! us use its public typed client (`create_client_impl`) with an explicit
//! TypeInfo, the same way subscriptions bind to an advertised hash.

use hiroz::msg::{ZDeserializer, ZMessage, ZSerializer, ZService};
// hiroz's serializer trait speaks zenoh's internal buffer type.
use zenoh::internal::buffers::ZBuf;

/// CDR bytes, including the 4-byte encapsulation header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawCdr(pub Vec<u8>);

/// Serdes that copies [`RawCdr`] bytes verbatim.
#[derive(Debug)]
pub struct RawSerdes;

impl ZSerializer for RawSerdes {
    type Input<'a> = &'a RawCdr;

    fn serialize_to_zbuf(input: &RawCdr) -> ZBuf {
        ZBuf::from(input.0.clone())
    }

    fn serialize_to_zbuf_with_hint(input: &RawCdr, _capacity_hint: usize) -> ZBuf {
        ZBuf::from(input.0.clone())
    }

    fn serialize_to_shm(
        _input: &RawCdr,
        _estimated_size: usize,
        _provider: &zenoh::shm::ShmProvider<zenoh::shm::PosixShmProviderBackend>,
    ) -> zenoh::Result<(ZBuf, usize)> {
        Err("shared memory is not used for service calls".into())
    }

    fn serialize_to_buf(input: &RawCdr, buffer: &mut Vec<u8>) {
        buffer.clear();
        buffer.extend_from_slice(&input.0);
    }
}

/// Never fails: the bytes are decoded later against the response schema.
#[derive(Debug, thiserror::Error)]
#[error("unreachable")]
pub struct Never;

impl ZDeserializer for RawSerdes {
    type Input<'a> = &'a [u8];
    type Output = RawCdr;
    type Error = Never;

    fn deserialize(input: &[u8]) -> Result<RawCdr, Never> {
        Ok(RawCdr(input.to_vec()))
    }
}

impl ZMessage for RawCdr {
    type Serdes = RawSerdes;
}

/// A service whose request and response are [`RawCdr`].
#[derive(Debug)]
pub struct RawService;

impl ZService for RawService {
    type Request = RawCdr;
    type Response = RawCdr;
}
