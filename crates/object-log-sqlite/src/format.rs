use bytes::Bytes;
use minicbor::{Decode, Encode, Encoder, encode::Write};

use crate::{PAGE_SIZE, SqliteError, wal::WAL_FRAME_HEADER_BYTES};

const FORMAT_VERSION: u32 = 1;
const WAL_HEADER_BYTES: usize = 32;
const WAL_FRAME_BYTES: usize = PAGE_SIZE as usize + WAL_FRAME_HEADER_BYTES;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Record {
    kind: RecordKind,
    payload: Payload,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum RecordKind {
    Snapshot,
    Wal {
        header: [u8; WAL_HEADER_BYTES],
        prior: u32,
        current: u32,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Payload {
    Inline(Bytes),
    Chunks { len: usize, count: u32 },
}

impl Record {
    pub(crate) fn snapshot(
        len: usize,
        inline: Option<Bytes>,
        chunks: usize,
    ) -> Result<Self, SqliteError> {
        Ok(Self {
            kind: RecordKind::Snapshot,
            payload: Payload::new(len, inline, chunks, PAGE_SIZE as usize)?,
        })
    }

    pub(crate) fn wal(
        len: usize,
        inline: Option<Bytes>,
        chunks: usize,
        header: [u8; WAL_HEADER_BYTES],
        prior: u32,
        current: u32,
    ) -> Result<Self, SqliteError> {
        validate_wal_range(u64::try_from(len)?, prior, current)?;
        Ok(Self {
            kind: RecordKind::Wal {
                header,
                prior,
                current,
            },
            payload: Payload::new(len, inline, chunks, WAL_FRAME_BYTES)?,
        })
    }

    pub(crate) fn encode(&self) -> Result<Bytes, SqliteError> {
        minicbor::to_vec(self.wire()?)
            .map(Bytes::from)
            .map_err(codec_error)
    }

    pub(crate) fn decode(bytes: &[u8], objects: usize) -> Result<Self, SqliteError> {
        let wire: RecordWire<'_> = minicbor::decode(bytes).map_err(codec_error)?;
        if !is_canonical(&wire, bytes) {
            return Err(invalid("record is not canonical CBOR"));
        }
        if wire.version != FORMAT_VERSION || wire.page_size != PAGE_SIZE {
            return Err(invalid("record has an unsupported version or page size"));
        }

        let kind = match (
            wire.kind,
            wire.wal_header,
            wire.prior_mx_frame,
            wire.mx_frame,
        ) {
            (0, None, None, None) => RecordKind::Snapshot,
            (1, Some(header), Some(prior), Some(current)) => {
                validate_wal_range(wire.payload_len, prior, current)?;
                RecordKind::Wal {
                    header: header
                        .try_into()
                        .map_err(|_| invalid("WAL header is not 32 bytes"))?,
                    prior,
                    current,
                }
            }
            (0, ..) => return Err(invalid("snapshot contains WAL fields")),
            (1, ..) => return Err(invalid("WAL record lacks its boundary")),
            _ => return Err(invalid("record has an unknown kind")),
        };
        let unit = match kind {
            RecordKind::Snapshot => PAGE_SIZE as usize,
            RecordKind::Wal { .. } => WAL_FRAME_BYTES,
        };
        Ok(Self {
            kind,
            payload: Payload::decode(&wire, objects, unit)?,
        })
    }

    pub(crate) const fn kind(&self) -> &RecordKind {
        &self.kind
    }

    pub(crate) const fn payload_len(&self) -> usize {
        match &self.payload {
            Payload::Inline(bytes) => bytes.len(),
            Payload::Chunks { len, .. } => *len,
        }
    }

    pub(crate) const fn inline(&self) -> Option<&Bytes> {
        match &self.payload {
            Payload::Inline(bytes) => Some(bytes),
            Payload::Chunks { .. } => None,
        }
    }

    fn wire(&self) -> Result<RecordWire<'_>, SqliteError> {
        let (inline_payload, chunk_count) = match &self.payload {
            Payload::Inline(bytes) => (Some(bytes.as_ref()), None),
            Payload::Chunks { count, .. } => (None, Some(*count)),
        };
        let (kind, wal_header, prior_mx_frame, mx_frame) = match &self.kind {
            RecordKind::Snapshot => (0, None, None, None),
            RecordKind::Wal {
                header,
                prior,
                current,
            } => (1, Some(header.as_slice()), Some(*prior), Some(*current)),
        };
        Ok(RecordWire {
            version: FORMAT_VERSION,
            kind,
            page_size: PAGE_SIZE,
            payload_len: u64::try_from(self.payload_len())?,
            inline_payload,
            chunk_count,
            wal_header,
            prior_mx_frame,
            mx_frame,
        })
    }
}

impl Payload {
    fn new(
        len: usize,
        inline: Option<Bytes>,
        chunks: usize,
        unit: usize,
    ) -> Result<Self, SqliteError> {
        if len == 0 || !len.is_multiple_of(unit) {
            return Err(invalid("record payload is empty or misaligned"));
        }
        match (inline, chunks) {
            (Some(bytes), 0) if bytes.len() == len => Ok(Self::Inline(bytes)),
            (None, count) if count > 0 => Ok(Self::Chunks {
                len,
                count: u32::try_from(count)?,
            }),
            _ => Err(invalid("record payload form is inconsistent")),
        }
    }

    fn decode(wire: &RecordWire<'_>, objects: usize, unit: usize) -> Result<Self, SqliteError> {
        let len = usize::try_from(wire.payload_len)
            .map_err(|_| invalid("record payload length is too large"))?;
        if len == 0 || !len.is_multiple_of(unit) {
            return Err(invalid("record payload is empty or misaligned"));
        }
        match (wire.inline_payload, wire.chunk_count) {
            (Some(bytes), None) if objects == 0 && bytes.len() == len => {
                Ok(Self::Inline(Bytes::copy_from_slice(bytes)))
            }
            (None, Some(count)) if count > 0 && usize::try_from(count).ok() == Some(objects) => {
                Ok(Self::Chunks { len, count })
            }
            _ => Err(invalid(
                "record payload form or object count is inconsistent",
            )),
        }
    }
}

fn validate_wal_range(len: u64, prior: u32, current: u32) -> Result<(), SqliteError> {
    let frames = current
        .checked_sub(prior)
        .filter(|count| *count > 0)
        .ok_or_else(|| invalid("WAL frame range is not positive"))?;
    if u64::from(frames).checked_mul(u64::try_from(WAL_FRAME_BYTES)?) != Some(len) {
        return Err(invalid("WAL payload length does not match its frame range"));
    }
    Ok(())
}

#[derive(Clone, Copy, Decode, Encode)]
#[cbor(map)]
struct RecordWire<'a> {
    #[n(0)]
    version: u32,
    #[n(1)]
    kind: u8,
    #[n(2)]
    page_size: u32,
    #[n(3)]
    payload_len: u64,
    #[cbor(n(4), with = "minicbor::bytes")]
    inline_payload: Option<&'a [u8]>,
    #[n(5)]
    chunk_count: Option<u32>,
    #[cbor(n(6), with = "minicbor::bytes")]
    wal_header: Option<&'a [u8]>,
    #[n(7)]
    prior_mx_frame: Option<u32>,
    #[n(8)]
    mx_frame: Option<u32>,
}

struct Exact<'a>(&'a [u8]);

impl Write for Exact<'_> {
    type Error = ();

    fn write_all(&mut self, bytes: &[u8]) -> Result<(), Self::Error> {
        self.0 = self.0.strip_prefix(bytes).ok_or(())?;
        Ok(())
    }
}

fn is_canonical(value: &RecordWire<'_>, bytes: &[u8]) -> bool {
    let mut exact = Exact(bytes);
    value.encode(&mut Encoder::new(&mut exact), &mut ()).is_ok() && exact.0.is_empty()
}

fn invalid(message: &str) -> SqliteError {
    SqliteError::InvalidRecord(message.into())
}

fn codec_error(error: impl std::fmt::Display) -> SqliteError {
    SqliteError::InvalidRecord(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::error::Error as StdError;

    use super::*;

    type TestResult = Result<(), Box<dyn StdError>>;

    #[test]
    fn snapshot_and_wal_have_stable_canonical_bytes() -> TestResult {
        let records = [
            (
                Record::snapshot(PAGE_SIZE as usize, None, 1)?,
                "a50001010002191000031910000501",
            ),
            (
                Record::wal(WAL_FRAME_BYTES, None, 1, [0x11; 32], 2, 3)?,
                "a80001010102191000031910180501065820111111111111111111111111111111111111111111111111111111111111111107020803",
            ),
        ];
        for (record, golden) in records {
            let bytes = record.encode()?;
            assert_eq!(hex::encode(&bytes), golden);
            assert_eq!(Record::decode(&bytes, 1)?, record);
        }
        Ok(())
    }

    #[test]
    fn decoder_rejects_invalid_record_classes() -> TestResult {
        for (bytes, objects) in [
            ("a600010100021910000319100005010900", 1),
            ("a5001801010002191000031910000501", 1),
            ("a5000101000219100003191000050100", 1),
            ("a50001010002191000031910000501", 0),
            ("a60001010002191000031910000441000501", 1),
            ("a50001010002192000031910000501", 1),
            ("a5000101000219100003000501", 1),
            ("a5000101000219100003010501", 1),
            (
                "a80001010002191000031910000501065820000000000000000000000000000000000000000000000000000000000000000007000801",
                1,
            ),
            (
                "a80001010102191000031910180501065820000000000000000000000000000000000000000000000000000000000000000007020802",
                1,
            ),
        ] {
            let bytes = hex::decode(bytes)?;
            assert!(Record::decode(&bytes, objects).is_err());
        }
        Ok(())
    }
}
