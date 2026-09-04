use bytes::Bytes;
use minicbor::{Decode, Encode, encode::Write};

use crate::{PAGE_SIZE, SqliteError, wal::WAL_FRAME_HEADER_BYTES};

const WAL_HEADER_BYTES: usize = 32;
const WAL_FRAME_BYTES: usize = PAGE_SIZE as usize + WAL_FRAME_HEADER_BYTES;

#[derive(Clone, Debug, Eq, PartialEq, Decode, Encode)]
#[cbor(array)]
pub(crate) enum Record {
    #[n(0)]
    SnapshotInline(#[cbor(n(0), with = "byte_string")] Bytes),
    #[n(1)]
    SnapshotChunks {
        #[n(0)]
        len: u64,
        #[n(1)]
        count: u32,
    },
    #[n(2)]
    WalInline {
        #[cbor(n(0), with = "byte_string")]
        payload: Bytes,
        #[cbor(n(1), with = "minicbor::bytes")]
        header: [u8; WAL_HEADER_BYTES],
        #[n(2)]
        prior: u32,
        #[n(3)]
        current: u32,
    },
    #[n(3)]
    WalChunks {
        #[n(0)]
        len: u64,
        #[n(1)]
        count: u32,
        #[cbor(n(2), with = "minicbor::bytes")]
        header: [u8; WAL_HEADER_BYTES],
        #[n(3)]
        prior: u32,
        #[n(4)]
        current: u32,
    },
}

impl Record {
    pub(crate) fn snapshot(
        len: usize,
        inline: Option<Bytes>,
        chunks: usize,
    ) -> Result<Self, SqliteError> {
        let record = match inline {
            Some(payload) => Self::SnapshotInline(payload),
            None => Self::SnapshotChunks {
                len: u64::try_from(len)?,
                count: u32::try_from(chunks)?,
            },
        };
        if record.payload()?.0 != len {
            return Err(invalid("record payload length is inconsistent"));
        }
        record.validate(chunks)?;
        Ok(record)
    }

    pub(crate) fn wal(
        len: usize,
        inline: Option<Bytes>,
        chunks: usize,
        header: [u8; WAL_HEADER_BYTES],
        prior: u32,
        current: u32,
    ) -> Result<Self, SqliteError> {
        let record = match inline {
            Some(payload) => Self::WalInline {
                payload,
                header,
                prior,
                current,
            },
            None => Self::WalChunks {
                len: u64::try_from(len)?,
                count: u32::try_from(chunks)?,
                header,
                prior,
                current,
            },
        };
        if record.payload()?.0 != len {
            return Err(invalid("record payload length is inconsistent"));
        }
        record.validate(chunks)?;
        Ok(record)
    }

    pub(crate) fn encode(&self) -> Result<Bytes, SqliteError> {
        minicbor::to_vec(self).map(Bytes::from).map_err(codec_error)
    }

    pub(crate) fn decode(bytes: &[u8], objects: usize) -> Result<Self, SqliteError> {
        let record: Self = minicbor::decode(bytes).map_err(codec_error)?;
        let mut exact = Exact(bytes);
        if minicbor::encode(&record, &mut exact).is_err() || !exact.0.is_empty() {
            return Err(invalid("record is not canonical CBOR"));
        }
        record.validate(objects)?;
        Ok(record)
    }

    pub(crate) fn payload(&self) -> Result<(usize, Option<&Bytes>), SqliteError> {
        match self {
            Self::SnapshotInline(payload) | Self::WalInline { payload, .. } => {
                Ok((payload.len(), Some(payload)))
            }
            Self::SnapshotChunks { len, .. } | Self::WalChunks { len, .. } => {
                let len = usize::try_from(*len)
                    .map_err(|_| invalid("record payload length is too large"))?;
                Ok((len, None))
            }
        }
    }

    fn validate(&self, objects: usize) -> Result<(), SqliteError> {
        let (len, inline) = self.payload()?;
        let (count, unit) = match self {
            Self::SnapshotInline(_) => (0, PAGE_SIZE as usize),
            Self::SnapshotChunks { count, .. } => (usize::try_from(*count)?, PAGE_SIZE as usize),
            Self::WalInline {
                payload,
                prior,
                current,
                ..
            } => {
                validate_wal_range(u64::try_from(payload.len())?, *prior, *current)?;
                (0, WAL_FRAME_BYTES)
            }
            Self::WalChunks {
                count,
                prior,
                current,
                len,
                ..
            } => {
                validate_wal_range(*len, *prior, *current)?;
                (usize::try_from(*count)?, WAL_FRAME_BYTES)
            }
        };
        if len == 0
            || !len.is_multiple_of(unit)
            || count != objects
            || (inline.is_none() && count == 0)
        {
            return Err(invalid(
                "record payload form or object count is inconsistent",
            ));
        }
        Ok(())
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

struct Exact<'a>(&'a [u8]);

impl Write for Exact<'_> {
    type Error = ();

    fn write_all(&mut self, bytes: &[u8]) -> Result<(), Self::Error> {
        self.0 = self.0.strip_prefix(bytes).ok_or(())?;
        Ok(())
    }
}

mod byte_string {
    use bytes::Bytes;
    use minicbor::{Decoder, Encoder};

    pub(super) fn decode<C>(
        decoder: &mut Decoder<'_>,
        _: &mut C,
    ) -> Result<Bytes, minicbor::decode::Error> {
        decoder.bytes().map(Bytes::copy_from_slice)
    }

    pub(super) fn encode<C, W: minicbor::encode::Write>(
        value: &Bytes,
        encoder: &mut Encoder<W>,
        _: &mut C,
    ) -> Result<(), minicbor::encode::Error<W::Error>> {
        encoder.bytes(value).map(|_| ())
    }
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
                "82018219100001",
            ),
            (
                Record::wal(WAL_FRAME_BYTES, None, 1, [0x11; 32], 2, 3)?,
                "82038519101801582011111111111111111111111111111111111111111111111111111111111111110203",
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
        let valid = Record::snapshot(PAGE_SIZE as usize, None, 1)?.encode()?;
        let mut trailing = valid.to_vec();
        trailing.push(0);
        assert!(Record::decode(&trailing, 1).is_err());
        for bytes in [
            "8218018219100001",
            "82048219100001",
            "8201831910000100",
            "820181191000",
        ] {
            assert!(Record::decode(&hex::decode(bytes)?, 1).is_err());
        }

        for (record, objects) in [
            (
                Record::SnapshotChunks {
                    len: u64::from(PAGE_SIZE),
                    count: 0,
                },
                0,
            ),
            (
                Record::SnapshotChunks {
                    len: u64::from(PAGE_SIZE),
                    count: 1,
                },
                0,
            ),
            (Record::SnapshotChunks { len: 1, count: 1 }, 1),
            (
                Record::WalChunks {
                    len: u64::try_from(WAL_FRAME_BYTES)?,
                    count: 1,
                    header: [0; WAL_HEADER_BYTES],
                    prior: 2,
                    current: 2,
                },
                1,
            ),
        ] {
            let bytes = record.encode()?;
            assert!(Record::decode(&bytes, objects).is_err());
        }
        Ok(())
    }
}
