use bytes::Bytes;
use minicbor::{Decode, Encode};

use crate::{PAGE_SIZE, SqliteError, wal::WAL_FRAME_HEADER_BYTES};

const FORMAT_VERSION: u32 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RecordKind {
    Snapshot,
    Wal,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Record {
    pub(crate) kind: RecordKind,
    pub(crate) payload_len: usize,
    pub(crate) inline: Option<Bytes>,
    pub(crate) chunk_count: usize,
    pub(crate) wal_header: Option<[u8; 32]>,
    pub(crate) prior_mx_frame: Option<u32>,
    pub(crate) mx_frame: Option<u32>,
}

impl Record {
    pub(crate) fn snapshot(payload_len: usize, inline: Option<Bytes>, chunks: usize) -> Self {
        Self {
            kind: RecordKind::Snapshot,
            payload_len,
            inline,
            chunk_count: chunks,
            wal_header: None,
            prior_mx_frame: None,
            mx_frame: None,
        }
    }

    pub(crate) fn wal(
        payload_len: usize,
        inline: Option<Bytes>,
        chunks: usize,
        header: [u8; 32],
        prior: u32,
        current: u32,
    ) -> Self {
        Self {
            kind: RecordKind::Wal,
            payload_len,
            inline,
            chunk_count: chunks,
            wal_header: Some(header),
            prior_mx_frame: Some(prior),
            mx_frame: Some(current),
        }
    }

    pub(crate) fn encode(&self) -> Result<Bytes, SqliteError> {
        self.validate()?;
        let wire = RecordWire::try_from(self)?;
        minicbor::to_vec(&wire)
            .map(Bytes::from)
            .map_err(|error| SqliteError::InvalidRecord(error.to_string()))
    }

    pub(crate) fn decode(bytes: &[u8], objects: usize) -> Result<Self, SqliteError> {
        let mut decoder = minicbor::Decoder::new(bytes);
        let wire: RecordWire = decoder
            .decode()
            .map_err(|error| SqliteError::InvalidRecord(error.to_string()))?;
        if decoder.position() != bytes.len() {
            return Err(SqliteError::InvalidRecord(
                "record has trailing bytes".into(),
            ));
        }
        let canonical = minicbor::to_vec(&wire)
            .map_err(|error| SqliteError::InvalidRecord(error.to_string()))?;
        if canonical != bytes {
            return Err(SqliteError::InvalidRecord(
                "record is not canonical CBOR".into(),
            ));
        }
        if wire.version != FORMAT_VERSION {
            return Err(SqliteError::InvalidRecord(
                "record has an unsupported version".into(),
            ));
        }
        if wire.page_size != PAGE_SIZE {
            return Err(SqliteError::InvalidRecord(
                "record has a different page size".into(),
            ));
        }
        let record = Self {
            kind: match wire.kind {
                0 => RecordKind::Snapshot,
                1 => RecordKind::Wal,
                _ => {
                    return Err(SqliteError::InvalidRecord(
                        "record has an unknown kind".into(),
                    ));
                }
            },
            payload_len: usize::try_from(wire.payload_len)?,
            inline: wire.inline_payload.map(Bytes::from),
            chunk_count: usize::try_from(wire.chunk_count)?,
            wal_header: wire
                .wal_header
                .map(|header| {
                    header.try_into().map_err(|_| {
                        SqliteError::InvalidRecord("WAL header is not 32 bytes".into())
                    })
                })
                .transpose()?,
            prior_mx_frame: wire.prior_mx_frame,
            mx_frame: wire.mx_frame,
        };
        record.validate()?;
        if record.chunk_count != objects {
            return Err(SqliteError::InvalidRecord(
                "record chunk count does not match its object references".into(),
            ));
        }
        Ok(record)
    }

    fn validate(&self) -> Result<(), SqliteError> {
        if self.payload_len == 0 {
            return Err(SqliteError::InvalidRecord("record payload is empty".into()));
        }
        match (&self.inline, self.chunk_count) {
            (Some(payload), 0) if payload.len() == self.payload_len => {}
            (None, chunks) if chunks > 0 => {}
            _ => {
                return Err(SqliteError::InvalidRecord(
                    "record payload form is inconsistent".into(),
                ));
            }
        }
        let unit = match self.kind {
            RecordKind::Snapshot => {
                if self.wal_header.is_some()
                    || self.prior_mx_frame.is_some()
                    || self.mx_frame.is_some()
                {
                    return Err(SqliteError::InvalidRecord(
                        "snapshot contains WAL fields".into(),
                    ));
                }
                PAGE_SIZE as usize
            }
            RecordKind::Wal => {
                let (Some(_), Some(prior), Some(current)) =
                    (self.wal_header, self.prior_mx_frame, self.mx_frame)
                else {
                    return Err(SqliteError::InvalidRecord(
                        "WAL record lacks its boundary".into(),
                    ));
                };
                let frames = current
                    .checked_sub(prior)
                    .filter(|count| *count > 0)
                    .ok_or_else(|| {
                        SqliteError::InvalidRecord("WAL frame range is not positive".into())
                    })?;
                let expected = usize::try_from(frames)?
                    .checked_mul(PAGE_SIZE as usize + WAL_FRAME_HEADER_BYTES)
                    .ok_or_else(|| SqliteError::InvalidRecord("WAL range is too large".into()))?;
                if self.payload_len != expected {
                    return Err(SqliteError::InvalidRecord(
                        "WAL payload length does not match its frame range".into(),
                    ));
                }
                PAGE_SIZE as usize + WAL_FRAME_HEADER_BYTES
            }
        };
        if !self.payload_len.is_multiple_of(unit) {
            return Err(SqliteError::InvalidRecord(
                "record payload splits a page or WAL frame".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Decode, Encode)]
#[cbor(map)]
struct RecordWire {
    #[n(0)]
    version: u32,
    #[n(1)]
    kind: u8,
    #[n(2)]
    page_size: u32,
    #[n(3)]
    payload_len: u64,
    #[cbor(n(4), with = "minicbor::bytes")]
    inline_payload: Option<Vec<u8>>,
    #[n(5)]
    chunk_count: u32,
    #[cbor(n(6), with = "minicbor::bytes")]
    wal_header: Option<Vec<u8>>,
    #[n(7)]
    prior_mx_frame: Option<u32>,
    #[n(8)]
    mx_frame: Option<u32>,
}

impl TryFrom<&Record> for RecordWire {
    type Error = SqliteError;

    fn try_from(record: &Record) -> Result<Self, Self::Error> {
        Ok(Self {
            version: FORMAT_VERSION,
            kind: match record.kind {
                RecordKind::Snapshot => 0,
                RecordKind::Wal => 1,
            },
            page_size: PAGE_SIZE,
            payload_len: u64::try_from(record.payload_len)?,
            inline_payload: record.inline.as_ref().map(|bytes| bytes.to_vec()),
            chunk_count: u32::try_from(record.chunk_count)?,
            wal_header: record.wal_header.map(|header| header.to_vec()),
            prior_mx_frame: record.prior_mx_frame,
            mx_frame: record.mx_frame,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error as StdError;

    use bytes::Bytes;

    use super::Record;
    use crate::{PAGE_SIZE, wal::WAL_FRAME_HEADER_BYTES};

    type TestResult = Result<(), Box<dyn StdError>>;

    #[test]
    fn external_snapshot_has_stable_canonical_bytes() -> TestResult {
        let record = Record::snapshot(PAGE_SIZE as usize, None, 1);
        let encoded = record.encode()?;
        assert_eq!(hex::encode(&encoded), "a50001010002191000031910000501");
        assert_eq!(Record::decode(&encoded, 1)?, record);
        Ok(())
    }

    #[test]
    fn decoder_rejects_trailing_mixed_and_mismatched_records() -> TestResult {
        let external = Record::snapshot(PAGE_SIZE as usize, None, 1).encode()?;
        let mut trailing = external.to_vec();
        trailing.push(0);
        assert!(Record::decode(&trailing, 1).is_err());
        assert!(Record::decode(&external, 0).is_err());
        assert!(
            Record::snapshot(
                PAGE_SIZE as usize,
                Some(Bytes::from(vec![0; PAGE_SIZE as usize])),
                1,
            )
            .encode()
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn wal_range_must_be_positive_and_frame_aligned() {
        let header = [0_u8; 32];
        assert!(
            Record::wal(1, Some(Bytes::from_static(b"x")), 0, header, 0, 1)
                .encode()
                .is_err()
        );
        assert!(
            Record::wal(
                PAGE_SIZE as usize + WAL_FRAME_HEADER_BYTES,
                None,
                1,
                header,
                2,
                2,
            )
            .encode()
            .is_err()
        );
    }
}
