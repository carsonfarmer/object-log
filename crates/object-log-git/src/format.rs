use bytes::Bytes;
use minicbor::{Decode, Encode};

use crate::{Error, ObjectFormat, ObjectId, RefSnapshot, RefUpdate};

const VERSION: u32 = 1;
const MAX_UPDATES: usize = 1_024;
const MAX_STATE_ITEMS: usize = 100_000;

#[derive(Clone, Debug, Decode, Encode, Eq, PartialEq)]
#[cbor(array)]
pub(crate) struct PackDescriptor {
    #[n(0)]
    pub(crate) id: ObjectId,
    #[n(1)]
    pub(crate) bytes: u64,
}

#[derive(Clone, Debug, Decode, Encode)]
#[cbor(map)]
pub(crate) struct Record {
    #[n(0)]
    version: u32,
    #[n(1)]
    pub(crate) checkpoint: bool,
    #[n(2)]
    format: ObjectFormat,
    #[n(3)]
    pub(crate) refs: Vec<RefUpdate>,
    #[n(4)]
    pub(crate) packs: Vec<PackDescriptor>,
}

impl Record {
    pub(crate) fn transaction(
        format: ObjectFormat,
        mut refs: Vec<RefUpdate>,
        packs: Vec<PackDescriptor>,
    ) -> Result<Self, Error> {
        refs.sort_unstable_by(|a, b| a.name.cmp(&b.name));
        Self::new(false, format, refs, packs)
    }

    pub(crate) fn snapshot(
        format: ObjectFormat,
        refs: RefSnapshot,
        packs: Vec<PackDescriptor>,
    ) -> Result<Self, Error> {
        let refs = refs
            .into_iter()
            .map(|(name, target)| RefUpdate {
                name,
                expected: None,
                target: Some(target),
            })
            .collect();
        Self::new(true, format, refs, packs)
    }

    pub(crate) fn encode(&self) -> Result<Bytes, Error> {
        minicbor::to_vec(self)
            .map(Bytes::from)
            .map_err(|_| Error::InvalidRecord("record cannot be encoded"))
    }

    pub(crate) fn decode(
        bytes: &[u8],
        format: ObjectFormat,
        object_count: usize,
    ) -> Result<Self, Error> {
        let mut decoder = minicbor::Decoder::new(bytes);
        let record: Self = decoder
            .decode()
            .map_err(|_| Error::InvalidRecord("record cannot be decoded"))?;
        if decoder.position() != bytes.len() || record.encode().ok().as_deref() != Some(bytes) {
            return Err(Error::InvalidRecord("record is not canonical CBOR"));
        }
        if record.version != VERSION || record.format != format {
            return Err(Error::InvalidRecord(
                "version or object format does not match",
            ));
        }
        if record.packs.len() != object_count {
            return Err(Error::InvalidRecord("packs and objects are not aligned"));
        }
        record.validate()?;
        Ok(record)
    }

    pub(crate) fn into_snapshot(self) -> Result<(RefSnapshot, Vec<PackDescriptor>), Error> {
        let refs = self
            .refs
            .into_iter()
            .map(|item| {
                item.target
                    .map(|target| (item.name, target))
                    .ok_or(Error::InvalidRecord("checkpoint ref has no target"))
            })
            .collect::<Result<RefSnapshot, _>>()?;
        Ok((refs, self.packs))
    }

    fn new(
        checkpoint: bool,
        format: ObjectFormat,
        refs: Vec<RefUpdate>,
        packs: Vec<PackDescriptor>,
    ) -> Result<Self, Error> {
        let record = Self {
            version: VERSION,
            checkpoint,
            format,
            refs,
            packs,
        };
        record.validate()?;
        Ok(record)
    }

    fn validate(&self) -> Result<(), Error> {
        let limit = if self.checkpoint {
            MAX_STATE_ITEMS
        } else {
            MAX_UPDATES
        };
        if self.refs.len() > limit || (!self.checkpoint && self.refs.is_empty()) {
            return Err(Error::InvalidRecord("invalid ref count"));
        }
        if self.refs.iter().any(|item| {
            item.validate().is_err()
                || item
                    .expected
                    .into_iter()
                    .chain(item.target)
                    .any(|id| id.format() != self.format)
                || self.checkpoint && (item.expected.is_some() || item.target.is_none())
        }) {
            return Err(Error::InvalidRecord("invalid ref"));
        }
        if !self.refs.windows(2).all(|pair| pair[0].name < pair[1].name) {
            return Err(Error::InvalidRecord("refs are not ordered"));
        }
        let valid_packs = self.packs.len() <= MAX_STATE_ITEMS
            && self
                .packs
                .iter()
                .all(|pack| pack.bytes > 0 && pack.id.format() == self.format)
            && self.packs.windows(2).all(|pair| pair[0].id < pair[1].id);
        if !valid_packs {
            return Err(Error::InvalidRecord("packs are invalid or not ordered"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(value: u8) -> ObjectId {
        ObjectId(crate::Digest::Sha1([value; 20]))
    }

    fn pack(value: u8) -> PackDescriptor {
        PackDescriptor {
            id: id(value),
            bytes: 1,
        }
    }

    #[test]
    fn records_have_one_strict_encoding() -> Result<(), Error> {
        let record = Record::transaction(
            ObjectFormat::Sha1,
            vec![
                RefUpdate::new("refs/tags/v1", None, Some(id(2)))?,
                RefUpdate::new("refs/heads/main", None, Some(id(1)))?,
            ],
            vec![pack(3)],
        )?;
        let bytes = record.encode()?;
        assert_eq!(
            Record::decode(&bytes, ObjectFormat::Sha1, 1)?.encode()?,
            bytes
        );
        assert!(Record::decode(&bytes, ObjectFormat::Sha1, 0).is_err());

        let mut trailing = bytes.to_vec();
        trailing.push(0);
        assert!(Record::decode(&trailing, ObjectFormat::Sha1, 1).is_err());
        let mut noncanonical = bytes.to_vec();
        noncanonical.splice(2..3, [0x18, 0x01]);
        assert!(Record::decode(&noncanonical, ObjectFormat::Sha1, 1).is_err());
        Ok(())
    }

    #[test]
    fn duplicate_and_unordered_items_are_rejected() -> Result<(), Error> {
        let update = RefUpdate::new("refs/heads/main", None, Some(id(1)))?;
        assert!(
            Record::transaction(ObjectFormat::Sha1, vec![update.clone(), update], vec![]).is_err()
        );
        assert!(
            Record::transaction(
                ObjectFormat::Sha1,
                vec![RefUpdate::new("refs/heads/main", None, Some(id(1)))?],
                vec![pack(2), pack(2)],
            )
            .is_err()
        );
        let unordered = Record {
            version: VERSION,
            checkpoint: true,
            format: ObjectFormat::Sha1,
            refs: vec![
                RefUpdate::new("refs/tags/z", None, Some(id(1)))?,
                RefUpdate::new("refs/tags/a", None, Some(id(2)))?,
            ],
            packs: vec![],
        }
        .encode()?;
        assert!(Record::decode(&unordered, ObjectFormat::Sha1, 0).is_err());
        Ok(())
    }
}
