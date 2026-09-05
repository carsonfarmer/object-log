use bytes::Bytes;
use minicbor::{Decode, Encode};

use crate::{Error, ObjectFormat, ObjectId, RefSnapshot, RefUpdate};

const VERSION: u32 = 2;
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

#[derive(Clone, Copy, Debug, Decode, Encode, Eq, PartialEq)]
#[cbor(index_only)]
pub(crate) enum CatalogOperation {
    #[n(0)]
    LegacySnapshot,
    #[n(1)]
    TreeSnapshot,
    #[n(2)]
    Unchanged,
    #[n(3)]
    Migrate,
    #[n(4)]
    Replace,
}

#[derive(Clone, Debug, Decode, Encode, Eq, PartialEq)]
#[cbor(array)]
pub(crate) enum Metadata {
    #[n(0)]
    Unchanged,
    #[n(1)]
    Snapshot(#[cbor(n(0), with = "minicbor::bytes")] Vec<u8>),
    #[n(2)]
    Update {
        #[cbor(n(0), with = "minicbor::bytes")]
        expected: Vec<u8>,
        #[cbor(n(1), with = "minicbor::bytes")]
        target: Vec<u8>,
    },
}

impl Metadata {
    pub(crate) fn target(&self) -> Option<&[u8]> {
        match self {
            Self::Unchanged => None,
            Self::Snapshot(target) | Self::Update { target, .. } => Some(target),
        }
    }
}

pub(crate) fn validate_default_branch(name: &[u8]) -> Result<(), Error> {
    if !name.starts_with(b"refs/heads/") || !crate::is_valid_ref_name(name) {
        return Err(Error::InvalidReference);
    }
    Ok(())
}

// Borrowed legacy wire shape preserves v1 bytes without cloning record vectors.
#[derive(Encode)]
#[cbor(map)]
struct LegacyRecord<'a> {
    #[n(0)]
    version: u32,
    #[n(1)]
    checkpoint: bool,
    #[n(2)]
    format: ObjectFormat,
    #[n(3)]
    refs: &'a [RefUpdate],
    #[n(4)]
    packs: &'a [PackDescriptor],
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
    #[n(5)]
    pub(crate) catalog: Option<CatalogOperation>,
    #[n(6)]
    pub(crate) metadata: Option<Metadata>,
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
        let encoded = if self.version == 1 {
            minicbor::to_vec(LegacyRecord {
                version: self.version,
                checkpoint: self.checkpoint,
                format: self.format,
                refs: &self.refs,
                packs: &self.packs,
            })
        } else {
            minicbor::to_vec(self)
        };
        encoded
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
        if !matches!(record.version, 1 | VERSION) || record.format != format {
            return Err(Error::InvalidRecord(
                "version or object format does not match",
            ));
        }
        if if record.tree_root_operation() {
            object_count > 1
        } else {
            record.packs.len() != object_count
        } {
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
            version: 1,
            checkpoint,
            format,
            refs,
            packs,
            catalog: None,
            metadata: None,
        };
        record.validate()?;
        Ok(record)
    }

    pub(crate) fn with_metadata(mut self, metadata: Metadata) -> Result<Self, Error> {
        self.version = VERSION;
        self.catalog = Some(if self.checkpoint {
            CatalogOperation::LegacySnapshot
        } else {
            CatalogOperation::Unchanged
        });
        self.metadata = Some(metadata);
        self.validate()?;
        Ok(self)
    }

    pub(crate) fn metadata_update(
        format: ObjectFormat,
        expected: Vec<u8>,
        target: Vec<u8>,
    ) -> Result<Self, Error> {
        let record = Self {
            version: VERSION,
            checkpoint: false,
            format,
            refs: Vec::new(),
            packs: Vec::new(),
            catalog: Some(CatalogOperation::Unchanged),
            metadata: Some(Metadata::Update { expected, target }),
        };
        record.validate()?;
        Ok(record)
    }

    pub(crate) fn tree_root_operation(&self) -> bool {
        matches!(
            self.catalog,
            Some(
                CatalogOperation::TreeSnapshot
                    | CatalogOperation::Migrate
                    | CatalogOperation::Replace
            )
        )
    }

    #[cfg(test)]
    pub(crate) fn with_catalog(mut self, catalog: CatalogOperation) -> Result<Self, Error> {
        self.catalog = Some(catalog);
        self.validate()?;
        Ok(self)
    }

    #[cfg(test)]
    pub(crate) fn migration(format: ObjectFormat, default_branch: Vec<u8>) -> Result<Self, Error> {
        Self::metadata_update(format, default_branch.clone(), default_branch)?
            .with_catalog(CatalogOperation::Migrate)
    }

    fn validate(&self) -> Result<(), Error> {
        let valid_metadata = match (&self.metadata, self.checkpoint) {
            (None, _) => self.version == 1 && self.catalog.is_none(),
            (Some(Metadata::Unchanged), false) => self.version == VERSION,
            (Some(Metadata::Snapshot(target)), true) => {
                self.version == VERSION && validate_default_branch(target).is_ok()
            }
            (Some(Metadata::Update { expected, target }), false) => {
                self.version == VERSION
                    && validate_default_branch(expected).is_ok()
                    && validate_default_branch(target).is_ok()
            }
            _ => false,
        };
        let valid_catalog = match (self.catalog, self.checkpoint) {
            (None, _) => self.version == 1,
            (Some(CatalogOperation::LegacySnapshot | CatalogOperation::TreeSnapshot), true)
            | (
                Some(
                    CatalogOperation::Unchanged
                    | CatalogOperation::Migrate
                    | CatalogOperation::Replace,
                ),
                false,
            ) => self.version == VERSION,
            _ => false,
        };
        if !valid_metadata
            || !valid_catalog
            || self.tree_root_operation() && !self.packs.is_empty()
            || self.catalog == Some(CatalogOperation::Migrate)
                && (!self.refs.is_empty()
                    || matches!(&self.metadata, Some(Metadata::Update { expected, target }) if expected != target))
        {
            return Err(Error::InvalidRecord(
                "invalid metadata or catalog operation",
            ));
        }
        let limit = if self.checkpoint {
            MAX_STATE_ITEMS
        } else {
            MAX_UPDATES
        };
        if self.refs.len() > limit
            || (!self.checkpoint
                && self.refs.is_empty()
                && !matches!(self.metadata, Some(Metadata::Update { .. }))
                && self.catalog != Some(CatalogOperation::Replace))
        {
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
    fn versioned_metadata_requires_complete_canonical_operations() -> Result<(), Error> {
        let record = Record::metadata_update(
            ObjectFormat::Sha1,
            b"refs/heads/main".to_vec(),
            b"refs/heads/trunk".to_vec(),
        )?;
        let encoded = record.encode()?;
        assert_eq!(
            Record::decode(&encoded, ObjectFormat::Sha1, 0)?.encode()?,
            encoded
        );
        let mut reserved_catalog = encoded.to_vec();
        let mut decoder = minicbor::Decoder::new(&encoded);
        decoder
            .map()
            .map_err(|_| Error::InvalidRecord("test map"))?;
        for _ in 0..7 {
            let key = decoder.u8().map_err(|_| Error::InvalidRecord("test key"))?;
            if key == 5 {
                reserved_catalog[decoder.position()] = 1;
                break;
            }
            decoder
                .skip()
                .map_err(|_| Error::InvalidRecord("test value"))?;
        }
        assert!(Record::decode(&reserved_catalog, ObjectFormat::Sha1, 0).is_err());
        let mut malformed = record.clone();
        malformed.catalog = None;
        assert!(Record::decode(&malformed.encode()?, ObjectFormat::Sha1, 0).is_err());
        let mut malformed = record.clone();
        malformed.metadata = None;
        assert!(Record::decode(&malformed.encode()?, ObjectFormat::Sha1, 0).is_err());
        let mut malformed = record;
        malformed.checkpoint = true;
        assert!(Record::decode(&malformed.encode()?, ObjectFormat::Sha1, 0).is_err());
        for target in [b"HEAD".as_slice(), b"refs/tags/v1", b"refs/heads/a..b"] {
            assert!(
                Record::metadata_update(
                    ObjectFormat::Sha1,
                    b"refs/heads/main".to_vec(),
                    target.to_vec()
                )
                .is_err()
            );
        }
        Record::metadata_update(
            ObjectFormat::Sha256,
            b"refs/heads/main".to_vec(),
            b"refs/heads/non-utf8-\xff".to_vec(),
        )?;
        Ok(())
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
            version: 1,
            checkpoint: true,
            format: ObjectFormat::Sha1,
            refs: vec![
                RefUpdate::new("refs/tags/z", None, Some(id(1)))?,
                RefUpdate::new("refs/tags/a", None, Some(id(2)))?,
            ],
            packs: vec![],
            catalog: None,
            metadata: None,
        }
        .encode()?;
        assert!(Record::decode(&unordered, ObjectFormat::Sha1, 0).is_err());
        Ok(())
    }
}
