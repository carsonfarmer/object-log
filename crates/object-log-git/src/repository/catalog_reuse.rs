//! Optional duplicate-object reuse with a bounded physical read probe.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

use object_log::{Request, RequestDenied, RequestGuard};

use super::Repository;
use crate::{
    Error, ObjectId, durable,
    pack::{MAX_OBJECTS, Normalized, object_hash, pack_error},
};

const READS: usize = 8;
const READ_BYTES: usize = 64 * 1024;
const OBJECTS: u32 = 16;

pub(super) enum Reuse {
    KnownObjects,
    Pack(crate::catalog_tree::PackLocation),
    Stage,
}

#[derive(Debug)]
struct Probe {
    used: Mutex<(usize, usize)>,
    refused: AtomicBool,
}

impl RequestGuard for Probe {
    fn before_request(&self, request: Request) -> Result<(), RequestDenied> {
        let Request::Read { max_bytes } = request else {
            return Err(RequestDenied);
        };
        let mut used = self.used.lock().map_err(|_| RequestDenied)?;
        if used.0 == READS || max_bytes > READ_BYTES - used.1 {
            self.refused.store(true, Ordering::Relaxed);
            return Err(RequestDenied);
        }
        used.0 += 1;
        used.1 += max_bytes;
        Ok(())
    }
}

impl Repository {
    pub(super) async fn reuse_catalog_objects(&self, pack: &Normalized) -> Result<Reuse, Error> {
        if matches!(self.state.catalog, crate::state::CatalogState::Legacy) {
            return Ok(if self.state.packs.contains_key(&pack.id) {
                Reuse::KnownObjects
            } else {
                Reuse::Stage
            });
        }
        let index = gix_pack::index::File::from_data(
            pack.index.as_slice(),
            std::path::PathBuf::new(),
            object_hash(self.format),
        )
        .map_err(pack_error)?;
        let count = index.num_objects();
        if count == 0 {
            return Ok(Reuse::KnownObjects);
        }
        if count > OBJECTS {
            return Ok(Reuse::Stage);
        }
        let options = self.log.options();
        let width = (1024 * 1024).min(options.max_object_bytes);
        if width == 0 {
            return Ok(Reuse::Stage);
        }
        let chunks = pack.bytes.len().div_ceil(width);
        let root = self.log.node_size(
            pack.index.len(),
            pack.bytes.chunks(width).map(|chunk| chunk.len() as u64),
        )?;
        // Each inserted OID can change/split every bounded tree level. Include
        // plan reads for each write, and the ordinary commit/head publication.
        let nodes = count as usize * 2 * 9;
        let puts = chunks + 1 + nodes;
        let plan =
            usize::try_from(self.view.collection_plan_bytes().unwrap_or(0)).map_err(pack_error)?;
        let bounds = (|| {
            let calls = puts.checked_mul(2)?.checked_add(3 + READS + 1)?;
            let transfer = pack
                .bytes
                .len()
                .checked_add(root)?
                .checked_add(nodes.checked_mul(16 * 1024)?)?
                .checked_add(puts.checked_mul(plan)?)?
                .checked_add(options.max_commit_bytes)?
                .checked_add(options.max_head_bytes.checked_mul(3)?)?
                // Earlier guards charge the one request our own guard refuses.
                .checked_add(READ_BYTES)?
                .checked_add(options.max_object_bytes)?;
            let probe_work = options
                .max_object_bytes
                .checked_mul(2)?
                .checked_add(MAX_OBJECTS as usize * 16 * 8)?
                .checked_add(READ_BYTES * 2)?;
            let work = pack
                .bytes
                .len()
                .checked_mul(4)?
                .checked_add(root.checked_mul(4)?)?
                .checked_add(nodes.checked_mul(16 * 1024 * 4)?)?
                .checked_add(puts.checked_mul(plan)?)?
                .checked_add(probe_work)?;
            Some((calls, transfer, work))
        })();
        let Some((calls, transfer, work)) = bounds else {
            return Ok(Reuse::Stage);
        };
        if !self.operation.has_headroom(calls, transfer, work) {
            return Ok(Reuse::Stage);
        }
        let probe = Arc::new(Probe {
            used: Mutex::new((0, 0)),
            refused: AtomicBool::new(false),
        });
        let log = self.log.with_request_guard(probe.clone());
        let catalog = self.catalog().await?;
        let mut reader = durable::Reader::new(&log, &self.view, &catalog);
        let mut missing = false;
        let mut matching = None;
        for entry in index.iter() {
            let id = ObjectId::from_bytes(self.format, entry.oid.as_slice())?;
            match reader.selected_location(id).await {
                Ok(Some(location)) => {
                    if location.descriptor.id == pack.id {
                        matching = Some(location);
                    }
                }
                Ok(None) => missing = true,
                Err(Error::ObjectLog(object_log::Error::RequestDenied))
                    if probe.refused.load(Ordering::Relaxed) =>
                {
                    return Ok(matching.map_or(Reuse::Stage, Reuse::Pack));
                }
                Err(error) => return Err(error),
            }
        }
        Ok(if missing {
            matching.map_or(Reuse::Stage, Reuse::Pack)
        } else {
            Reuse::KnownObjects
        })
    }
}
