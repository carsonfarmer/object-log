use super::{Bytes, Error, ObjectId, Repository, durable, size_of};

impl Repository {
    pub(super) async fn uri_locations(
        &self,
        graph: &crate::graph::Graph,
        reader: &mut durable::Reader<'_>,
        ids: &mut Vec<ObjectId>,
        uris: Option<&crate::PackfileUris>,
    ) -> Result<Vec<(ObjectId, String)>, Error> {
        let mut locations = Vec::new();
        if let Some(base) = uris {
            for id in &*ids {
                if locations.len() == crate::packfile_uri::MAX_URIS {
                    break;
                }
                let node =
                    &graph.nodes[graph.location(*id).ok_or(Error::InvalidReference)? as usize];
                if node.kind != Some(gix_object::Kind::Blob)
                    || !crate::PackfileUris::eligible(
                        reader
                            .object_size(*id)
                            .await?
                            .ok_or(Error::InvalidReference)?,
                    )
                {
                    continue;
                }
                let object = reader.find(*id).await?.ok_or(Error::InvalidReference)?;
                if object.kind != gix_object::Kind::Blob {
                    return Err(Error::InvalidReference);
                }
                let (checksum, _pack) =
                    crate::packfile_uri::canonical(&self.operation, self.format, &object.data)?;
                locations.push((*id, checksum, base.uri(*id, checksum)));
            }
            ids.retain(|id| !locations.iter().any(|(blob, _, _)| blob == id));
        }
        let locations = locations
            .into_iter()
            .map(|(_, checksum, uri)| (checksum, uri))
            .collect::<Vec<_>>();
        Ok(locations)
    }

    /// Reconstructs an exact URI pack after checking current blob reachability.
    /// The caller must authenticate the HTTP request independently. No URI grants
    /// retention or bypasses the current repository view.
    ///
    /// # Errors
    /// Rejects unreachable/non-blob IDs, mismatched checksums and exhausted limits.
    /// An expired-view retry shares the original operation's counters.
    pub async fn fetch_uri_pack(self, blob: ObjectId, checksum: ObjectId) -> Result<Bytes, Error> {
        if blob.format() != self.format || checksum.format() != self.format {
            return Err(Error::InvalidObjectId);
        }
        match self.uri_attempt(blob, checksum).await {
            Err(Error::ObjectLog(object_log::Error::ViewExpired)) => {
                let operation = self.operation.clone();
                let (log, format) = (self.log.clone(), self.format);
                drop(self);
                operation.retry()?;
                Self::open_attempt(&log, format, &operation)
                    .await?
                    .uri_attempt(blob, checksum)
                    .await
            }
            result => result,
        }
    }

    async fn uri_attempt(&self, blob: ObjectId, checksum: ObjectId) -> Result<Bytes, Error> {
        let catalog = self.catalog().await?;
        let mut reader = durable::Reader::new(&self.log, &self.view, &catalog);
        let _roots = self
            .operation
            .reserve_state(self.state.refs.len() * size_of::<ObjectId>())?;
        let roots = self.state.refs.values().copied().collect::<Vec<_>>();
        let graph = crate::graph::Graph::load(&self.operation, &mut reader, &roots).await?;
        let index = graph.location(blob).ok_or(Error::InvalidReference)?;
        if graph.nodes[index as usize].kind != Some(gix_object::Kind::Blob) {
            return Err(Error::InvalidReference);
        }
        let object = reader.find(blob).await?.ok_or(Error::InvalidReference)?;
        if object.kind != gix_object::Kind::Blob {
            return Err(Error::InvalidReference);
        }
        let (actual, bytes) =
            crate::packfile_uri::canonical(&self.operation, self.format, &object.data)?;
        if actual != checksum {
            return Err(Error::InvalidReference);
        }
        Ok(bytes)
    }
}
