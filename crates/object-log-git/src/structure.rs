//! One structural parser shared by retained graphs and edge-free walks.
use crate::{
    Error, ObjectId,
    pack::{budget::Operation, invalid, object_hash, pack_error},
};
use gix_object::{Kind, commit::ref_iter::Token, tree::EntryKind};
use std::collections::HashSet;

pub(crate) trait Links: Send {
    fn link(
        &mut self,
        id: ObjectId,
        kind: Kind,
        verify: bool,
    ) -> impl Future<Output = Result<(), Error>> + Send;
}

pub(crate) async fn visit(
    operation: &Operation,
    id: ObjectId,
    kind: Kind,
    data: &[u8],
    mut link: impl Links,
) -> Result<i64, Error> {
    operation.work(data.len())?;
    // Preserve the existing parser's allocation admission: multiline commit
    // headers and the borrowed tree-name set are bounded before parsing.
    let _scratch = if kind == Kind::Tree {
        operation.reserve_state(data.len() * 2 + 128)?
    } else {
        operation.reserve(if kind == Kind::Commit {
            data.len() * 2
        } else {
            0
        })?
    };
    let hash = object_hash(id.format());
    let mut time = 0;
    match kind {
        Kind::Commit => {
            let mut complete = false;
            for token in gix_object::CommitRefIter::from_bytes(data, hash) {
                match token.map_err(pack_error)? {
                    Token::Tree { id: target } => {
                        link.link(
                            ObjectId::from_bytes(id.format(), target.as_slice())?,
                            Kind::Tree,
                            true,
                        )
                        .await?;
                    }
                    Token::Parent { id: target } => {
                        link.link(
                            ObjectId::from_bytes(id.format(), target.as_slice())?,
                            Kind::Commit,
                            true,
                        )
                        .await?;
                    }
                    Token::Committer { signature } => {
                        time = signature.time().map_err(pack_error)?.seconds;
                    }
                    Token::Message(_) => complete = true,
                    _ => {}
                }
            }
            if !complete {
                return invalid("commit headers are incomplete");
            }
        }
        Kind::Tree => tree(id, data, &mut link).await?,
        Kind::Tag => {
            let tag = gix_object::TagRef::from_bytes(data, hash).map_err(pack_error)?;
            link.link(
                ObjectId::from_bytes(id.format(), tag.target().as_slice())?,
                tag.target_kind,
                tag.target_kind != Kind::Blob,
            )
            .await?;
        }
        Kind::Blob => {}
    }
    Ok(time)
}

async fn tree(id: ObjectId, data: &[u8], link: &mut impl Links) -> Result<(), Error> {
    let mut previous = None;
    let mut names = HashSet::with_capacity(data.len() / (id.format().digest_len() + 8));
    for entry in gix_object::TreeRefIter::from_bytes(data, object_hash(id.format())) {
        let entry = entry.map_err(pack_error)?;
        if !matches!(
            entry.mode.value(),
            0o040_000 | 0o100_644 | 0o100_755 | 0o120_000 | 0o160_000
        ) {
            return invalid("tree entry mode is invalid");
        }
        gix_validate::path::component(
            entry.filename,
            (entry.mode.kind() == EntryKind::Link)
                .then_some(gix_validate::path::component::Mode::Symlink),
            gix_validate::path::component::Options {
                protect_windows: false,
                protect_hfs: true,
                protect_ntfs: true,
            },
        )
        .map_err(pack_error)?;
        if !names.insert(entry.filename)
            || previous
                .as_ref()
                .is_some_and(|last: &gix_object::tree::EntryRef<'_>| last >= &entry)
        {
            return invalid("tree entries are duplicated or unordered");
        }
        ObjectId::from_bytes(id.format(), entry.oid.as_bytes())?;
        previous = Some(entry);
        let kind = match entry.mode.kind() {
            EntryKind::Tree => Kind::Tree,
            EntryKind::Blob | EntryKind::BlobExecutable | EntryKind::Link => Kind::Blob,
            EntryKind::Commit => continue,
        };
        link.link(
            ObjectId::from_bytes(id.format(), entry.oid.as_bytes())?,
            kind,
            kind != Kind::Blob,
        )
        .await?;
    }
    Ok(())
}
