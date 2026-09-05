#![doc = include_str!("../README.md")]
#![deny(missing_docs, unsafe_code)]

mod server;
mod shared;
pub use shared::SharedGitHttpServer;

const CACHE_CONTROL: &str = "no-cache, max-age=0, must-revalidate";

/// One supported Git smart HTTP service.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Service {
    /// Fetch negotiation and pack transfer.
    UploadPack,
    /// Atomic ref updates and optional pack transfer.
    ReceivePack,
}

impl Service {
    /// Returns the exact media type for an `info/refs` response.
    #[must_use]
    const fn advertisement_content_type(self) -> &'static str {
        match self {
            Self::UploadPack => "application/x-git-upload-pack-advertisement",
            Self::ReceivePack => "application/x-git-receive-pack-advertisement",
        }
    }

    /// Returns the exact media type for a service POST response.
    #[must_use]
    const fn result_content_type(self) -> &'static str {
        match self {
            Self::UploadPack => "application/x-git-upload-pack-result",
            Self::ReceivePack => "application/x-git-receive-pack-result",
        }
    }
}

/// A Git protocol or local I/O failure.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
enum Error {
    /// The request exceeds a fixed byte or item limit.
    #[error("Git request is too large: {0}")]
    RequestTooLarge(&'static str),
    /// A request body operation failed.
    #[error("Git HTTP body I/O failed: {0}")]
    Io(#[from] std::io::Error),
    /// The repository rejected a request or durable state.
    #[error(transparent)]
    Git(#[from] object_log_git::Error),
}
