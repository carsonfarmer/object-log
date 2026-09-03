//! Object-log publication protocol.

use crate::Cursor;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Options {
    pub max_tail_entries: usize,
    pub resolution_window: usize,
    pub max_inline_operation_bytes: usize,
    pub max_inline_result_bytes: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            max_tail_entries: 1_024,
            resolution_window: 1_024,
            max_inline_operation_bytes: 64 * 1_024,
            max_inline_result_bytes: 4 * 1_024,
        }
    }
}

#[derive(Clone, Debug)]
pub struct View {
    pub(crate) cursor: Cursor,
}

impl View {
    #[must_use]
    pub const fn cursor(&self) -> &Cursor {
        &self.cursor
    }
}

#[derive(Debug)]
pub enum Refresh {
    NotModified,
    Updated(View),
}

#[derive(Debug)]
pub enum CommitStatus {
    Committed(View),
    Conflict(View),
    Pending(crate::PendingCommit),
}

#[derive(Debug)]
pub enum Resolution {
    Committed(View),
    NotCommitted(View),
    StillPending(crate::PendingCommit),
    Expired(View),
}

#[derive(Debug)]
pub enum CheckpointStatus {
    Published(View),
    Conflict(View),
}

#[derive(Debug)]
pub struct Log;
