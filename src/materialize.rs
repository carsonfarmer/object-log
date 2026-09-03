//! Typed recovery of state from one durable base and ordered WAL tail.

use std::error::Error as StdError;

use crate::{Error, Log, View};

/// Applies opaque log data to one application state type.
pub trait Materializer {
    /// The reconstructed application state.
    type State;
    /// A domain-specific decode or state-transition error.
    type Error: StdError + 'static;

    /// Creates the state before the first committed operation.
    fn empty(&self) -> Self::State;

    /// Restores one application snapshot.
    ///
    /// # Errors
    ///
    /// Returns a domain error when the snapshot is invalid.
    fn restore(&self, checkpoint: &[u8]) -> Result<Self::State, Self::Error>;

    /// Applies one committed operation in sequence order.
    ///
    /// # Errors
    ///
    /// Returns a domain error when the operation is invalid for `state`.
    fn apply(
        &self,
        state: &mut Self::State,
        sequence: u64,
        operation: &[u8],
    ) -> Result<(), Self::Error>;

    /// Encodes one application snapshot.
    ///
    /// # Errors
    ///
    /// Returns a domain error when the state cannot be encoded.
    fn checkpoint(&self, state: &Self::State) -> Result<Vec<u8>, Self::Error>;
}

/// One state value reconstructed from one exact durable view.
#[derive(Clone, Debug)]
pub struct Materialized<S> {
    view: View,
    state: S,
}

impl<S> Materialized<S> {
    /// Returns the exact durable view used for reconstruction.
    #[must_use]
    pub const fn view(&self) -> &View {
        &self.view
    }

    /// Returns the reconstructed state.
    #[must_use]
    pub const fn state(&self) -> &S {
        &self.state
    }

    /// Separates the durable view and reconstructed state.
    #[must_use]
    pub fn into_parts(self) -> (View, S) {
        (self.view, self.state)
    }
}

/// Failure while reconstructing one application state.
#[derive(Debug, thiserror::Error)]
pub enum MaterializeError<E: StdError + 'static> {
    /// Durable log loading or verification failed.
    #[error(transparent)]
    Log(#[from] Error),
    /// Application snapshot or operation processing failed.
    #[error("state materialization failed: {0}")]
    State(E),
}

/// Reconstructs state from one exact index, its base, and its active WAL tail.
///
/// # Errors
///
/// Returns a log error for invalid durable storage or a domain error for an
/// invalid application snapshot or operation.
pub async fn materialize<M>(
    log: &Log,
    materializer: &M,
) -> Result<Materialized<M::State>, MaterializeError<M::Error>>
where
    M: Materializer,
{
    let view = log.load().await?;
    let mut state = match log.read_checkpoint(&view).await? {
        Some(snapshot) => materializer
            .restore(&snapshot)
            .map_err(MaterializeError::State)?,
        None => materializer.empty(),
    };
    for record in log.read_tail(&view).await? {
        materializer
            .apply(
                &mut state,
                record.reference().sequence(),
                record.operation(),
            )
            .map_err(MaterializeError::State)?;
    }
    Ok(Materialized { view, state })
}
