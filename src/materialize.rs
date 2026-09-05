//! Typed recovery of state from one durable base and ordered WAL tail.

use std::error::Error as StdError;

use futures::TryStreamExt;

use crate::{Error, Log, ObjectRef, StagedObject, View};

/// Applies opaque log data to one application state type.
///
/// Callbacks may run before a later storage or state error is discovered.
/// Failed materialization drops its partial state; it does not roll back any
/// external effects performed by callbacks.
pub trait Materializer {
    /// The reconstructed application state.
    type State;
    /// A domain-specific decode or state-transition error.
    type Error: StdError + 'static;

    /// Creates the state before the first committed operation.
    fn empty(&self) -> Self::State;

    /// Restores one application snapshot.
    ///
    /// `objects` contains publication proofs for the snapshot's ordered object
    /// references. State can retain these proofs for a checkpoint against the
    /// returned [`Materialized::view`].
    ///
    /// # Errors
    ///
    /// Returns a domain error when the snapshot is invalid.
    fn restore(
        &self,
        checkpoint: &[u8],
        objects: &[StagedObject],
    ) -> Result<Self::State, Self::Error>;

    /// Applies one committed operation in sequence order.
    ///
    /// `objects` contains publication proofs for the operation's ordered
    /// object references.
    ///
    /// # Errors
    ///
    /// Returns a domain error when the operation is invalid for `state`.
    fn apply(
        &self,
        state: &mut Self::State,
        operation: &[u8],
        objects: &[StagedObject],
    ) -> Result<(), Self::Error>;
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

/// Reconstructs state from one exact observed view.
///
/// The returned [`Materialized`] contains the supplied view.
///
/// # Errors
///
/// Returns a log error for invalid durable storage or a domain error for an
/// invalid application snapshot or operation.
pub async fn materialize<M>(
    log: &Log,
    view: View,
    materializer: &M,
) -> Result<Materialized<M::State>, MaterializeError<M::Error>>
where
    M: Materializer,
{
    let mut state = match log.read_checkpoint(&view).await? {
        Some(checkpoint) => {
            let objects = record_proofs(log, &view, checkpoint.objects());
            materializer
                .restore(checkpoint.snapshot(), &objects)
                .map_err(MaterializeError::State)?
        }
        None => materializer.empty(),
    };
    {
        let records = log.tail_records(&view)?;
        futures::pin_mut!(records);
        while let Some(record) = records.try_next().await? {
            let objects = record_proofs(log, &view, record.objects());
            materializer
                .apply(&mut state, record.operation(), &objects)
                .map_err(MaterializeError::State)?;
        }
    }
    Ok(Materialized { view, state })
}

fn record_proofs(log: &Log, view: &View, objects: &[ObjectRef]) -> Vec<StagedObject> {
    objects
        .iter()
        .cloned()
        .map(|object| log.staged_object(view, object))
        .collect()
}
