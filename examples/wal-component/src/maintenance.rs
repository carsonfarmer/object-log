use super::{CollectionResult, Failure, MaintenanceState, SessionState, failure};
use object_log::{
    CheckpointStatus, CollectionFinish, CollectionReport, CollectionStart, Log, StagedObject, View,
};

pub(super) async fn checkpoint(
    log: &Log,
    view: &View,
    data: Vec<u8>,
    roots: Vec<StagedObject>,
) -> Result<CheckpointStatus, Failure> {
    let through = view
        .tail()
        .last()
        .ok_or_else(|| Failure::Other("checkpoint requires an active tail".into()))?;
    log.publish_checkpoint(view, through, data.into(), roots)
        .await
        .map_err(failure)
}

// One durable deletion plan per call; a later call resumes an interrupted plan.
pub(super) async fn collect(
    session: &SessionState,
    max_candidates: usize,
) -> Result<CollectionResult, Failure> {
    let current = session.current_view();
    let view = match session
        .log
        .start_collection_with_limit(&current, max_candidates)
        .await
        .map_err(failure)?
    {
        CollectionStart::Empty(report) => return Ok(result(MaintenanceState::Complete, report)),
        CollectionStart::Installed(view, _) | CollectionStart::Active(view) => view,
        CollectionStart::Conflict(_) => return Ok(empty(MaintenanceState::Conflict)),
        CollectionStart::Pending => return Ok(empty(MaintenanceState::Pending)),
        CollectionStart::Retained(_) => return Ok(empty(MaintenanceState::Retained)),
    };
    let (state, report) = match session
        .log
        .resume_collection(&view)
        .await
        .map_err(failure)?
    {
        CollectionFinish::Complete(_, report) => (MaintenanceState::More, report),
        CollectionFinish::Pending(report) => (MaintenanceState::Pending, report),
        CollectionFinish::Conflict(_, report) => (MaintenanceState::Conflict, report),
    };
    Ok(result(state, report))
}
fn empty(state: MaintenanceState) -> CollectionResult {
    CollectionResult {
        state,
        objects: 0,
        bytes: 0,
    }
}
fn result(state: MaintenanceState, report: CollectionReport) -> CollectionResult {
    CollectionResult {
        state,
        objects: report.candidate_count() as u64,
        bytes: report.candidate_bytes(),
    }
}
