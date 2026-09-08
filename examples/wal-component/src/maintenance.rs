use super::{CollectionResult, Failure, MaintenanceState, SessionState, failure};
use object_log::{
    CheckpointResolution, CheckpointStatus, CollectionFinish, CollectionReport, CollectionStart,
    StagedObject,
};

pub(super) async fn checkpoint(
    session: &SessionState,
    data: Vec<u8>,
    roots: Vec<StagedObject>,
) -> Result<MaintenanceState, Failure> {
    let through = session
        .view
        .tail()
        .last()
        .ok_or_else(|| Failure::Other("checkpoint requires an active tail".into()))?;
    match session
        .log
        .publish_checkpoint(&session.view, through, data.into(), roots)
        .await
        .map_err(failure)?
    {
        CheckpointStatus::Published(_) => Ok(MaintenanceState::Complete),
        CheckpointStatus::Conflict(_) => Ok(MaintenanceState::Conflict),
        CheckpointStatus::Pending(pending) => {
            match session
                .log
                .resolve_checkpoint(pending)
                .await
                .map_err(failure)?
            {
                CheckpointResolution::Published(_) => Ok(MaintenanceState::Complete),
                CheckpointResolution::NotPublished(_) => Ok(MaintenanceState::Conflict),
                CheckpointResolution::StillPending(_) | CheckpointResolution::Expired(_) => {
                    Ok(MaintenanceState::Pending)
                }
            }
        }
    }
}

// One durable deletion plan per call; a later call resumes an interrupted plan.
pub(super) async fn collect(session: &SessionState) -> Result<CollectionResult, Failure> {
    let view = match session
        .log
        .start_collection(&session.view)
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
