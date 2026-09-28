use super::{CollectionResult, Failure, MaintenanceState, SessionState};
use object_log::{CollectionFinish, CollectionReport, CollectionStart};

// One durable deletion plan per call; a later call resumes an interrupted plan.
pub(super) async fn collect(
    session: &SessionState,
    max_candidates: usize,
) -> Result<CollectionResult, Failure> {
    let current = session.current_view();
    let (view, state) = match session
        .log
        .start_collection_with_limit(&current, max_candidates)
        .await?
    {
        CollectionStart::Empty(report) => {
            return Ok(result(MaintenanceState::Complete, Some(report)));
        }
        CollectionStart::Installed(view, _) | CollectionStart::Active(view) => (view, None),
        CollectionStart::Conflict(view) => (view, Some(MaintenanceState::Conflict)),
        CollectionStart::Retained(view) => (view, Some(MaintenanceState::Retained)),
        CollectionStart::Pending => return Ok(result(MaintenanceState::Pending, None)),
    };
    session.view.replace(view.clone());
    if let Some(state) = state {
        return Ok(result(state, None));
    }
    let (state, report) = match session.log.resume_collection(&view).await? {
        CollectionFinish::Complete(view, report) => {
            session.view.replace(view);
            (MaintenanceState::More, report)
        }
        CollectionFinish::Pending(report) => (MaintenanceState::Pending, report),
        CollectionFinish::Conflict(view, report) => {
            session.view.replace(view);
            (MaintenanceState::Conflict, report)
        }
    };
    Ok(result(state, Some(report)))
}
fn result(state: MaintenanceState, report: Option<CollectionReport>) -> CollectionResult {
    CollectionResult {
        state,
        objects: report.map_or(0, |report| report.candidate_count() as u64),
        bytes: report.map_or(0, |report| report.candidate_bytes()),
    }
}
