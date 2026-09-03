use std::sync::{
    Arc,
    atomic::{AtomicU8, Ordering},
};

use rusqlite::Connection;
use rusqlite::hooks::{AuthAction, AuthContext, Authorization};

use crate::SqliteError;

const INTERNAL: u8 = 0;
const READ: u8 = 1;
const WRITE: u8 = 2;

#[derive(Clone, Debug)]
pub(crate) struct Policy(Arc<AtomicU8>);

impl Policy {
    pub(crate) fn install(conn: &Connection) -> Result<Self, SqliteError> {
        let policy = Self(Arc::new(AtomicU8::new(INTERNAL)));
        let installed = policy.clone();
        let callback: Box<dyn for<'a> FnMut(AuthContext<'a>) -> Authorization + Send + 'static> =
            Box::new(move |context| installed.authorize(context));
        conn.authorizer(Some(callback))?;
        Ok(policy)
    }

    pub(crate) fn read(&self) -> Guard<'_> {
        self.enter(READ)
    }

    pub(crate) fn write(&self) -> Guard<'_> {
        self.enter(WRITE)
    }

    fn enter(&self, mode: u8) -> Guard<'_> {
        self.0.store(mode, Ordering::Relaxed);
        Guard(self)
    }

    fn authorize(&self, context: AuthContext<'_>) -> Authorization {
        match self.0.load(Ordering::Relaxed) {
            INTERNAL => Authorization::Allow,
            READ if read_action(context.action) => Authorization::Allow,
            WRITE if read_action(context.action) || write_action(context) => Authorization::Allow,
            _ => Authorization::Deny,
        }
    }
}

pub(crate) struct Guard<'a>(&'a Policy);

impl Drop for Guard<'_> {
    fn drop(&mut self) {
        self.0.0.store(INTERNAL, Ordering::Relaxed);
    }
}

fn read_action(action: AuthAction<'_>) -> bool {
    matches!(
        action,
        AuthAction::Read { .. }
            | AuthAction::Select
            | AuthAction::Function { .. }
            | AuthAction::Recursive
    )
}

fn write_action(context: AuthContext<'_>) -> bool {
    if matches!(context.action, AuthAction::Savepoint { .. }) {
        return true;
    }
    if let AuthAction::AlterTable { database_name, .. } = context.action {
        return database_name == "main";
    }
    if context.database_name != Some("main") {
        return false;
    }
    matches!(
        context.action,
        AuthAction::CreateIndex { .. }
            | AuthAction::CreateTable { .. }
            | AuthAction::CreateTrigger { .. }
            | AuthAction::CreateView { .. }
            | AuthAction::Delete { .. }
            | AuthAction::DropIndex { .. }
            | AuthAction::DropTable { .. }
            | AuthAction::DropTrigger { .. }
            | AuthAction::DropView { .. }
            | AuthAction::Insert { .. }
            | AuthAction::Update { .. }
            | AuthAction::Reindex { .. }
            | AuthAction::Analyze { .. }
    )
}
