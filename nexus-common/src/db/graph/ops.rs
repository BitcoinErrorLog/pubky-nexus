use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use neo4rs::Row;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use tokio::sync::{Semaphore, SemaphorePermit};

use super::error::GraphResult;
use super::query::Query;

/// Abstraction over graph database operations.
/// Callers depend on this trait, not the concrete implementations.
#[async_trait]
pub trait GraphOps: Send + Sync {
    /// Execute query, return boxed row stream.
    async fn execute(
        &self,
        query: Query,
    ) -> neo4rs::Result<BoxStream<'static, Result<Row, neo4rs::Error>>>;

    /// Fire-and-forget query execution.
    async fn run(&self, query: Query) -> neo4rs::Result<()>;

    /// Opens an explicit transaction on a dedicated connection.
    async fn start_txn(&self) -> neo4rs::Result<GraphTxn>;
}

/// Transactions that may be open at once. Each holds one connection of the
/// driver's pool (16 by default) from its first statement to its end, and the
/// tag writes and target deletions that open them wait on each other's locks.
/// Capping them at half the pool leaves connections for the autocommit
/// statements of everything else, so a burst of blocked writers cannot occupy
/// the whole pool and starve the statement that would release them.
pub const MAX_OPEN_TXNS: usize = 8;

static TXN_PERMITS: Semaphore = Semaphore::const_new(MAX_OPEN_TXNS);
static OPEN_TXNS: AtomicUsize = AtomicUsize::new(0);
static AUTOCOMMIT_STATEMENTS: AtomicU64 = AtomicU64::new(0);
static FAIL_NEXT_COMMIT: AtomicBool = AtomicBool::new(false);

/// Makes the next [`GraphTxn::commit`] roll the transaction back and fail, as
/// a lost connection would. Integration tests only.
#[doc(hidden)]
pub fn fail_next_commit() {
    FAIL_NEXT_COMMIT.store(true, Ordering::SeqCst);
}

/// Graph transactions open right now. Integration tests only.
#[doc(hidden)]
pub fn open_txn_count() -> usize {
    OPEN_TXNS.load(Ordering::SeqCst)
}

/// Statements run so far outside a transaction, each on a pool connection
/// of its own. Integration tests only.
#[doc(hidden)]
pub fn autocommit_statement_count() -> u64 {
    AUTOCOMMIT_STATEMENTS.load(Ordering::SeqCst)
}

/// An open graph transaction on its own connection.
///
/// Write locks a statement takes are held until [`GraphTxn::commit`] or
/// [`GraphTxn::rollback`]. A handle dropped without either (an error path
/// that returns early, a cancelled task) rolls the transaction back in a
/// spawned task: the driver would otherwise hold the locks until the pool
/// next hands out and resets that connection.
pub struct GraphTxn {
    inner: Option<neo4rs::Txn>,
    _permit: SemaphorePermit<'static>,
}

impl GraphTxn {
    fn new(inner: neo4rs::Txn, permit: SemaphorePermit<'static>) -> Self {
        OPEN_TXNS.fetch_add(1, Ordering::SeqCst);
        Self {
            inner: Some(inner),
            _permit: permit,
        }
    }

    fn open(&mut self) -> &mut neo4rs::Txn {
        self.inner
            .as_mut()
            .expect("a transaction is open until it is committed or rolled back")
    }

    /// Runs a statement and discards its rows.
    pub async fn run(&mut self, query: Query) -> GraphResult<()> {
        self.open().run(query.into()).await.map_err(Into::into)
    }

    /// Runs a statement and collects every row. The rows are consumed before
    /// the next statement, as the connection allows one open result.
    pub async fn fetch_all(&mut self, query: Query) -> GraphResult<Vec<Row>> {
        let txn = self.open();
        let mut stream = txn.execute(query.into()).await?;
        let mut rows = Vec::new();
        while let Some(row) = stream.next(&mut *txn).await? {
            rows.push(row);
        }
        Ok(rows)
    }

    /// Runs a statement and returns its first row, consuming the rest.
    pub async fn fetch_row(&mut self, query: Query) -> GraphResult<Option<Row>> {
        Ok(self.fetch_all(query).await?.into_iter().next())
    }

    pub async fn commit(mut self) -> GraphResult<()> {
        let txn = self.inner.take().expect("transaction is open");
        if FAIL_NEXT_COMMIT.swap(false, Ordering::SeqCst) {
            txn.rollback().await?;
            return Err(super::error::GraphError::Generic(
                "injected commit failure".to_string(),
            ));
        }
        txn.commit().await.map_err(Into::into)
    }

    pub async fn rollback(mut self) -> GraphResult<()> {
        let txn = self.inner.take().expect("transaction is open");
        txn.rollback().await.map_err(Into::into)
    }
}

impl Drop for GraphTxn {
    fn drop(&mut self) {
        OPEN_TXNS.fetch_sub(1, Ordering::SeqCst);
        let Some(txn) = self.inner.take() else {
            return;
        };
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                if let Err(error) = txn.rollback().await {
                    tracing::warn!("Rolling back an abandoned graph transaction failed: {error}");
                }
            });
        }
    }
}

/// Thin wrapper around `neo4rs::Graph` implementing `GraphOps` without any observability overhead.
#[derive(Clone)]
pub struct Graph {
    inner: neo4rs::Graph,
}

impl Graph {
    pub fn new(graph: neo4rs::Graph) -> Self {
        Self { inner: graph }
    }
}

#[async_trait]
impl GraphOps for Graph {
    async fn execute(
        &self,
        query: Query,
    ) -> neo4rs::Result<BoxStream<'static, Result<Row, neo4rs::Error>>> {
        AUTOCOMMIT_STATEMENTS.fetch_add(1, Ordering::SeqCst);
        let stream = self
            .inner
            .execute(query.into())
            .await?
            .into_stream()
            .map_err(Into::into)
            .boxed();
        Ok(stream)
    }

    async fn run(&self, query: Query) -> neo4rs::Result<()> {
        AUTOCOMMIT_STATEMENTS.fetch_add(1, Ordering::SeqCst);
        self.inner.run(query.into()).await
    }

    async fn start_txn(&self) -> neo4rs::Result<GraphTxn> {
        let permit = TXN_PERMITS
            .acquire()
            .await
            .expect("the transaction semaphore is never closed");
        let txn = self.inner.start_txn().await?;
        Ok(GraphTxn::new(txn, permit))
    }
}
