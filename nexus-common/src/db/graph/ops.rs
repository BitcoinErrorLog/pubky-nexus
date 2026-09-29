use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use neo4rs::Row;

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

/// An open graph transaction on its own connection.
///
/// Write locks a statement takes are held until [`GraphTxn::commit`] or
/// [`GraphTxn::rollback`]. A handle dropped without either (an error path
/// that returns early, a cancelled task) rolls the transaction back in a
/// spawned task: the driver would otherwise hold the locks until the pool
/// next hands out and resets that connection.
pub struct GraphTxn {
    inner: Option<neo4rs::Txn>,
    /// Locks the statements of this transaction hold, registered in the
    /// enclosing [`super::lock::lock_scope`] so a cache fill of the same
    /// code does not wait for its own transaction.
    held: Vec<String>,
}

impl GraphTxn {
    fn new(inner: neo4rs::Txn) -> Self {
        Self {
            inner: Some(inner),
            held: Vec::new(),
        }
    }

    /// Records that this transaction holds the write lock named by `key`
    /// (see [`super::lock::LockTarget::key`]) until it ends.
    pub fn hold(&mut self, key: &str) {
        if super::lock::register_held(key) {
            self.held.push(key.to_string());
        }
    }

    fn release_held(&mut self) {
        for key in self.held.drain(..) {
            super::lock::release_held(&key);
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
        self.release_held();
        txn.commit().await.map_err(Into::into)
    }

    pub async fn rollback(mut self) -> GraphResult<()> {
        let txn = self.inner.take().expect("transaction is open");
        self.release_held();
        txn.rollback().await.map_err(Into::into)
    }
}

impl Drop for GraphTxn {
    fn drop(&mut self) {
        self.release_held();
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
        self.inner.run(query.into()).await
    }

    async fn start_txn(&self) -> neo4rs::Result<GraphTxn> {
        self.inner.start_txn().await.map(GraphTxn::new)
    }
}
