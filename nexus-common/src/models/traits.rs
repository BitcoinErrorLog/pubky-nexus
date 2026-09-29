use crate::db::graph::lock::{under_batch_lock, BatchLock};
use crate::db::graph::Query;
use crate::db::kv::RedisResult;
use crate::db::{exec_single_row, fetch_all_rows_from_graph, GraphResult, RedisOps};
use crate::models::error::{ModelError, ModelResult};
use async_trait::async_trait;
use core::fmt;
use std::collections::HashSet;
use std::fmt::Debug;

pub trait CollectionId {
    fn to_string_id(self) -> String;
}

impl CollectionId for &str {
    fn to_string_id(self) -> String {
        String::from(self)
    }
}

impl CollectionId for &[&str] {
    fn to_string_id(self) -> String {
        self.join(":")
    }
}

#[async_trait]
pub trait Collection<T>
where
    Self: RedisOps + Clone + Debug,
    T: CollectionId + fmt::Debug + Sync + Send + Copy,
{
    /// Retrieves records by their IDs, first attempting to fetch them from a cache (e.g., Redis),
    /// and then querying a graph database (e.g., Neo4j) if necessary.
    ///
    /// # Arguments
    ///
    /// * `ids` - A slice of id slices representing the IDs to query.
    ///
    /// # Returns
    ///
    /// This function returns a `Result` containing a vector of `Option<Self>`. Each `Option` corresponds to
    /// a queried ID, containing `Some(record)` if the record was found in either the cache or the graph database,
    /// or `None` if it was not found in either.
    async fn get_by_ids(ids: &[T]) -> ModelResult<Vec<Option<Self>>> {
        let key_parts_list: Vec<String> = ids.iter().map(|id| id.to_string_id()).collect();

        let keys_refs: Vec<Vec<&str>> = key_parts_list.iter().map(|id| vec![id.as_str()]).collect();

        let keys: Vec<&[&str]> = keys_refs.iter().map(|arr| &arr[..]).collect();

        let mut collection = Self::get_from_index(keys).await?;

        let mut missing_ids: Vec<(usize, T)> = Vec::new();
        for (i, details) in collection.iter().enumerate() {
            if details.is_none() {
                missing_ids.push((i, ids[i]));
            }
        }

        if !missing_ids.is_empty() {
            let flat_missing_ids: Vec<T> = missing_ids.iter().map(|&(_, id)| id).collect();
            let fetched_details = Self::fetch_and_index_locked(&flat_missing_ids).await?;

            if !fetched_details.is_empty() {
                for (i, (original_index, _)) in missing_ids.iter().enumerate() {
                    collection[*original_index].clone_from(&fetched_details[i]);
                }
            }
        }

        Ok(collection)
    }

    /// Queries a Neo4j graph database to retrieve records based on the provided IDs and collection type.
    ///
    /// # Arguments
    ///
    /// * `ids` - A slice of string slices representing the IDs.
    ///
    /// # Returns
    ///
    /// This function returns a `Result` containing a vector of `Option<Self>`. Each `Option` corresponds to
    /// a queried ID, containing `Some(record)` if the record was found in the graph database, or `None` if it was not found.
    async fn get_from_graph(ids: &[T]) -> GraphResult<Vec<Option<Self>>> {
        let query = Self::collection_details_graph_query(ids);
        let rows = fetch_all_rows_from_graph(query).await?;

        let mut records = Vec::with_capacity(ids.len());

        for row in rows {
            let record: Option<Self> = row.get("record").ok();
            records.push(record);
        }
        Ok(records)
    }

    async fn get_from_index(keys: Vec<&[&str]>) -> RedisResult<Vec<Option<Self>>> {
        Self::try_from_index_multiple_json(&keys).await
    }

    /// Indexes collection of records in Redis for faster access in future queries.
    ///
    /// # Arguments
    ///
    /// * `ids` - A slice of id slices representing the IDs of the records to index.
    /// * `records` - A vector of `Option<Self>` containing the records to be indexed.
    ///   Each `Option` corresponds to an ID.
    ///
    /// # Returns
    ///
    /// This function returns a `Result` indicating success or failure. A successful result indicates that the
    /// records were successfully indexed in the cache.
    async fn put_to_index(ids: &[T], records: Vec<Option<Self>>) -> RedisResult<()> {
        let mut found_records = Vec::with_capacity(records.len());
        let mut found_record_ids = Vec::with_capacity(records.len());

        for (detail, id) in records.iter().zip(ids.iter()) {
            if let Some(value) = detail {
                found_records.push(Some(value.clone()));
                found_record_ids.push(*id);
            }
        }
        let key_parts_list: Vec<String> = found_record_ids
            .iter()
            .map(|id| id.to_string_id())
            .collect();

        let keys_refs: Vec<Vec<&str>> = key_parts_list.iter().map(|id| vec![id.as_str()]).collect();

        let keys: Vec<&[&str]> = keys_refs.iter().map(|arr| &arr[..]).collect();

        Self::put_multiple_json_indexes(&keys, found_records).await?;
        Self::extend_on_index_miss(&records).await?;
        Ok(())
    }

    // Save new graph node
    async fn put_to_graph(&self) -> GraphResult<()> {
        exec_single_row(self.put_graph_query()?).await
    }

    async fn reindex(collection_ids: &[T]) -> ModelResult<()> {
        match Self::fetch_and_index_locked(collection_ids).await {
            Ok(_) => {}
            Err(ModelError::GraphOperationFailed(e)) => {
                tracing::error!("Error: Could not find any element of the collection: {}", e)
            }
            Err(e) => return Err(e),
        }
        Ok(())
    }

    /// The node locks that serialize this collection's Redis writes against
    /// the deletion of its records (users, files), naming the records `ids`.
    /// A deletion holds them until it has cleaned Redis.
    fn locked_ids(_ids: &[T]) -> Option<BatchLock> {
        None
    }

    /// Reads the records from the graph and writes them to Redis. For locked
    /// records, both happen under their locks and only records that still
    /// exist are written.
    async fn fetch_and_index_locked(ids: &[T]) -> ModelResult<Vec<Option<Self>>> {
        let Some(batch) = Self::locked_ids(ids) else {
            return Self::fetch_and_index(ids, None).await;
        };
        under_batch_lock(batch, |present| async move {
            Self::fetch_and_index(ids, Some(&present)).await
        })
        .await
    }

    async fn fetch_and_index(
        ids: &[T],
        present: Option<&HashSet<String>>,
    ) -> ModelResult<Vec<Option<Self>>> {
        let mut records = Self::get_from_graph(ids).await?;
        if let Some(present) = present {
            for (record, id) in records.iter_mut().zip(ids) {
                if !present.contains(&id.to_string_id()) {
                    *record = None;
                }
            }
        }
        if !records.is_empty() {
            Self::put_to_index(ids, records.clone()).await?;
        }
        Ok(records)
    }

    /// Returns the neo4j query to return a list records by passing a list of ids.
    /// The query should return each record in the "record" attribute of the node.
    fn collection_details_graph_query(id_list: &[T]) -> Query;

    /// Returns the neo4j query to put a record into the graph.
    fn put_graph_query(&self) -> GraphResult<Query>;

    async fn extend_on_index_miss(elements: &[std::option::Option<Self>]) -> RedisResult<()>;
}
