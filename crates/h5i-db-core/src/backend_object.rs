//! Object-store (S3 / GCS / Azure / MinIO / in-memory) backend (ROADMAP §4.6).
//!
//! The local backend's writer critical section is an OS `flock`; object
//! stores have no locks, but modern ones (S3 since 2024, GCS, Azure, MinIO,
//! R2, and `object_store`'s `InMemory`) support **conditional writes**, which
//! is what the commit protocol actually needs:
//!
//! - The manifest slot `manifests/<seq>.json` is written with
//!   `PutMode::Create` (If-None-Match: *): of two racing writers, exactly one
//!   materializes the slot; the loser gets `VersionConflict` *before* HEAD is
//!   touched, so a committed slot can never be overwritten.
//! - HEAD is swapped with `PutMode::Update` on the ETag observed at read
//!   time (`PutMode::Create` for the first commit): the storage layer itself
//!   is the compare-and-swap.
//!
//! Failure modes vs the local backend:
//! - A writer that crashes after creating the manifest slot but before the
//!   HEAD swap leaves an *orphan slot* at `head+1`. Later commits then fail
//!   with a retryable `VersionConflict` whose hint says to run
//!   `vacuum --apply` — which already classifies manifests above HEAD as
//!   debris and clears the slot. Safe (no corruption is possible), explicit,
//!   and self-describing; the trade-off for having no lock to lose.
//! - There is no fsync barrier to manage: object-store PUTs are
//!   durable-on-ack, so `sync_objects` is a no-op.
//!
//! `flock` does not exist here, so `Backend::meta_lock` degrades to a no-op
//! guard; catalog/snapshot creation stays safe through `put_if_absent`
//! (conditional create), and policy read-modify-write goes through
//! [`Database::update_policy`] whose object-store variant uses ETag CAS.

use std::sync::Arc;

use async_trait::async_trait;
use futures::future::BoxFuture;
use object_store::{
    path::Path as ObjPath, ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload,
    UpdateVersion,
};
use url::Url;
use uuid::Uuid;

use crate::backend::{Backend, HeadState, HeadStore, HeadTag};
use crate::error::{Error, Result};
use crate::layout;
use crate::manifest::Head;

/// HEAD store implemented purely with conditional puts.
#[derive(Debug)]
pub struct ObjectStoreHeadStore {
    store: Arc<dyn ObjectStore>,
}

impl ObjectStoreHeadStore {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
    }

    async fn read_with_meta(&self, table_id: Uuid) -> Result<Option<(Head, HeadTag)>> {
        match self.store.get(&layout::head_path(table_id)).await {
            Ok(res) => {
                // The ETag is the CAS token. Every real object store returns
                // one; treat its absence as an implementation bug.
                let etag = res.meta.e_tag.clone().ok_or_else(|| {
                    Error::internal("object store returned no ETag for HEAD; CAS impossible")
                })?;
                let version = res.meta.version.clone();
                let bytes = res.bytes().await.map_err(Error::ObjectStore)?;
                let head = Head::from_bytes(&bytes, layout::head_path(table_id).as_ref())?;
                Ok(Some((head, HeadTag(encode_tag(&etag, version.as_deref())))))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(Error::ObjectStore(e)),
        }
    }
}

/// Pack ETag (+ optional version id) into the opaque `HeadTag`.
fn encode_tag(etag: &str, version: Option<&str>) -> String {
    match version {
        Some(v) => format!("{etag}\u{1f}{v}"),
        None => etag.to_string(),
    }
}

fn decode_tag(tag: &HeadTag) -> UpdateVersion {
    match tag.0.split_once('\u{1f}') {
        Some((etag, version)) => UpdateVersion {
            e_tag: Some(etag.to_string()),
            version: Some(version.to_string()),
        },
        None => UpdateVersion {
            e_tag: Some(tag.0.clone()),
            version: None,
        },
    }
}

#[async_trait]
impl HeadStore for ObjectStoreHeadStore {
    async fn read(&self, table_id: Uuid) -> Result<Option<HeadState>> {
        Ok(self
            .read_with_meta(table_id)
            .await?
            .map(|(head, tag)| HeadState { head, tag }))
    }

    async fn commit(
        &self,
        table_id: Uuid,
        table_name: &str,
        expected: Option<&HeadTag>,
        new_head: &Head,
        publish: BoxFuture<'_, Result<()>>,
    ) -> Result<HeadTag> {
        // Revalidate cheaply first so a stale caller aborts before
        // publishing anything (mirrors the local critical section).
        let current = self.read_with_meta(table_id).await?;
        match (&expected, &current) {
            (None, None) => {}
            (Some(exp), Some((_, cur_tag))) if *exp == cur_tag => {}
            _ => {
                let actual = current.as_ref().map(|(h, _)| h.sequence).unwrap_or(0);
                return Err(Error::VersionConflict {
                    table: table_name.to_string(),
                    expected: new_head.sequence.saturating_sub(1),
                    actual,
                });
            }
        }

        // Publish the manifest (create-if-absent slot) and any segments.
        publish.await?;

        // The swap: ETag-conditional put (or create for the first commit).
        let bytes = new_head.to_bytes()?;
        let mode = match expected {
            None => PutMode::Create,
            Some(tag) => PutMode::Update(decode_tag(tag)),
        };
        let res = self
            .store
            .put_opts(
                &layout::head_path(table_id),
                PutPayload::from_bytes(bytes.clone().into()),
                PutOptions::from(mode),
            )
            .await;
        match res {
            Ok(put) => {
                let etag = put.e_tag.ok_or_else(|| {
                    Error::internal("object store returned no ETag on HEAD put")
                })?;
                Ok(HeadTag(encode_tag(&etag, put.version.as_deref())))
            }
            Err(object_store::Error::Precondition { .. })
            | Err(object_store::Error::AlreadyExists { .. }) => {
                let actual = self
                    .read_with_meta(table_id)
                    .await?
                    .map(|(h, _)| h.sequence)
                    .unwrap_or(0);
                Err(Error::VersionConflict {
                    table: table_name.to_string(),
                    expected: new_head.sequence.saturating_sub(1),
                    actual,
                })
            }
            Err(e) => Err(Error::ObjectStore(e)),
        }
    }

    async fn remove(&self, table_id: Uuid) -> Result<()> {
        match self.store.delete(&layout::head_path(table_id)).await {
            Ok(()) => Ok(()),
            Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(Error::ObjectStore(e)),
        }
    }
}

impl Backend {
    /// Build a backend over any `object_store` URL (`s3://bucket/prefix`,
    /// `gs://…`, `az://…`, `memory:///`). Credentials and region come from
    /// `options` (key/value pairs passed through to the store builder, e.g.
    /// `[("aws_region","us-east-1")]`) and the standard environment.
    ///
    /// Requires a store with conditional-write support (S3, GCS, Azure,
    /// MinIO, R2, InMemory). Plain filesystems should use `Backend::local`,
    /// whose flock-based writer lock is strictly stronger; NFS is unsafe
    /// with either and unsupported (see docs/OPERATIONS.md).
    pub fn from_url<I, K, V>(url: &Url, options: I) -> Result<Self>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: Into<String>,
    {
        let (store, prefix) =
            object_store::parse_url_opts(url, options).map_err(Error::ObjectStore)?;
        // Re-root the store at the URL's path prefix so layout paths stay
        // database-relative, matching the local backend.
        let store: Arc<dyn ObjectStore> = if prefix.parts().count() > 0 {
            Arc::new(object_store::prefix::PrefixStore::new(store, prefix))
        } else {
            Arc::from(store)
        };
        Ok(Self {
            heads: Arc::new(ObjectStoreHeadStore::new(store.clone())),
            store,
            base_url: url.clone(),
            local_root: None,
        })
    }

    /// Backend over a caller-supplied store (embedders, tests).
    pub fn from_store(store: Arc<dyn ObjectStore>, base_url: Url) -> Self {
        Self {
            heads: Arc::new(ObjectStoreHeadStore::new(store.clone())),
            store,
            base_url,
            local_root: None,
        }
    }
}

/// Publish a manifest into its direct-addressed slot with create-if-absent
/// semantics — the object-store side of the "loser publishes nothing"
/// invariant. Returns `VersionConflict` when the slot is already taken.
pub(crate) async fn create_manifest_slot(
    store: &Arc<dyn ObjectStore>,
    table_name: &str,
    table_id: Uuid,
    sequence: u64,
    bytes: Vec<u8>,
) -> Result<()> {
    match store
        .put_opts(
            &layout::manifest_path(table_id, sequence),
            PutPayload::from_bytes(bytes.into()),
            PutOptions::from(PutMode::Create),
        )
        .await
    {
        Ok(_) => Ok(()),
        Err(object_store::Error::AlreadyExists { .. }) => Err(Error::VersionConflict {
            table: table_name.to_string(),
            expected: sequence.saturating_sub(1),
            actual: sequence,
        }),
        Err(e) => Err(Error::ObjectStore(e)),
    }
}
