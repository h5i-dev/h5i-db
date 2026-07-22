//! Shared lifecycle helpers for disposable performance sidecars.

use h5i_db_core::Backend;
use object_store::path::Path as ObjectPath;

pub(crate) async fn enforce_budget(
    backend: &Backend,
    prefix: &str,
    max_bytes: u64,
) -> h5i_db_core::Result<usize> {
    let mut objects = backend.list(&ObjectPath::from(prefix)).await?;
    let mut total = objects.iter().map(|object| object.size).sum::<u64>();
    objects.sort_by_key(|object| object.last_modified);
    let mut evictions = 0;
    for object in objects {
        if total <= max_bytes {
            break;
        }
        backend.delete(&object.location).await?;
        total = total.saturating_sub(object.size);
        evictions += 1;
    }
    Ok(evictions)
}
