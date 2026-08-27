//! In-memory implementation of `JobsRepo` + base `Repository`.
//!
//! Thread-safe store with secondary indices by (kind, status) for quick scans.
//! Suitable for unit tests and local development.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use contexide_core::errors::Result;
use contexide_core::prelude::{JobId, TenantId};

use super::{Job, JobKind, JobStatus};
use crate::traits::{JobsRepo, Repository};

/// In-memory jobs repository.
#[derive(Default)]
pub struct MemJobsRepo {
    // Primary storage by JobId.
    by_id: Mutex<HashMap<JobId, Job>>,
    // Secondary index: (kind, status) -> set of JobId.
    by_kind_status: Mutex<HashMap<(JobKind, JobStatus), HashSet<JobId>>>,
}

impl MemJobsRepo {
    pub fn new() -> Self {
        Self::default()
    }

    fn idx_add(&self, kind: JobKind, status: JobStatus, id: JobId) {
        let mut idx = self.by_kind_status.lock().expect("poisoned");
        idx.entry((kind, status)).or_default().insert(id);
    }

    fn idx_move(&self, id: JobId, old: (JobKind, JobStatus), new: (JobKind, JobStatus)) {
        let mut idx = self.by_kind_status.lock().expect("poisoned");
        if let Some(set) = idx.get_mut(&old) {
            set.remove(&id);
            if set.is_empty() {
                idx.remove(&old);
            }
        }
        idx.entry(new).or_default().insert(id);
    }

    fn idx_remove(&self, kind: JobKind, status: JobStatus, id: JobId) {
        let mut idx = self.by_kind_status.lock().expect("poisoned");
        if let Some(set) = idx.get_mut(&(kind, status)) {
            set.remove(&id);
            if set.is_empty() {
                idx.remove(&(kind, status));
            }
        }
    }
}

#[async_trait::async_trait]
impl Repository for MemJobsRepo {
    type Key = JobId;
    type Entity = Job;

    /// Fetch by id (clone-on-read).
    async fn get(&self, id: JobId) -> Result<Option<Job>> {
        let m = self.by_id.lock().expect("poisoned");
        Ok(m.get(&id).cloned())
    }

    /// Save (create or update) and return the stored entity.
    ///
    /// Semantics:
    /// - Insert if missing and maintain index.
    /// - On update, if (kind,status) changed, move index entry.
    async fn save(&self, entity: Job) -> Result<Job> {
        let mut by_id = self.by_id.lock().expect("poisoned");

        match by_id.insert(entity.id, entity.clone()) {
            None => {
                // New record.
                drop(by_id);
                self.idx_add(entity.kind, entity.status, entity.id);
            }
            Some(old) => {
                if (old.kind, old.status) != (entity.kind, entity.status) {
                    drop(by_id);
                    self.idx_move(
                        entity.id,
                        (old.kind, old.status),
                        (entity.kind, entity.status),
                    );
                }
            }
        }

        Ok(entity)
    }

    /// Delete by id; returns whether a row was removed.
    async fn delete(&self, id: JobId) -> Result<bool> {
        let mut by_id = self.by_id.lock().expect("poisoned");
        if let Some(old) = by_id.remove(&id) {
            drop(by_id);
            self.idx_remove(old.kind, old.status, id);
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

#[async_trait::async_trait]
impl JobsRepo for MemJobsRepo {
    async fn create(
        &self,
        tenant_id: TenantId,
        kind: JobKind,
        status: JobStatus,
        payload_json: Option<String>,
    ) -> Result<JobId> {
        let id = JobId::new();
        let job = Job {
            id,
            tenant_id,
            kind,
            status,
            payload_json,
        };
        self.save(job).await?;
        Ok(id)
    }

    async fn set_status(&self, id: JobId, status: JobStatus) -> Result<bool> {
        let mut m = self.by_id.lock().expect("poisoned");
        if let Some(job) = m.get_mut(&id) {
            let old = (job.kind, job.status);
            job.status = status;
            self.idx_move(id, old, (job.kind, job.status));
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn list_by_kind_status(
        &self,
        kind: JobKind,
        status: JobStatus,
        limit: Option<usize>,
    ) -> Result<Vec<Job>> {
        let ids: Vec<JobId> = {
            let idx = self.by_kind_status.lock().expect("poisoned");
            idx.get(&(kind, status))
                .map(|s| s.iter().cloned().collect())
                .unwrap_or_default()
        };

        let store = self.by_id.lock().expect("poisoned");
        let mut out: Vec<Job> = ids
            .into_iter()
            .filter_map(|id| store.get(&id).cloned())
            .collect();

        // Deterministic order by id (UUIDv7 is roughly time-ordered; we still sort).
        out.sort_by(|a, b| a.id.0.cmp(&b.id.0));

        if let Some(n) = limit {
            out.truncate(n);
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use contexide_core::prelude::TenantId;

    #[tokio::test]
    async fn create_get_transition_list_delete() {
        let repo = MemJobsRepo::new();
        let t = TenantId::new();

        let id = repo
            .create(
                t,
                JobKind::Embed,
                JobStatus::Pending,
                Some("{\"es\":1}".into()),
            )
            .await
            .unwrap();

        let j = repo.get(id).await.unwrap().unwrap();
        assert!(matches!(j.kind, JobKind::Embed));
        assert!(matches!(j.status, JobStatus::Pending));

        assert!(repo.set_status(id, JobStatus::Running).await.unwrap());
        let j2 = repo.get(id).await.unwrap().unwrap();
        assert!(matches!(j2.status, JobStatus::Running));

        let list = repo
            .list_by_kind_status(JobKind::Embed, JobStatus::Running, None)
            .await
            .unwrap();
        assert_eq!(list.len(), 1);

        assert!(repo.delete(id).await.unwrap());
        assert!(repo.get(id).await.unwrap().is_none());
    }
}
