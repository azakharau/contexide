//! In-memory implementation of `TenantsRepo` + base `Repository`.
//!
//! Thread-safe store with secondary unique indices by `name` and `email` to support
//! idempotent creation and quick lookups.

use std::{collections::HashMap, sync::Mutex};

use contexide_core::{
    errors::{Error, Result},
    prelude::TenantId,
};

use crate::{
    tenants::Tenant,
    traits::{Repository, TenantsRepo},
};

/// In-memory tenants repository.
#[derive(Default)]
pub struct MemTenantsRepo {
    // Primary storage by TenantId.
    by_id: Mutex<HashMap<TenantId, Tenant>>,
    // Unique secondary indices:
    by_name: Mutex<HashMap<String, TenantId>>,
    by_email: Mutex<HashMap<String, TenantId>>,
}

impl MemTenantsRepo {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait::async_trait]
impl Repository for MemTenantsRepo {
    type Key = TenantId;
    type Entity = Tenant;

    /// Fetch by id (clone-on-read).
    async fn get(&self, id: TenantId) -> Result<Option<Tenant>> {
        let m = self.by_id.lock().expect("poisoned");
        Ok(m.get(&id).cloned())
    }

    /// Save (create or update) and return the stored entity.
    ///
    /// Notes:
    /// - This method maintains both unique indices (`name`, `email`) to point to `entity.id`.
    /// - For simplicity in MVP, we assume callers do not mutate `name`/`email` to collide with
    ///   existing tenants. Conflict checks are done in `create()`.
    async fn save(&self, entity: Tenant) -> Result<Tenant> {
        {
            let mut by_name = self.by_name.lock().expect("poisoned");
            by_name.insert(entity.name.clone(), entity.id);
        }
        {
            let mut by_email = self.by_email.lock().expect("poisoned");
            by_email.insert(entity.email.clone(), entity.id);
        }
        let mut by_id = self.by_id.lock().expect("poisoned");
        by_id.insert(entity.id, entity.clone());
        Ok(entity)
    }

    /// Delete by id; returns whether a row was removed.
    async fn delete(&self, id: TenantId) -> Result<bool> {
        let mut by_id = self.by_id.lock().expect("poisoned");
        if let Some(old) = by_id.remove(&id) {
            drop(by_id);
            // Remove name mapping if it points to the same id.
            let mut by_name = self.by_name.lock().expect("poisoned");
            if let Some(mapped) = by_name.get(&old.name)
                && *mapped == id
            {
                by_name.remove(&old.name);
            }
            // Remove email mapping if it points to the same id.
            let mut by_email = self.by_email.lock().expect("poisoned");
            if let Some(mapped) = by_email.get(&old.email)
                && *mapped == id
            {
                by_email.remove(&old.email);
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

#[async_trait::async_trait]
impl TenantsRepo for MemTenantsRepo {
    /// Idempotent create by `(name, email)`, enforcing uniqueness of both fields.
    async fn create(&self, name: &str, email: &str) -> Result<TenantId> {
        // Fast checks on unique indices.
        let by_name_id = self.by_name.lock().expect("poisoned").get(name).cloned();
        let by_email_id = self.by_email.lock().expect("poisoned").get(email).cloned();

        match (by_name_id, by_email_id) {
            (Some(id_n), Some(id_e)) if id_n == id_e => {
                // Same tenant already registered with this (name, email).
                return Ok(id_n);
            }
            (Some(_id_n), Some(_id_e)) => {
                // Conflict: name and email belong to different tenants.
                return Err(Error::Other(anyhow::anyhow!(
                    "tenant conflict: name '{}' and email '{}' map to different tenants",
                    name,
                    email
                )));
            }
            (Some(id_n), None) => {
                // Name exists; verify email matches the same tenant.
                let m = self.by_id.lock().expect("poisoned");
                if let Some(t) = m.get(&id_n)
                    && t.email == email
                {
                    return Ok(id_n);
                }
                return Err(Error::Other(anyhow::anyhow!(
                    "tenant conflict: name '{}' already exists with different email",
                    name
                )));
            }
            (None, Some(id_e)) => {
                // Email exists; verify name matches the same tenant.
                let m = self.by_id.lock().expect("poisoned");
                if let Some(t) = m.get(&id_e)
                    && t.name == name
                {
                    return Ok(id_e);
                }
                return Err(Error::Other(anyhow::anyhow!(
                    "tenant conflict: email '{}' already exists with different name",
                    email
                )));
            }
            (None, None) => {
                // Create new tenant.
                let id = TenantId::new();
                let t = Tenant {
                    id,
                    name: name.to_string(),
                    email: email.to_string(),
                };
                self.save(t).await?;
                Ok(id)
            }
        }
    }

    async fn get_by_name(&self, name: &str) -> Result<Option<Tenant>> {
        if let Some(id) = self.by_name.lock().expect("poisoned").get(name).cloned() {
            let m = self.by_id.lock().expect("poisoned");
            Ok(m.get(&id).cloned())
        } else {
            Ok(None)
        }
    }

    async fn get_by_email(&self, email: &str) -> Result<Option<Tenant>> {
        if let Some(id) = self.by_email.lock().expect("poisoned").get(email).cloned() {
            let m = self.by_id.lock().expect("poisoned");
            Ok(m.get(&id).cloned())
        } else {
            Ok(None)
        }
    }

    async fn list(&self, limit: Option<usize>) -> Result<Vec<Tenant>> {
        let m = self.by_id.lock().expect("poisoned");
        let mut v: Vec<Tenant> = m.values().cloned().collect();
        v.sort_by(|a, b| a.id.0.cmp(&b.id.0));
        if let Some(n) = limit {
            v.truncate(n);
        }
        Ok(v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn idempotent_create_and_lookup_by_name_and_email() {
        let repo = MemTenantsRepo::new();

        let id1 = repo.create("acme", "ops@acme.io").await.unwrap();
        // Idempotent by same pair:
        let id2 = repo.create("acme", "ops@acme.io").await.unwrap();
        assert_eq!(id1, id2);

        // Lookup by name/email:
        let t1 = repo.get_by_name("acme").await.unwrap().unwrap();
        assert_eq!(t1.email, "ops@acme.io");
        let t2 = repo.get_by_email("ops@acme.io").await.unwrap().unwrap();
        assert_eq!(t2.name, "acme");

        // Conflict cases:
        let err1 = repo.create("acme", "other@acme.io").await.unwrap_err();
        assert!(err1.to_string().contains("conflict"));

        let err2 = repo.create("other", "ops@acme.io").await.unwrap_err();
        assert!(err2.to_string().contains("conflict"));

        // Delete clears indices:
        assert!(repo.delete(id1).await.unwrap());
        assert!(repo.get_by_name("acme").await.unwrap().is_none());
        assert!(repo.get_by_email("ops@acme.io").await.unwrap().is_none());
    }
}
