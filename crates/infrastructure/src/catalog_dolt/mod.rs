//! The versioned catalog on Dolt (decision 0021): one database per tenant,
//! `main` what runs read, each change a commit. For now a dataset's
//! mapping and its rules files, as YAML: `datasets`, `mappings` and
//! `rules` (one row per rules file, the field group of decision 0034).
//! Drafts on branches, their diff and blame, and approval by merge come
//! with the editor.
//!
//! The application's user (`qd_app`) holds only the tenants' databases,
//! created by root (`infra/dolt/tenant.sh`); the tables it creates itself,
//! on first use, in one commit.

use std::collections::HashMap;
use std::sync::Mutex;

use sqlx::mysql::{MySqlPool, MySqlPoolOptions};
use sqlx::Row;

use crate::oidc_es256::key_from_env;

mod drafts;
mod edits;
mod gate;
mod history;
mod password;
mod restart;

pub use drafts::{Draft, Merged};
pub use edits::{FileEdit, FileSaved, FilesEdit, MappingEdit};
pub use gate::RESTARTING;
pub use history::{
    first_parent_changes, is_commit, DraftCommit, DraftHistory, FileChange, MainChange,
};
pub use restart::{DoltHold, HOLD_WAIT};

const SCHEMA: [&str; 3] = [
    "CREATE TABLE IF NOT EXISTS datasets (
        name VARCHAR(128) PRIMARY KEY,
        description TEXT)",
    "CREATE TABLE IF NOT EXISTS mappings (
        dataset VARCHAR(128) PRIMARY KEY,
        body LONGTEXT NOT NULL)",
    "CREATE TABLE IF NOT EXISTS rules (
        dataset VARCHAR(128) NOT NULL,
        file VARCHAR(128) NOT NULL,
        body LONGTEXT NOT NULL,
        PRIMARY KEY (dataset, file))",
];

/// A dataset's catalog at one commit of `main`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogFiles {
    pub commit: String,
    pub mapping: String,
    /// `(file, yaml)`, by file name.
    pub rules: Vec<(String, String)>,
    /// The other datasets' mappings, `(dataset, yaml)`: what the rules'
    /// references read.
    pub others: Vec<(String, String)>,
}

/// What an import writes for one dataset.
#[derive(Debug, Clone)]
pub struct DatasetImport<'a> {
    pub dataset: &'a str,
    pub description: Option<&'a str>,
    pub mapping: &'a str,
    pub rules: &'a [(String, String)],
}

/// The tenants' catalogs on one Dolt server: a pool per database, every
/// connection back on `main` when it returns to its pool. Every operation
/// passes its gate, so that Dolt can be restarted when free (`restart`).
pub struct DoltCatalog {
    base: String,
    pools: Mutex<HashMap<String, MySqlPool>>,
    gate: gate::Gate,
}

impl std::fmt::Debug for DoltCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DoltCatalog").finish_non_exhaustive()
    }
}

impl DoltCatalog {
    /// `base`: `mysql://user:password@host:port`, without a database, and
    /// its options after `?` (with certificates: `ssl-mode=VERIFY_IDENTITY`,
    /// `ssl-ca`, `ssl-cert`, `ssl-key`, decision 0066).
    pub fn new(base: &str) -> Self {
        Self {
            base: base.to_string(),
            pools: Mutex::new(HashMap::new()),
            gate: gate::Gate::new(HOLD_WAIT),
        }
    }

    /// The URL of `database`: the base's path, then its options.
    fn url(&self, database: &str) -> String {
        let (server, options) = match self.base.split_once('?') {
            Some((server, options)) => (server, Some(options)),
            None => (self.base.as_str(), None),
        };
        let server = server.trim_end_matches('/');
        match options {
            Some(options) => format!("{server}/{database}?{options}"),
            None => format!("{server}/{database}"),
        }
    }

    /// From `DOLT_URL` (or its `_FILE`); `None` without it. With
    /// `DOLT_PASSWORD` (or its `_FILE`, as an installation has it), the URL
    /// names the user only and the password is written into it.
    pub fn from_env() -> Result<Option<Self>, String> {
        let Some(url) = key_from_env("DOLT_URL")? else {
            return Ok(None);
        };
        let url = match key_from_env("DOLT_PASSWORD")? {
            Some(password) => password::with_password(&url, &password)?,
            None => url,
        };
        Ok(Some(Self::new(&url)))
    }

    /// The pool of `database`, once the gate lets the operation in; the
    /// entry is held for as long as the operation runs.
    async fn pool(&self, database: &str) -> Result<(MySqlPool, gate::Entry), sqlx::Error> {
        let entry = self
            .gate
            .enter()
            .await
            .map_err(|e| sqlx::Error::Io(std::io::Error::other(e)))?;
        Ok((self.pool_now(database)?, entry))
    }

    fn pool_now(&self, database: &str) -> Result<MySqlPool, sqlx::Error> {
        let mut pools = self.pools.lock().expect("catalog pools");
        if let Some(pool) = pools.get(database) {
            return Ok(pool.clone());
        }
        let pool = MySqlPoolOptions::new()
            .max_connections(4)
            // A connection that checked out a branch must not lend it on.
            .after_release(|conn, _| {
                Box::pin(async move {
                    sqlx::query("CALL DOLT_CHECKOUT('main')")
                        .execute(&mut *conn)
                        .await?;
                    Ok(true)
                })
            })
            .connect_lazy(&self.url(database))?;
        pools.insert(database.to_string(), pool.clone());
        Ok(pool)
    }

    /// The dataset's mapping and rules on `main` now, with its commit;
    /// `None` if the catalog has no mapping for it.
    pub async fn files(
        &self,
        database: &str,
        dataset: &str,
    ) -> Result<Option<CatalogFiles>, sqlx::Error> {
        self.files_at(database, "main", dataset).await
    }

    /// The dataset's mapping and rules as the branch `reference` has them
    /// now (`main`, or a draft), with its commit; `None` if the branch or
    /// the mapping is not there.
    pub async fn files_at(
        &self,
        database: &str,
        reference: &str,
        dataset: &str,
    ) -> Result<Option<CatalogFiles>, sqlx::Error> {
        let (pool, entry) = self.pool(database).await?;
        let mut conn = pool.acquire().await?;
        let commit: Option<String> =
            sqlx::query_scalar("SELECT hash FROM dolt_branches WHERE name = ?")
                .bind(reference)
                .fetch_optional(&mut *conn)
                .await?;
        let Some(commit) = commit else {
            return Ok(None);
        };
        drop(conn);
        drop(entry);
        self.files_at_commit(database, &commit, dataset).await
    }

    /// The dataset's mapping and rules at `commit` (a run's manifest's,
    /// decision 0090 step 22's backfill); `None` if it had no mapping then.
    pub async fn files_at_commit(
        &self,
        database: &str,
        commit: &str,
        dataset: &str,
    ) -> Result<Option<CatalogFiles>, sqlx::Error> {
        let commit = commit.to_string();
        // `AS OF` the commit: the mapping and the rules of the same version.
        // Dolt takes no parameter there; a hash is letters and digits only.
        if commit.is_empty() || !commit.chars().all(|c| c.is_ascii_alphanumeric()) {
            return Err(sqlx::Error::Protocol(format!(
                "unexpected commit hash {commit:?}"
            )));
        }
        let (pool, _entry) = self.pool(database).await?;
        let mut conn = pool.acquire().await?;
        let mapping: Option<String> = match sqlx::query_scalar(&format!(
            "SELECT body FROM mappings AS OF '{commit}' WHERE dataset = ?"
        ))
        .bind(dataset)
        .fetch_optional(&mut *conn)
        .await
        {
            Ok(m) => m,
            // A database with no catalog yet: no table to read.
            Err(sqlx::Error::Database(e)) if e.message().contains("table not found") => None,
            Err(e) => return Err(e),
        };
        let Some(mapping) = mapping else {
            return Ok(None);
        };
        let rules = sqlx::query(&format!(
            "SELECT file, body FROM rules AS OF '{commit}' WHERE dataset = ? ORDER BY file"
        ))
        .bind(dataset)
        .fetch_all(&mut *conn)
        .await?
        .iter()
        .map(|r| Ok((r.try_get("file")?, r.try_get("body")?)))
        .collect::<Result<_, sqlx::Error>>()?;
        let others = sqlx::query(&format!(
            "SELECT dataset, body FROM mappings AS OF '{commit}' WHERE dataset <> ? ORDER BY dataset"
        ))
        .bind(dataset)
        .fetch_all(&mut *conn)
        .await?
        .iter()
        .map(|r| Ok((r.try_get("dataset")?, r.try_get("body")?)))
        .collect::<Result<_, sqlx::Error>>()?;
        Ok(Some(CatalogFiles {
            commit,
            mapping,
            rules,
            others,
        }))
    }

    /// Every dataset's mapping on `main` now, `(dataset, yaml)` by dataset;
    /// none if the database has no catalog yet.
    pub async fn mappings(&self, database: &str) -> Result<Vec<(String, String)>, sqlx::Error> {
        let (pool, _entry) = self.pool(database).await?;
        // A connection is on `main` whenever it leaves its pool.
        match sqlx::query("SELECT dataset, body FROM mappings ORDER BY dataset")
            .fetch_all(&pool)
            .await
        {
            Ok(rows) => rows
                .iter()
                .map(|r| Ok((r.try_get("dataset")?, r.try_get("body")?)))
                .collect(),
            Err(sqlx::Error::Database(e)) if e.message().contains("table not found") => {
                Ok(Vec::new())
            }
            Err(e) => Err(e),
        }
    }

    /// Writes a dataset's mapping and rules on `main`, replacing its rules
    /// files, as one commit by `author` (`Name <email>`). Returns the
    /// commit `main` is on after it (the same if nothing changed).
    pub async fn import(
        &self,
        database: &str,
        import: &DatasetImport<'_>,
        author: &str,
    ) -> Result<String, sqlx::Error> {
        let (pool, _entry) = self.pool(database).await?;
        let mut conn = pool.acquire().await?;
        sqlx::query("CALL DOLT_CHECKOUT('main')")
            .execute(&mut *conn)
            .await?;
        write(
            &mut conn,
            import,
            author,
            &format!("catalog: import {}", import.dataset),
        )
        .await?;
        sqlx::query_scalar("SELECT HASHOF('main')")
            .fetch_one(&mut *conn)
            .await
    }
}

impl DoltCatalog {
    /// Removes a dataset from `main`, its mapping and rules, as one commit
    /// by `author` (decision 0055); the catalog's history keeps them.
    /// `false` if `main` had none of it.
    pub async fn remove(
        &self,
        database: &str,
        dataset: &str,
        author: &str,
    ) -> Result<bool, sqlx::Error> {
        let (pool, _entry) = self.pool(database).await?;
        let mut conn = pool.acquire().await?;
        sqlx::query("CALL DOLT_CHECKOUT('main')")
            .execute(&mut *conn)
            .await?;
        for sql in SCHEMA {
            sqlx::query(sql).execute(&mut *conn).await?;
        }
        for sql in [
            "DELETE FROM rules WHERE dataset = ?",
            "DELETE FROM mappings WHERE dataset = ?",
            "DELETE FROM datasets WHERE name = ?",
        ] {
            sqlx::query(sql).bind(dataset).execute(&mut *conn).await?;
        }
        let changed: i64 = sqlx::query_scalar("SELECT count(*) FROM dolt_status")
            .fetch_one(&mut *conn)
            .await?;
        if changed > 0 {
            sqlx::query("CALL DOLT_COMMIT('-Am', ?, '--author', ?)")
                .bind(format!("catalog: delete {dataset}"))
                .bind(author)
                .execute(&mut *conn)
                .await?;
        }
        Ok(changed > 0)
    }
}

/// Replaces a dataset's mapping and rules on the branch `conn` is on, and
/// commits it as `author` if anything changed.
async fn write(
    conn: &mut sqlx::pool::PoolConnection<sqlx::MySql>,
    import: &DatasetImport<'_>,
    author: &str,
    message: &str,
) -> Result<(), sqlx::Error> {
    for sql in SCHEMA {
        sqlx::query(sql).execute(&mut **conn).await?;
    }
    sqlx::query("REPLACE INTO datasets (name, description) VALUES (?, ?)")
        .bind(import.dataset)
        .bind(import.description)
        .execute(&mut **conn)
        .await?;
    sqlx::query("REPLACE INTO mappings (dataset, body) VALUES (?, ?)")
        .bind(import.dataset)
        .bind(import.mapping)
        .execute(&mut **conn)
        .await?;
    sqlx::query("DELETE FROM rules WHERE dataset = ?")
        .bind(import.dataset)
        .execute(&mut **conn)
        .await?;
    for (file, body) in import.rules {
        sqlx::query("INSERT INTO rules (dataset, file, body) VALUES (?, ?, ?)")
            .bind(import.dataset)
            .bind(file)
            .bind(body)
            .execute(&mut **conn)
            .await?;
    }
    let changed: i64 = sqlx::query_scalar("SELECT count(*) FROM dolt_status")
        .fetch_one(&mut **conn)
        .await?;
    if changed > 0 {
        sqlx::query("CALL DOLT_COMMIT('-Am', ?, '--author', ?)")
            .bind(message)
            .bind(author)
            .execute(&mut **conn)
            .await?;
    }
    Ok(())
}
