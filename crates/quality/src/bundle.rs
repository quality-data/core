//! A job's signed statement bundle (decision 0090, point 2): every
//! statement a job runner may submit for one job, compiled by the core and
//! signed with the OIDC provider's ES256 key, which job runners already
//! verify (ruled by the user 2026-11-25). The module reaches the key
//! through [`BundleSigner`], a port the server wires to the provider; it
//! never depends on `auth-gateway`. A statement not in a verified bundle
//! is never run.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

mod steps;

pub use steps::{fill, tokens, OnlyIfRows, Placeholder, PlaceholderFrom, PlaceholderType};
use steps::{is_count_read, one_row_column};

/// The kind whose bundle may carry a typed document (ruling 102).
pub const ENTITY_KEY: &str = "entity_key";

/// The JWS `typ` of a bundle, so no other token passes for one.
pub const BUNDLE_TYP: &str = "qd-bundle+jwt";

/// What a statement's answer must look like (point 7.3); anything else
/// fails the job and its data is discarded. The wire shape is the one
/// `engine-trino`'s job runner proposed and the user accepted (2026-11-25).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Expect {
    /// A write: one of these update types (`INSERT`, `DELETE`) with one
    /// `bigint` row, the rows written.
    Count { update_types: Vec<String> },
    /// A read: exactly these columns, as `(name, Trino type)`, at most
    /// `max_rows` rows (counts read back, samples and pages, point 7.4;
    /// snapshot ids from `$snapshots`).
    Rows {
        columns: Vec<(String, String)>,
        max_rows: u64,
    },
    /// A statement that answers nothing but success (`CREATE`, `ALTER`).
    Done,
    /// A read whose rows carry values (rulings 79 and 80): exactly these
    /// columns, at most `max_rows` rows, never more than
    /// [`MAX_VALUES_ROWS`]. Run as the job's own user; its rows go to
    /// `POST …/runs/{run}/rows`, never into the report, and the job runner
    /// logs none of them.
    Values {
        columns: Vec<(String, String)>,
        max_rows: u64,
    },
    /// A values read whose columns come with its answer, every value text
    /// (ruling 84: the console's statement, whose columns are not known
    /// before it runs); at most `max_rows`, never more than
    /// [`MAX_VALUES_ROWS`], posted on the same values route.
    Text { max_rows: u64 },
}

/// The most rows a values read may give: a page of a source's records and
/// the one more that says there are others (decision 0049).
pub const MAX_VALUES_ROWS: u64 = 1_001;

impl Expect {
    /// An `INSERT`'s count.
    pub fn insert() -> Self {
        Self::Count {
            update_types: vec!["INSERT".into()],
        }
    }

    /// A `DELETE`'s count.
    pub fn delete() -> Self {
        Self::Count {
            update_types: vec!["DELETE".into()],
        }
    }

    /// Whether it is a values read.
    pub fn is_values(&self) -> bool {
        matches!(self, Self::Values { .. } | Self::Text { .. })
    }
}

/// Who a statement runs as: the job's own user, or its dataset's report
/// user, which reads gold only (point 7.3).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunAs {
    #[default]
    Job,
    Report,
}

/// One statement of a bundle. Its SQL is complete: nothing is bound to it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BundleStatement {
    /// Its id within the job (`[a-z0-9_]`, at most 64), as the job runner
    /// reports it.
    pub id: String,
    pub sql: String,
    pub expects: Expect,
    #[serde(default)]
    pub run_as: RunAs,
    /// Whether it reads the source, and so takes the source's secrets as
    /// extra credentials (point 7.5).
    #[serde(default)]
    pub reads_source: bool,
    /// Run only when an earlier count of the bundle showed rows (amendment
    /// A1 of decision 0090): gold's count-then-delete, and the guard counts
    /// that pick one percentile method.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub only_if_rows: Option<OnlyIfRows>,
    /// The gold table it writes, `<catalog>.<schema>.<table>`, whose
    /// mutex the operator holds while it runs (decision 0090, ruling
    /// 92(b): one writer per gold table, point 4); signed with the rest.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lock: Option<String>,
}

/// The gold table a statement writes (`INSERT INTO` or `DELETE FROM` one
/// of `lake.quality_gold`'s tables), as its lock names it.
pub fn gold_lock(sql: &str) -> Option<String> {
    const GOLD: &str = "\"lake\".\"quality_gold\".\"";
    let rest = sql
        .strip_prefix("INSERT INTO ")
        .or_else(|| sql.strip_prefix("DELETE FROM "))?
        .strip_prefix(GOLD)?;
    let table = &rest[..rest.find('"')?];
    is_statement_id(table).then(|| format!("lake.quality_gold.{table}"))
}

/// The job contract (point 2).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Bundle {
    pub job_id: String,
    pub kind: String,
    pub dataset: Option<String>,
    pub cut: Option<String>,
    pub attempt: u32,
    pub run_id: Option<String>,
    pub run_seq: Option<u64>,
    pub catalog_commit: Option<String>,
    pub compiler_version: String,
    /// The tokens its statements hold, declared before any is filled.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub placeholders: Vec<Placeholder>,
    pub statements: Vec<BundleStatement>,
}

/// Whether `id` is a statement id the job runner accepts.
pub fn is_statement_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 64
        && id
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
}

impl Bundle {
    /// Its canonical JSON: the fields in the order declared, no spaces.
    pub fn canonical(&self) -> String {
        serde_json::to_string(self).expect("a bundle serializes")
    }

    /// The digest the operator carries with the job: the lower-case hex
    /// SHA-256 of the canonical JSON.
    pub fn digest(&self) -> String {
        format!("{:x}", Sha256::digest(self.canonical().as_bytes()))
    }

    /// Whether the job runner will take it: statements, each with a well
    /// formed id used once.
    pub fn well_formed(&self) -> bool {
        let mut ids = std::collections::BTreeSet::new();
        !self.statements.is_empty()
            && self
                .statements
                .iter()
                .all(|s| is_statement_id(&s.id) && ids.insert(s.id.as_str()))
            && self.placeholders_declared()
            && self
                .statements
                .iter()
                .enumerate()
                .all(|(i, s)| self.steps_checked(i, s))
            && self.statements.iter().all(values_checked)
            && self
                .statements
                .iter()
                .all(|s| s.lock.is_none() || s.lock == gold_lock(&s.sql))
    }

    /// The statement `id` among the first `before`, if any.
    fn earlier(&self, id: &str, before: usize) -> Option<&BundleStatement> {
        self.statements[..before].iter().find(|s| s.id == id)
    }

    /// Each placeholder named once, read from a column of an earlier
    /// one-row answer whose type it takes (`bigint`).
    fn placeholders_declared(&self) -> bool {
        let mut names = std::collections::BTreeSet::new();
        self.placeholders.iter().all(|p| {
            let from_answer = match (&p.ty, &p.from) {
                // A typed document: only an entity lookup's (ruling 102).
                (PlaceholderType::Text, None) => self.kind == ENTITY_KEY,
                (PlaceholderType::Text, Some(_)) | (_, None) => false,
                (_, Some(from)) => {
                    let source = self.statements.iter().find(|s| s.id == from.statement);
                    source.is_some_and(|s| one_row_column(&s.expects, &from.column))
                }
            };
            is_statement_id(&p.name) && names.insert(p.name.as_str()) && from_answer
        })
    }

    /// A statement's tokens declared and filled from statements before it,
    /// and its `only_if_rows` waiting on an earlier count.
    fn steps_checked(&self, i: usize, s: &BundleStatement) -> bool {
        let filled = tokens(&s.sql).is_some_and(|names| {
            names.iter().all(|n| {
                self.placeholders
                    .iter()
                    .find(|p| p.name == *n)
                    .is_some_and(|p| match &p.from {
                        Some(from) => self.earlier(&from.statement, i).is_some(),
                        // A document goes only into the lookup's values read.
                        None => s.expects.is_values(),
                    })
            })
        });
        let guarded = s.only_if_rows.as_ref().is_none_or(|g| {
            self.earlier(&g.after, i)
                .is_some_and(|c| is_count_read(&c.expects))
        });
        filled && guarded
    }
}

/// A values read within the hard limit, with columns, as the job's own
/// user: the report user reads gold, which holds no values.
fn values_checked(s: &BundleStatement) -> bool {
    match &s.expects {
        Expect::Values { columns, max_rows } => {
            !columns.is_empty()
                && (1..=MAX_VALUES_ROWS).contains(max_rows)
                && s.run_as == RunAs::Job
        }
        Expect::Text { max_rows } => {
            (1..=MAX_VALUES_ROWS).contains(max_rows) && s.run_as == RunAs::Job
        }
        _ => true,
    }
}

/// The port to the provider's signing key.
pub trait BundleSigner: Send + Sync {
    /// A compact JWS of `claims` (a JSON object) with `typ` in its header.
    fn sign(&self, typ: &str, claims: &str) -> String;
    /// The claims of a JWS this key signed with `typ`, if it verifies.
    fn verify(&self, typ: &str, token: &str) -> Option<String>;
}

/// Why a bundle was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BundleError {
    /// The signature, the key or the `typ` does not check out.
    Signature,
    /// It verified but is not a bundle.
    Shape,
    /// Not the job's, or not the digest the operator carries.
    NotThisJob,
}

/// The signed bundle.
pub fn sign(signer: &dyn BundleSigner, bundle: &Bundle) -> String {
    signer.sign(BUNDLE_TYP, &bundle.canonical())
}

/// A signed bundle checked as a job runner checks it: the signature, then
/// that it is the job's and has the digest the operator handed out.
pub fn verify(
    signer: &dyn BundleSigner,
    token: &str,
    job_id: &str,
    digest: &str,
) -> Result<Bundle, BundleError> {
    let claims = signer
        .verify(BUNDLE_TYP, token)
        .ok_or(BundleError::Signature)?;
    let bundle: Bundle = serde_json::from_str(&claims).map_err(|_| BundleError::Shape)?;
    if bundle.job_id != job_id || bundle.digest() != digest || !bundle.well_formed() {
        return Err(BundleError::NotThisJob);
    }
    Ok(bundle)
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use base64::Engine;

    /// A stand-in key: `typ.payload.mac`, the MAC a keyed SHA-256. The real
    /// one is the provider's ES256 key, tested where the server wires it.
    struct Fake(&'static str);

    impl Fake {
        fn mac(&self, typ: &str, payload: &str) -> String {
            format!(
                "{:x}",
                Sha256::digest(format!("{}|{typ}|{payload}", self.0).as_bytes())
            )
        }
    }

    impl BundleSigner for Fake {
        fn sign(&self, typ: &str, claims: &str) -> String {
            let payload = URL_SAFE_NO_PAD.encode(claims);
            format!("{typ}.{payload}.{}", self.mac(typ, &payload))
        }
        fn verify(&self, typ: &str, token: &str) -> Option<String> {
            let mut parts = token.split('.');
            let (t, payload, mac) = (parts.next()?, parts.next()?, parts.next()?);
            (t == typ && mac == self.mac(typ, payload))
                .then(|| String::from_utf8(URL_SAFE_NO_PAD.decode(payload).ok()?).ok())?
        }
    }

    fn bundle() -> Bundle {
        Bundle {
            job_id: "0b9c6a1e-0000-4000-8000-000000000001".into(),
            kind: "evaluate".into(),
            dataset: Some("loans".into()),
            cut: Some("2026-10-31".into()),
            attempt: 1,
            run_id: Some("r-1".into()),
            run_seq: Some(7),
            catalog_commit: Some("c0ffee".into()),
            compiler_version: "quality-rules 0.1.0".into(),
            placeholders: vec![],
            statements: vec![BundleStatement {
                id: "rules".into(),
                sql: "INSERT INTO t SELECT 1".into(),
                expects: Expect::insert(),
                run_as: RunAs::Job,
                reads_source: false,
                only_if_rows: None,
                lock: None,
            }],
        }
    }

    #[test]
    fn a_signed_bundle_round_trips_for_its_job_and_digest() {
        let key = Fake("k1");
        let b = bundle();
        let token = sign(&key, &b);
        assert_eq!(verify(&key, &token, &b.job_id, &b.digest()), Ok(b.clone()));
        assert_eq!(b.digest().len(), 64);
        let rows = Expect::Rows {
            columns: vec![("n".into(), "bigint".into())],
            max_rows: 1,
        };
        assert_eq!(
            serde_json::to_string(&rows).unwrap(),
            "{\"kind\":\"rows\",\"columns\":[[\"n\",\"bigint\"]],\"max_rows\":1}"
        );
        assert!(b.canonical().starts_with("{\"job_id\":"));
        assert!(b.canonical().contains(
            "\"expects\":{\"kind\":\"count\",\"update_types\":[\"INSERT\"]},\"run_as\":\"job\""
        ));
    }

    #[test]
    fn a_tampered_bundle_another_key_or_another_job_is_refused() {
        let key = Fake("k1");
        let b = bundle();
        let token = sign(&key, &b);
        let mut evil = b.clone();
        evil.statements[0].sql = "SELECT * FROM lake.loans_silver.loans".into();
        let payload = URL_SAFE_NO_PAD.encode(evil.canonical());
        let parts: Vec<&str> = token.split('.').collect();
        let tampered = format!("{}.{payload}.{}", parts[0], parts[2]);
        assert_eq!(
            verify(&key, &tampered, &b.job_id, &b.digest()),
            Err(BundleError::Signature)
        );
        assert_eq!(
            verify(&Fake("k2"), &token, &b.job_id, &b.digest()),
            Err(BundleError::Signature)
        );
        assert_eq!(
            verify(&key, &token, "another-job", &b.digest()),
            Err(BundleError::NotThisJob)
        );
        assert_eq!(
            verify(&key, &token, &b.job_id, &evil.digest()),
            Err(BundleError::NotThisJob)
        );
        let other = key.sign("JWT", &b.canonical());
        assert_eq!(
            verify(&key, &other, &b.job_id, &b.digest()),
            Err(BundleError::Signature)
        );
        let mut repeated = b.clone();
        repeated.statements.push(b.statements[0].clone());
        let token = sign(&key, &repeated);
        assert_eq!(
            verify(&key, &token, &b.job_id, &repeated.digest()),
            Err(BundleError::NotThisJob),
            "a repeated statement id"
        );
        let not_a_bundle = key.sign(BUNDLE_TYP, "{\"sub\":\"x\"}");
        assert_eq!(
            verify(&key, &not_a_bundle, &b.job_id, &b.digest()),
            Err(BundleError::Shape)
        );
    }

    #[test]
    fn a_values_read_is_bounded_and_run_as_the_job() {
        let values = |max_rows| Expect::Values {
            columns: vec![("name".into(), "varchar".into())],
            max_rows,
        };
        let mut b = bundle();
        b.statements[0].expects = values(MAX_VALUES_ROWS);
        b.statements[0].reads_source = true;
        assert!(b.well_formed());
        assert!(b.canonical().contains(
            "\"expects\":{\"kind\":\"values\",\"columns\":[[\"name\",\"varchar\"]],\"max_rows\":1001}"
        ));
        let refused = |change: &dyn Fn(&mut BundleStatement)| {
            let mut b = bundle();
            b.statements[0].expects = values(10);
            change(&mut b.statements[0]);
            !b.well_formed()
        };
        assert!(refused(&|s| s.expects = values(MAX_VALUES_ROWS + 1)));
        assert!(refused(&|s| s.expects = values(0)));
        assert!(refused(&|s| s.run_as = RunAs::Report));
        assert!(!refused(&|s| s.expects = Expect::Text { max_rows: 201 }));
        assert!(refused(&|s| s.expects = Expect::Text { max_rows: 0 }));
        assert!(refused(&|s| {
            s.expects = Expect::Text { max_rows: 201 };
            s.run_as = RunAs::Report;
        }));
        assert!(refused(&|s| {
            s.expects = Expect::Values {
                columns: vec![],
                max_rows: 1,
            }
        }));
    }

    #[test]
    fn a_gold_write_is_locked_by_its_table_and_nothing_else_is() {
        assert_eq!(
            gold_lock("INSERT INTO \"lake\".\"quality_gold\".\"rule_results\" (dataset) SELECT 1")
                .as_deref(),
            Some("lake.quality_gold.rule_results")
        );
        assert_eq!(
            gold_lock("DELETE FROM \"lake\".\"quality_gold\".\"findings\" WHERE run_id = 'r'")
                .as_deref(),
            Some("lake.quality_gold.findings")
        );
        for sql in [
            "SELECT * FROM \"lake\".\"quality_gold\".\"findings\"",
            "INSERT INTO \"lake\".\"loans_bronze\".\"loans\" SELECT 1",
            "DELETE FROM \"lake\".\"quality_scratch\".\"record_icd\"",
        ] {
            assert_eq!(gold_lock(sql), None, "{sql}");
        }
        let mut b = bundle();
        b.statements[0].sql = "INSERT INTO \"lake\".\"quality_gold\".\"cells\" SELECT 1".into();
        b.statements[0].lock = gold_lock(&b.statements[0].sql);
        assert!(b.well_formed());
        assert!(b
            .canonical()
            .contains("\"lock\":\"lake.quality_gold.cells\""));
        b.statements[0].lock = Some("lake.quality_gold.findings".into());
        assert!(!b.well_formed(), "a lock that is not its table's");
    }
}
