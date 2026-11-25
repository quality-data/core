//! The rule language of Quality Data (decisions 0019, 0033), one stage per
//! crate, gathered here:
//!
//! YAML → [`model`] (`quality-rules-model`) → [`tree`]
//! (`quality-rules-tree`) → [`check`] (`quality-rules-compile`) → [`sql`]
//! (`quality-rules-sql`) → [`lint`] (`quality-rules-lint`) and [`explain`]
//! (`quality-rules-explain`).
//!
//! Each stage depends only on the ones before it; all are pure: they read
//! text and return values, and the callers run what they compile.

pub use quality_rules_model::{json_schema, parse, At, ParseError, Position};

/// The compiler's version, as a run's manifest records it (decision 0035).
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// The rules' YAML as written.
pub mod model {
    pub use quality_rules_model::*;
}

/// The neutral expression tree.
pub mod tree {
    pub use quality_rules_tree::*;
}

/// The patterns compiled to the tree.
pub mod check {
    pub use quality_rules_compile::*;
}

/// The tree as DuckDB's and Trino's SQL.
pub mod sql {
    pub use quality_rules_sql::*;
}

/// Lint of a rules file, its examples included.
pub mod lint {
    pub use quality_rules_lint::*;
}

/// A compiled rule in words and as SQL.
pub mod explain {
    pub use quality_rules_explain::*;
}
