-- The source a run's bundle reads when its job is a source's, not a
-- dataset's (a `tables` listing, decision 0090, ruling 68): its Trino user
-- is `job_source_<source>` and its secrets are that source's. Only adds.

ALTER TABLE quality.job_bundles ADD COLUMN source text;
