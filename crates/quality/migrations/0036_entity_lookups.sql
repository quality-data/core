-- An entity looked up by a typed document (decision 0090, rulings 85 and
-- 102): an `entity_key` job with no dataset; the document waits sealed
-- for its job runner, which reads it once (the row goes then); its result
-- (each dataset's ICD and band, no hash) is kept an hour for its asker.
-- Each check is replaced in one statement, only widened: the previous
-- version writes no `entity_key` job, and everything it writes still holds.
ALTER TABLE quality.jobs
    DROP CONSTRAINT jobs_kind_check,
    ADD CONSTRAINT jobs_kind_check
        CHECK (kind IN ('map', 'extract', 'evaluate', 'test_rule', 'maintain', 'tables',
                        'lake_tables', 'records', 'query', 'entity_key'));
ALTER TABLE quality.jobs
    DROP CONSTRAINT jobs_dataset_needed,
    ADD CONSTRAINT jobs_dataset_needed
        CHECK (dataset IS NOT NULL
               OR kind IN ('maintain', 'tables', 'lake_tables', 'records', 'query', 'entity_key'));

CREATE TABLE quality.lookup_documents (
    tenant_id uuid DEFAULT public.current_tenant() NOT NULL,
    job uuid NOT NULL,
    sealed text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    PRIMARY KEY (tenant_id, job),
    FOREIGN KEY (tenant_id, job) REFERENCES quality.jobs (tenant_id, id) ON DELETE CASCADE
);

CREATE TABLE quality.entity_lookups (
    tenant_id uuid DEFAULT public.current_tenant() NOT NULL,
    job uuid NOT NULL,
    result jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    PRIMARY KEY (tenant_id, job),
    FOREIGN KEY (tenant_id, job) REFERENCES quality.jobs (tenant_id, id) ON DELETE CASCADE
);

ALTER TABLE quality.lookup_documents ENABLE ROW LEVEL SECURITY;
ALTER TABLE quality.lookup_documents FORCE ROW LEVEL SECURITY;
CREATE POLICY isolation ON quality.lookup_documents
    USING (tenant_id = public.current_tenant())
    WITH CHECK (tenant_id = public.current_tenant());
ALTER TABLE quality.entity_lookups ENABLE ROW LEVEL SECURITY;
ALTER TABLE quality.entity_lookups FORCE ROW LEVEL SECURITY;
CREATE POLICY isolation ON quality.entity_lookups
    USING (tenant_id = public.current_tenant())
    WITH CHECK (tenant_id = public.current_tenant());
