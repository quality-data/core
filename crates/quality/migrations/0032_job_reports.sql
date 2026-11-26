-- Each run's report from its job runner (decision 0090, ruling 75): its
-- state, the class of its error, and what its statements answered (counts
-- only, the rows of the bundle's `Rows` reads), checked against the run's
-- pinned bundle. A repeated report of the same run is refused once one is
-- kept. Only adds.

CREATE TABLE quality.job_reports (
    tenant_id uuid DEFAULT public.current_tenant() NOT NULL,
    run text NOT NULL,
    state text NOT NULL CHECK (state IN ('done', 'failed')),
    error_class text,
    report jsonb NOT NULL,
    -- The machine client that reported.
    reported_by text NOT NULL,
    received_at timestamp with time zone DEFAULT now() NOT NULL,
    PRIMARY KEY (tenant_id, run)
);

ALTER TABLE quality.job_reports ENABLE ROW LEVEL SECURITY;
ALTER TABLE quality.job_reports FORCE ROW LEVEL SECURITY;
CREATE POLICY isolation ON quality.job_reports
    USING (tenant_id = public.current_tenant())
    WITH CHECK (tenant_id = public.current_tenant());
