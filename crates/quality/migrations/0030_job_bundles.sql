-- Each run's signed statement bundle (decision 0090, point 2; ruled by the
-- user 2026-11-25): compiled and signed when a job runner first claims the
-- run, then pinned to it, so a retry of the run gets the same bundle. The
-- row holds the signed bundle as given, its digest and what it was asked
-- for; nothing is ever updated. Only adds.

CREATE TABLE quality.job_bundles (
    tenant_id uuid DEFAULT public.current_tenant() NOT NULL,
    -- The operator's run, which the bundle names as its `job_id`.
    run text NOT NULL,
    kind text NOT NULL,
    dataset text,
    cut date,
    digest text NOT NULL,
    bundle text NOT NULL,
    -- The machine client that asked first.
    asked_by text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    PRIMARY KEY (tenant_id, run)
);

ALTER TABLE quality.job_bundles ENABLE ROW LEVEL SECURITY;
ALTER TABLE quality.job_bundles FORCE ROW LEVEL SECURITY;
CREATE POLICY isolation ON quality.job_bundles
    USING (tenant_id = public.current_tenant())
    WITH CHECK (tenant_id = public.current_tenant());
