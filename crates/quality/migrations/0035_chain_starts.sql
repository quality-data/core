-- The chains the core started in the orchestrator for the jobs a person
-- asks for (decision 0090, ruling 92(a)): one per job, with the chain the
-- orchestrator answered, so a job is handed over once. Only adds.

CREATE TABLE quality.chain_starts (
    tenant_id uuid DEFAULT public.current_tenant() NOT NULL,
    job uuid NOT NULL,
    chain_id text NOT NULL,
    started_at timestamp with time zone DEFAULT now() NOT NULL,
    PRIMARY KEY (tenant_id, job)
);

ALTER TABLE quality.chain_starts ENABLE ROW LEVEL SECURITY;
ALTER TABLE quality.chain_starts FORCE ROW LEVEL SECURITY;
CREATE POLICY isolation ON quality.chain_starts
    USING (tenant_id = public.current_tenant())
    WITH CHECK (tenant_id = public.current_tenant());
