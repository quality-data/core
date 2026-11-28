-- Each fire of a schedule the orchestrator asked for (decision 0090,
-- ruling 92(a)): the chain's steps the core answered, kept so the same
-- fire gets the same answer. Only adds.

CREATE TABLE quality.schedule_fires (
    tenant_id uuid DEFAULT public.current_tenant() NOT NULL,
    schedule uuid NOT NULL,
    fire_at timestamp with time zone NOT NULL,
    steps jsonb NOT NULL,
    fired_at timestamp with time zone DEFAULT now() NOT NULL,
    PRIMARY KEY (tenant_id, schedule, fire_at)
);

ALTER TABLE quality.schedule_fires ENABLE ROW LEVEL SECURITY;
ALTER TABLE quality.schedule_fires FORCE ROW LEVEL SECURITY;
CREATE POLICY isolation ON quality.schedule_fires
    USING (tenant_id = public.current_tenant())
    WITH CHECK (tenant_id = public.current_tenant());
