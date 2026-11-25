-- Gold's run order (decision 0090, point 4): each compile of an
-- evaluation, a retract or an SLA job takes the next value as its run's
-- `run_seq`, so a later compile of the same cut always wins the flip.
-- Only adds.
CREATE SEQUENCE IF NOT EXISTS quality.gold_run_seq AS bigint START WITH 1;
