ALTER TABLE dag_runs
    ADD COLUMN IF NOT EXISTS execution_policy jsonb,
    ADD COLUMN IF NOT EXISTS execution_policy_version smallint NOT NULL DEFAULT 1;

ALTER TABLE tasks
    ADD COLUMN IF NOT EXISTS execution_policy_override jsonb;
