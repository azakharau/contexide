ALTER TABLE tasks
    DROP COLUMN IF EXISTS execution_policy_override;

ALTER TABLE dag_runs
    DROP COLUMN IF EXISTS execution_policy,
    DROP COLUMN IF EXISTS execution_policy_version;
