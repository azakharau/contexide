DROP TABLE IF EXISTS workflow_tenant_limits;

ALTER TABLE task_runs
    DROP COLUMN IF EXISTS transient_error,
    DROP COLUMN IF EXISTS error_message,
    DROP COLUMN IF EXISTS error_code;

ALTER TABLE tasks
    DROP COLUMN IF EXISTS priority,
    DROP COLUMN IF EXISTS retry_params,
    DROP COLUMN IF EXISTS retry_policy,
    DROP COLUMN IF EXISTS max_attempts;
