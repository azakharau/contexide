ALTER TABLE tasks
    ADD COLUMN IF NOT EXISTS max_attempts integer,
    ADD COLUMN IF NOT EXISTS retry_policy text NOT NULL DEFAULT 'never',
    ADD COLUMN IF NOT EXISTS retry_params jsonb NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS priority smallint NOT NULL DEFAULT 0;

ALTER TABLE task_runs
    ADD COLUMN IF NOT EXISTS error_code text,
    ADD COLUMN IF NOT EXISTS error_message text,
    ADD COLUMN IF NOT EXISTS transient_error boolean;

CREATE TABLE IF NOT EXISTS workflow_tenant_limits (
    tenant_id uuid PRIMARY KEY,
    max_running_dag_runs integer NOT NULL DEFAULT 5,
    max_running_tasks integer NOT NULL DEFAULT 50,
    max_running_tasks_per_domain integer NOT NULL DEFAULT 20,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_workflow_tenant_limits_updated_at
    ON workflow_tenant_limits (updated_at);
