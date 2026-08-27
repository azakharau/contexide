use contexide_storage_pg::workflows::{Task, TaskRun};
use contexide_workflow_core::retry::{RetryContext, RetryDecision, RetryPolicyKind};

/// Domain-level defaults for retry policy per task kind / domain.
pub struct DomainRetryDefaults {
    pub policy: RetryPolicyKind,
    pub max_attempts: u32,
}

fn parse_policy(task: &Task, defaults: &DomainRetryDefaults) -> RetryPolicyKind {
    match task.retry_policy.as_str() {
        "never" => RetryPolicyKind::Never,
        "fixed" => {
            let delay_ms = task
                .retry_params
                .get("delay_ms")
                .and_then(|v| v.as_u64())
                .unwrap_or(30_000);
            RetryPolicyKind::Fixed {
                max_attempts: task
                    .max_attempts
                    .map(|m| m as u32)
                    .unwrap_or(defaults.max_attempts),
                delay: std::time::Duration::from_millis(delay_ms),
            }
        }
        "exponential" => {
            let base = task
                .retry_params
                .get("base_delay_ms")
                .and_then(|v| v.as_u64())
                .unwrap_or(1_000);
            let max = task
                .retry_params
                .get("max_delay_ms")
                .and_then(|v| v.as_u64())
                .unwrap_or(600_000);
            RetryPolicyKind::Exponential {
                max_attempts: task
                    .max_attempts
                    .map(|m| m as u32)
                    .unwrap_or(defaults.max_attempts),
                base_delay: std::time::Duration::from_millis(base),
                max_delay: std::time::Duration::from_millis(max),
            }
        }
        "infinite_exponential" => {
            let base = task
                .retry_params
                .get("base_delay_ms")
                .and_then(|v| v.as_u64())
                .unwrap_or(1_000);
            let max = task
                .retry_params
                .get("max_delay_ms")
                .and_then(|v| v.as_u64())
                .unwrap_or(600_000);
            RetryPolicyKind::InfiniteExponential {
                base_delay: std::time::Duration::from_millis(base),
                max_delay: std::time::Duration::from_millis(max),
            }
        }
        _ => defaults.policy,
    }
}

/// Resolve the effective retry policy for a task.
pub fn resolve_policy(task: &Task, defaults: &DomainRetryDefaults) -> RetryPolicyKind {
    parse_policy(task, defaults)
}

/// Decide whether to schedule another run for a failed task.
pub fn decide_retry_for_task(
    task: &Task,
    last_run: &TaskRun,
    domain_defaults: &DomainRetryDefaults,
    total_attempts_for_task: u32,
) -> RetryDecision {
    let policy = resolve_policy(task, domain_defaults);
    let transient = last_run.transient_error.unwrap_or(false);

    let ctx = RetryContext {
        attempt_no: last_run.attempt_no as u32,
        total_attempts: total_attempts_for_task,
        transient_error: transient,
    };

    policy.decide(&ctx)
}
