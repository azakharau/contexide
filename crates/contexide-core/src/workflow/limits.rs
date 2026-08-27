//! Quota and concurrency limit types for workflow.
//!
//! Pure control-plane types; mapping to config/DB happens elsewhere.

/// Per-tenant concurrency limits.
#[derive(Debug, Clone)]
pub struct TenantLimits {
    pub max_running_dag_runs: u32,
    pub max_running_tasks: u32,
}

/// Per-domain concurrency limits (cross-tenant).
#[derive(Debug, Clone)]
pub struct DomainLimits {
    pub max_running_tasks_global: u32,
    pub max_running_tasks_per_tenant: u32,
}

/// Global workflow limits.
#[derive(Debug, Clone)]
pub struct GlobalLimits {
    pub max_running_tasks_total: u32,
}

/// Aggregated limits view used by the executor.
#[derive(Debug, Clone)]
pub struct LimitsView {
    pub global: GlobalLimits,
    pub tenant: TenantLimits,
    pub domain: DomainLimits,
}

/// Information about current usage to compare with limits.
#[derive(Debug, Clone)]
pub struct UsageSnapshot {
    pub running_tasks_total: u32,
    pub tenant_running_dag_runs: u32,
    pub tenant_running_tasks: u32,
    pub tenant_domain_running_tasks: u32,
    pub domain_running_tasks_global: u32,
}

/// Result of evaluating whether we can start another task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionDecision {
    Allow,
    Throttle,
}

impl LimitsView {
    pub fn decide_admission(&self, usage: &UsageSnapshot) -> AdmissionDecision {
        use AdmissionDecision::*;

        if usage.running_tasks_total >= self.global.max_running_tasks_total {
            return Throttle;
        }
        if usage.tenant_running_dag_runs >= self.tenant.max_running_dag_runs {
            return Throttle;
        }
        if usage.tenant_running_tasks >= self.tenant.max_running_tasks {
            return Throttle;
        }
        if usage.tenant_domain_running_tasks >= self.domain.max_running_tasks_per_tenant {
            return Throttle;
        }
        if usage.domain_running_tasks_global >= self.domain.max_running_tasks_global {
            return Throttle;
        }
        Allow
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits() -> LimitsView {
        LimitsView {
            global: GlobalLimits {
                max_running_tasks_total: 10,
            },
            tenant: TenantLimits {
                max_running_dag_runs: 3,
                max_running_tasks: 5,
            },
            domain: DomainLimits {
                max_running_tasks_global: 6,
                max_running_tasks_per_tenant: 4,
            },
        }
    }

    #[test]
    fn allows_when_below_limits() {
        let l = limits();
        let usage = UsageSnapshot {
            running_tasks_total: 5,
            tenant_running_dag_runs: 1,
            tenant_running_tasks: 2,
            tenant_domain_running_tasks: 2,
            domain_running_tasks_global: 3,
        };
        assert_eq!(l.decide_admission(&usage), AdmissionDecision::Allow);
    }

    #[test]
    fn throttles_on_global_cap() {
        let l = limits();
        let usage = UsageSnapshot {
            running_tasks_total: 10,
            tenant_running_dag_runs: 0,
            tenant_running_tasks: 0,
            tenant_domain_running_tasks: 0,
            domain_running_tasks_global: 0,
        };
        assert_eq!(l.decide_admission(&usage), AdmissionDecision::Throttle);
    }

    #[test]
    fn throttles_on_domain_per_tenant() {
        let l = limits();
        let usage = UsageSnapshot {
            running_tasks_total: 2,
            tenant_running_dag_runs: 1,
            tenant_running_tasks: 3,
            tenant_domain_running_tasks: 4,
            domain_running_tasks_global: 5,
        };
        assert_eq!(l.decide_admission(&usage), AdmissionDecision::Throttle);
    }
}
