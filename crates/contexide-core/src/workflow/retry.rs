//! Retry policies and quota-related types for workflow executor.
//!
//! Control-plane only: no DB/transport bindings here.

use std::time::Duration;

/// Describes when a task is allowed to retry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryPolicyKind {
    /// Do not perform any retries.
    Never,
    /// Retry up to N times with a fixed delay.
    Fixed { max_attempts: u32, delay: Duration },
    /// Retry up to N times with exponential backoff.
    ///
    /// delay = base_delay * 2^attempt_no, clamped by max_delay.
    Exponential {
        max_attempts: u32,
        base_delay: Duration,
        max_delay: Duration,
    },
    /// Retry forever with exponential backoff, obeying quotas.
    InfiniteExponential {
        base_delay: Duration,
        max_delay: Duration,
    },
}

/// Runtime context for making a retry decision.
#[derive(Debug, Clone)]
pub struct RetryContext {
    /// 1-based attempt number for this TaskRun (1 = first attempt).
    pub attempt_no: u32,
    /// Total number of attempts recorded so far for the Task.
    pub total_attempts: u32,
    /// Whether the last failure is considered transient (retryable).
    pub transient_error: bool,
}

/// Result of applying a retry policy for a specific attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryDecision {
    /// Do not retry, mark the task as permanently failed.
    Fail,
    /// Schedule a retry after the given delay.
    RetryAfter(Duration),
}

impl RetryPolicyKind {
    /// Compute a retry decision for the given context.
    pub fn decide(&self, ctx: &RetryContext) -> RetryDecision {
        if !ctx.transient_error {
            return RetryDecision::Fail;
        }

        match *self {
            RetryPolicyKind::Never => RetryDecision::Fail,
            RetryPolicyKind::Fixed {
                max_attempts,
                delay,
            } => {
                if ctx.attempt_no >= max_attempts {
                    RetryDecision::Fail
                } else {
                    RetryDecision::RetryAfter(delay)
                }
            }
            RetryPolicyKind::Exponential {
                max_attempts,
                base_delay,
                max_delay,
            } => {
                if ctx.attempt_no >= max_attempts {
                    RetryDecision::Fail
                } else {
                    let pow = ctx.attempt_no.saturating_sub(1).min(16);
                    let mut delay = base_delay.saturating_mul(2_u32.saturating_pow(pow));
                    if delay > max_delay {
                        delay = max_delay;
                    }
                    RetryDecision::RetryAfter(delay)
                }
            }
            RetryPolicyKind::InfiniteExponential {
                base_delay,
                max_delay,
            } => {
                let pow = ctx.attempt_no.saturating_sub(1).min(16);
                let mut delay = base_delay.saturating_mul(2_u32.saturating_pow(pow));
                if delay > max_delay {
                    delay = max_delay;
                }
                RetryDecision::RetryAfter(delay)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn never_policy_fails() {
        let policy = RetryPolicyKind::Never;
        let ctx = RetryContext {
            attempt_no: 1,
            total_attempts: 1,
            transient_error: true,
        };
        assert_eq!(policy.decide(&ctx), RetryDecision::Fail);
    }

    #[test]
    fn fixed_respects_max_attempts() {
        let policy = RetryPolicyKind::Fixed {
            max_attempts: 3,
            delay: Duration::from_secs(1),
        };
        let ctx = RetryContext {
            attempt_no: 2,
            total_attempts: 2,
            transient_error: true,
        };
        assert!(matches!(policy.decide(&ctx), RetryDecision::RetryAfter(_)));

        let ctx_last = RetryContext {
            attempt_no: 3,
            total_attempts: 3,
            transient_error: true,
        };
        assert_eq!(policy.decide(&ctx_last), RetryDecision::Fail);
    }

    #[test]
    fn exponential_caps_at_max_delay() {
        let policy = RetryPolicyKind::Exponential {
            max_attempts: 10,
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(1),
        };
        let ctx = RetryContext {
            attempt_no: 6,
            total_attempts: 6,
            transient_error: true,
        };
        if let RetryDecision::RetryAfter(d) = policy.decide(&ctx) {
            assert!(d <= Duration::from_secs(1));
        } else {
            panic!("expected retry");
        }
    }

    #[test]
    fn permanent_error_skips_retry() {
        let policy = RetryPolicyKind::InfiniteExponential {
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(10),
        };
        let ctx = RetryContext {
            attempt_no: 5,
            total_attempts: 5,
            transient_error: false,
        };
        assert_eq!(policy.decide(&ctx), RetryDecision::Fail);
    }
}
