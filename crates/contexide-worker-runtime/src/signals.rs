use tokio::signal;

/// Wait for SIGINT or SIGTERM once.
///
/// Used by worker runner to initiate graceful shutdown.
pub async fn wait_for_shutdown_signal() {
    // Test hook: if a mock receiver is installed, use it.
    #[cfg(test)]
    if let Some(rx) = MOCK_SHUTDOWN_RX.get() {
        let mut guard = rx.lock().await;
        let _ = guard.recv().await;
        return;
    }

    #[cfg(unix)]
    {
        let mut term = signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler");
        tokio::select! {
            _ = signal::ctrl_c() => {},
            _ = term.recv() => {},
        }
    }

    #[cfg(not(unix))]
    {
        let _ = signal::ctrl_c().await;
    }
}

#[cfg(test)]
use once_cell::sync::OnceCell;
#[cfg(test)]
use tokio::sync::{Mutex, mpsc};

#[cfg(test)]
static MOCK_SHUTDOWN_RX: OnceCell<Mutex<mpsc::Receiver<()>>> = OnceCell::new();

/// Test helper: install a custom shutdown receiver to unblock `wait_for_shutdown_signal`.
#[cfg(test)]
pub fn install_mock_shutdown(rx: mpsc::Receiver<()>) {
    let _ = MOCK_SHUTDOWN_RX.set(Mutex::new(rx));
}
