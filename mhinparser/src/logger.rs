// logger.rs
use log::{Log, Metadata, Record};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, OnceLock};

/// Global state to track if logs have been emitted since last stats display
static HAS_LOGS_SINCE_MARK: AtomicBool = AtomicBool::new(false);

/// Static env_logger instance
static ENV_LOGGER: OnceLock<Mutex<env_logger::Logger>> = OnceLock::new();

/// Custom logger that wraps env_logger and tracks log emissions
struct SmartLogger;

impl Log for SmartLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        // Get the env_logger instance and check if it's enabled
        if let Some(logger_mutex) = ENV_LOGGER.get() {
            if let Ok(logger) = logger_mutex.lock() {
                return logger.enabled(metadata);
            }
        }
        // Fallback to basic level check
        log::max_level() >= metadata.level()
    }

    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            // Mark that a log has been emitted
            HAS_LOGS_SINCE_MARK.store(true, Ordering::Relaxed);

            // Delegate to env_logger
            if let Some(logger_mutex) = ENV_LOGGER.get() {
                if let Ok(logger) = logger_mutex.lock() {
                    logger.log(record);
                }
            }
        }
    }

    fn flush(&self) {
        if let Some(logger_mutex) = ENV_LOGGER.get() {
            if let Ok(logger) = logger_mutex.lock() {
                logger.flush();
            }
        }
    }
}

/// Initialize the smart logger as the global logger
pub fn init() -> Result<(), Box<dyn std::error::Error>> {
    // Create and store the env_logger instance
    let env_logger = env_logger::Builder::from_default_env().build();
    let level_filter = env_logger.filter();

    ENV_LOGGER
        .set(Mutex::new(env_logger))
        .map_err(|_| "Logger already initialized")?;

    // Set our custom logger
    log::set_boxed_logger(Box::new(SmartLogger))?;

    // Apply the level filter that env_logger determined
    log::set_max_level(level_filter);

    Ok(())
}

/// Check if any logs have been emitted since the last mark
pub fn has_logs_since_last_mark() -> bool {
    HAS_LOGS_SINCE_MARK.load(Ordering::Relaxed)
}

/// Mark that stats have been displayed and reset the log tracking
pub fn mark_stats_displayed() {
    HAS_LOGS_SINCE_MARK.store(false, Ordering::Relaxed);
}
