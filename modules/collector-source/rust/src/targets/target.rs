use async_trait::async_trait;

use crate::core::Result;

/// The `ScrapeTarget` trait represents an implementation which can retrieve raw
/// string metrics.
#[async_trait]
pub trait ScrapeTarget: Send + Sync {
    /// Refreshes the raw metrics at the current time.
    async fn refresh(&self) -> Result<String>;
}
