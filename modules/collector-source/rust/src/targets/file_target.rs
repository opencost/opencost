use std::path::{
    Path,
    PathBuf,
};

use async_trait::async_trait;
use tokio::fs;

use crate::core::Result;
use crate::targets::ScrapeTarget;

/// This target allows for a file path to be used to load and scrape a target.
#[derive(Debug)]
pub struct FileScrapeTarget {
    path: PathBuf,
}

impl FileScrapeTarget {
    #[allow(unused)]
    pub fn new<P: AsRef<Path>>(p: P) -> Self {
        let path = p.as_ref().to_owned();

        Self { path }
    }
}

#[async_trait]
impl ScrapeTarget for FileScrapeTarget {
    async fn refresh(&self) -> Result<String> {
        Ok(fs::read_to_string(&self.path).await?)
    }
}
