use crate::core::Result;
use crate::metrics::{
    Metric,
    MetricLine,
    MetricParser,
};
use crate::targets::ScrapeTarget;
use crate::util::*;

/// A `Scraper` leverages a `ScrapeTarget` to load and parse raw metrics data
/// at the moment `scrape` is called.
pub struct Scraper {
    target: Box<dyn ScrapeTarget>,
}

impl Scraper {
    /// Creates a new `Scraper` instance which accepts a `ScrapeTarget`
    /// implementation leveraged to refresh the raw metrics data for
    /// parsing.
    pub fn new<T: ScrapeTarget + 'static>(target: T) -> Self {
        Self {
            target: Box::new(target),
        }
    }

    /// This function accepts a list of metrics to include while parsing, and
    /// returns any parsed `Metric` values whose name appears in the
    /// provided set.
    pub async fn scrape(&self, metrics: &FnvSet<String>) -> Result<Vec<Metric>> {
        let target = self.target.refresh().await?;

        let parser = MetricParser::new(metrics);
        let result = parser.parse(&target)?;

        Ok(result
            .iter()
            .filter_map(|ml| -> Option<Metric> {
                match ml {
                    MetricLine::Metric(m) => Some(m.clone()),
                    MetricLine::Comment(_) => None,
                    MetricLine::Ignored => None,
                }
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hashset;
    use crate::targets::FileScrapeTarget;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[tracing_test::traced_test]
    async fn test_scrape() -> anyhow::Result<()> {
        const TOTAL_METRICS: usize = 96usize;

        let target = FileScrapeTarget::new("resources/scrape.txt");
        let scraper = Scraper::new(target);

        let set = hashset!(
            String::from("container_cpu_usage_seconds_total"),
            String::from("container_cpu_system_seconds_total"),
            String::from("container_fs_io_current"),
        );

        let result = scraper.scrape(&set).await?;
        assert_eq!(result.len(), TOTAL_METRICS);

        for m in result {
            println!("[{}", m.name);
            for (k, v) in m.labels.iter() {
                println!("  - {}: {}", k, v);
            }

            println!("], value: {}, timestamp: {:?}\n", m.value, m.timestamp);
        }

        Ok(())
    }
}
