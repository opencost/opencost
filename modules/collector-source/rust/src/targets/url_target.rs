use async_trait::async_trait;
use reqwest::header::AUTHORIZATION;
use reqwest::{
    Client,
    IntoUrl,
    Url,
};

use crate::core::Result;
use crate::targets::ScrapeTarget;

#[derive(Debug)]
pub struct TlsOptions {
    pub insecure_skip_verify: bool,
    // ... cover allowed reqwest options
}

#[derive(Debug)]
pub struct HttpOptions {
    pub auth_token: Option<String>,
    pub tls: Option<TlsOptions>,
}

/// This target allows for a file path to be used to load and scrape a target.
#[derive(Debug)]
pub struct UrlScrapeTarget {
    client: Client,
    token: Option<String>,
    url: Url,
}

impl UrlScrapeTarget {
    pub fn new<U: IntoUrl>(url: U, options: Option<HttpOptions>) -> Result<Self> {
        let url = url.into_url()?;

        let mut client = reqwest::Client::builder();
        let mut token = None;

        if let Some(http_opts) = options {
            token = http_opts.auth_token;

            if let Some(tls) = http_opts.tls {
                if tls.insecure_skip_verify {
                    client = client.danger_accept_invalid_certs(true)
                }
            }
        }

        let client = client.build()?;

        Ok(Self { client, token, url })
    }
}

#[async_trait]
impl ScrapeTarget for UrlScrapeTarget {
    async fn refresh(&self) -> Result<String> {
        let mut request = self.client.get(self.url.clone());

        if let Some(token) = &self.token {
            request = request.header(AUTHORIZATION, format!("Bearer {}", &token));
        }

        Ok(request.send().await?.text().await?)
    }
}
