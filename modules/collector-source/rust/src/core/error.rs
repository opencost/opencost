#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[allow(unused)]
    #[error("Generic {0}")]
    Generic(String),

    #[error("Parse Error: {0}")]
    Parse(String),

    #[error(transparent)]
    IO(#[from] std::io::Error),

    #[error(transparent)]
    Http(#[from] reqwest::Error),
}
