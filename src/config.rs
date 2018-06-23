use configrs::{Config, ConfigError, File};

#[derive(Debug, Deserialize)]
pub struct Settings {
    pub interval: u64,
    pub topic: String,
    pub brokers: String,
}

// https://github.com/mehcode/config-rs/blob/master/examples/hierarchical-env/src/settings.rs#L39
impl Settings {
    pub fn from(path: String) -> Result<Self, ConfigError> {
        let mut s = Config::new();

        s.merge(File::with_name(&path).required(true))?;

        // You can deserialize (and thus freeze) the entire configuration as
        s.try_into()
    }
}
