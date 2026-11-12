use crate::models::DurationOption;
use std::env;

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub port: u16,
    pub database_path: String,
    pub durations: Vec<DurationOption>,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self::from_env()
    }
}

impl AppConfig {
    pub fn from_env() -> Self {
        let port = env::var("PORT")
            .ok()
            .and_then(|p| p.parse::<u16>().ok())
            .unwrap_or(8085);

        let raw_db = env::var("DATABASE_URL")
            .or_else(|_| env::var("SPRING_DATASOURCE_URL"))
            .unwrap_or_else(|_| "pollster.db".to_string());

        let database_path = raw_db
            .strip_prefix("jdbc:sqlite:")
            .or_else(|| raw_db.strip_prefix("sqlite://"))
            .or_else(|| raw_db.strip_prefix("sqlite:"))
            .unwrap_or(&raw_db)
            .to_string();

        let durations = vec![
            DurationOption {
                minutes: None,
                label: "Never expire".to_string(),
            },
            DurationOption {
                minutes: Some(15),
                label: "15 Minutes".to_string(),
            },
            DurationOption {
                minutes: Some(60),
                label: "1 Hour".to_string(),
            },
            DurationOption {
                minutes: Some(1440),
                label: "24 Hours".to_string(),
            },
            DurationOption {
                minutes: Some(10080),
                label: "7 Days".to_string(),
            },
        ];

        Self {
            port,
            database_path,
            durations,
        }
    }
}
