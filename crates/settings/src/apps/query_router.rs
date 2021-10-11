use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::apps::{LogSettings, MonitoringSettings, RepositoryStaticRouting};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct QueryRouterSettings {
    pub cache_capacity: usize,
    pub input_port: u16,

    pub services: QueryRouterServicesSettings,

    pub monitoring: MonitoringSettings,

    #[serde(default)]
    pub log: LogSettings,

    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub repositories: HashMap<String, RepositoryStaticRouting>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct QueryRouterServicesSettings {
    pub schema_registry_url: String,
}
