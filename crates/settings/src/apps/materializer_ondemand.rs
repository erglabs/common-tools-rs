use serde::{Deserialize, Serialize};

use crate::apps::{LogSettings, MonitoringSettings};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MaterializerOndemandSettings {
    pub input_port: u16,

    pub services: MaterializerOndemandServicesSettings,

    pub monitoring: MonitoringSettings,

    #[serde(default)]
    pub log: LogSettings,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MaterializerOndemandServicesSettings {
    pub object_builder_url: String,
}
