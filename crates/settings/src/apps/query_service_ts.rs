use serde::{Deserialize, Serialize};
use url::Url;

use crate::apps::{LogSettings, MonitoringSettings};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct QueryServiceTsSettings {
    pub repository_kind: QueryServiceTsRepositoryKind,
    pub input_port: u16,

    pub druid: Option<QueryServiceTsDruidSettings>,
    pub victoria_metrics: Option<QueryServiceTsVictoriaMetricsSettings>,

    pub monitoring: MonitoringSettings,

    #[serde(default)]
    pub log: LogSettings,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryServiceTsRepositoryKind {
    VictoriaMetrics,
    Druid,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct QueryServiceTsDruidSettings {
    pub url: String,
    pub table_name: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct QueryServiceTsVictoriaMetricsSettings {
    pub url: Url,
}
