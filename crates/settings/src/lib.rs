use std::{env, fmt::Debug};

use anyhow::bail;
use communication_utils::publisher::CommonPublisher;
use config::{Config, Environment, File};
use serde::Deserialize;

pub mod apps;

pub fn load_settings<'de, T: Deserialize<'de> + Debug>() -> anyhow::Result<T> {
    let env = env::var("ENVIRONMENT").unwrap_or_else(|_| "development".to_string());
    let exe = if let Some(exe) =
        env::current_exe().map(|f| f.file_name().map(|s| s.to_string_lossy().to_string()))?
    {
        exe
    } else {
        bail!("Missing executable file name")
    };
    let mut s = Config::new();

    s.merge(File::with_name("/etc/cdl/default.toml").required(false))?;
    s.merge(File::with_name(&format!("/etc/cdl/{}.toml", exe)).required(false))?;

    s.merge(File::with_name(&format!("/etc/cdl/{}/default.toml", env)).required(false))?;
    s.merge(File::with_name(&format!("/etc/cdl/{}/{}.toml", env, exe)).required(false))?;

    if let Some(home) = dirs::home_dir() {
        s.merge(
            File::with_name(&format!("{}/.cdl/default.toml", home.to_string_lossy(),))
                .required(false),
        )?;
        s.merge(
            File::with_name(&format!("{}/.cdl/{}.toml", home.to_string_lossy(), env,))
                .required(false),
        )?;
        s.merge(
            File::with_name(&format!(
                "{}/.cdl/{}/default.toml",
                home.to_string_lossy(),
                env
            ))
            .required(false),
        )?;
        s.merge(
            File::with_name(&format!(
                "{}/.cdl/{}/{}.toml",
                home.to_string_lossy(),
                env,
                exe
            ))
            .required(false),
        )?;
    }

    s.merge(File::with_name(".cdl/default.toml").required(false))?;
    s.merge(File::with_name(&format!(".cdl/{}.toml", exe)).required(false))?;
    s.merge(File::with_name(&format!(".cdl/{}/default.toml", env)).required(false))?;
    s.merge(File::with_name(&format!(".cdl/{}/{}.toml", env, exe)).required(false))?;

    if let Ok(custom_dir) = env::var("CDL_CONFIG") {
        s.merge(File::with_name(&format!("{}/default.toml", custom_dir)).required(false))?;
        s.merge(File::with_name(&format!("{}/{}.toml", custom_dir, exe)).required(false))?;
        s.merge(File::with_name(&format!("{}/{}/default.toml", custom_dir, env)).required(false))?;
        s.merge(File::with_name(&format!("{}/{}/{}.toml", custom_dir, env, exe)).required(false))?;
    }

    s.merge(Environment::with_prefix(&exe.replace("-", "_")).separator("__"))?;

    let settings = s.try_into()?;

    Ok(settings)
}

pub async fn publisher<'a>(
    kafka: Option<&'a str>,
    amqp: Option<&'a str>,
    grpc: Option<()>,
) -> anyhow::Result<CommonPublisher> {
    Ok(match (kafka, amqp, grpc) {
        (Some(brokers), _, _) => CommonPublisher::new_kafka(brokers).await?,
        (_, Some(exchange), _) => CommonPublisher::new_amqp(exchange).await?,
        (_, _, Some(_)) => CommonPublisher::new_grpc().await?,
        _ => anyhow::bail!("Unsupported publisher specification"),
    })
}
