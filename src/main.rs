use std::path::{Path, PathBuf};

use anyhow::Context;
use clap::Parser;
use rosetta_mq::client::Client;
use rosetta_mq::config::Config;
use rosetta_mq::decoder::DecoderRegistryBuilder;
use rosetta_mq::pipeline;
use rosetta_mq::topic::TopicFilter;
use tokio_util::sync::CancellationToken;

#[derive(Parser)]
#[command(name = "rosetta-mq")]
struct Cli {
    /// Path to the TOML config file.
    #[arg(short, long, default_value = "rosetta-mq.toml")]
    config: PathBuf,

    /// Log level passed to the tracing env-filter (e.g. "info", "debug", "rosetta_mq=debug").
    #[arg(long, default_value = "info")]
    log_level: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    init_tracing(&cli.log_level);

    let config = Config::load(&cli.config)
        .with_context(|| format!("loading config from {}", cli.config.display()))?;

    let base_dir = cli
        .config
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf();

    let mut builder = DecoderRegistryBuilder::new();
    for mapping in &config.topics {
        let decoder = mapping.decoder.build(&base_dir).with_context(|| {
            format!(
                "building decoder for topic_filter {:?}",
                mapping.topic_filter
            )
        })?;
        let filter = TopicFilter::parse(&mapping.topic_filter)
            .with_context(|| format!("invalid topic_filter {:?}", mapping.topic_filter))?;
        builder
            .register(filter, decoder)
            .with_context(|| format!("registering topic_filter {:?}", mapping.topic_filter))?;
    }
    let registry = builder.build();

    let auth = match &config.connection.auth {
        Some(auth_cfg) => auth_cfg.build(&base_dir).context("resolving broker auth")?,
        None => rosetta_mq::auth::ResolvedAuth::None,
    };

    let conn = Client::connect(&config.connection, &auth).context("connecting to broker")?;
    Client::subscribe_all(
        &conn.client,
        config.topics.iter().map(|t| t.topic_filter.as_str()),
    )
    .await
    .context("subscribing to configured topics")?;

    let shutdown = CancellationToken::new();
    let pipeline_handle = tokio::spawn(pipeline::run(
        conn.incoming,
        conn.client.clone(),
        registry,
        config.pipeline.max_concurrent_decodes,
        shutdown.clone(),
    ));

    tokio::select! {
        res = conn.driver => {
            tracing::error!("MQTT client driver returned early: {:?}", res);
        }
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("shutting down");
        }
    }

    // Signals `pipeline::run` to stop taking new messages and drain in-flight ones (bounded by
    // its own internal timeout) rather than aborting them mid-flight by dropping its task.
    shutdown.cancel();
    if let Err(err) = pipeline_handle.await {
        tracing::error!(error = %err, "pipeline task panicked");
    }

    Ok(())
}

fn init_tracing(log_level: &str) {
    use tracing_subscriber::EnvFilter;

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_new(log_level).unwrap_or_else(|_| EnvFilter::new("info")))
        .init();
}
