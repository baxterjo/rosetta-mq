use std::path::PathBuf;

use anyhow::Context;
use clap::Parser;
use rosetta_mq::client::Client;
use rosetta_mq::config::Config;
use rosetta_mq::decoder::builtin;
use rosetta_mq::decoder::DecoderRegistryBuilder;
use rosetta_mq::pipeline;
use rosetta_mq::topic::TopicFilter;

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

    let mut builder = DecoderRegistryBuilder::new();
    for mapping in &config.topics {
        let decoder = builtin::by_name(&mapping.decoder).with_context(|| {
            format!(
                "unknown decoder {:?} for topic_filter {:?}",
                mapping.decoder, mapping.topic_filter
            )
        })?;
        let filter = TopicFilter::parse(&mapping.topic_filter)
            .with_context(|| format!("invalid topic_filter {:?}", mapping.topic_filter))?;
        builder
            .register(filter, decoder)
            .with_context(|| format!("registering topic_filter {:?}", mapping.topic_filter))?;
    }
    let registry = builder.build();

    let conn = Client::connect(&config.broker);
    Client::subscribe_all(
        &conn.client,
        config.topics.iter().map(|t| t.topic_filter.as_str()),
    )
    .await
    .context("subscribing to configured topics")?;

    tokio::select! {
        _ = pipeline::run(conn.incoming, conn.client.clone(), registry) => {}
        res = conn.driver => {
            tracing::error!("MQTT client driver returned early: {:?}", res)
        }
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("shutting down");
        }
    }

    Ok(())
}

fn init_tracing(log_level: &str) {
    use tracing_subscriber::EnvFilter;

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_new(log_level).unwrap_or_else(|_| EnvFilter::new("info")))
        .init();
}
