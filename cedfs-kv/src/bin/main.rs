use std::path::PathBuf;

use anyhow::Ok;
use clap::{ArgAction, Parser};
use tracing::level_filters::LevelFilter;
use tracing_subscriber::fmt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use cedfs_kv::KVServer;
//use cedfs_kv::client::KvCacheClient;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    path: String,
    #[arg(short, long, action = ArgAction::SetTrue, default_value = "false")]
    need_reset_storage: bool,
    #[arg(short, long, default_value = "WARN")]
    log: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let filter = match args.log.to_lowercase().as_str() {
        "trace" => LevelFilter::TRACE,
        "debug" => LevelFilter::DEBUG,
        "info" => LevelFilter::INFO,
        "warn" => LevelFilter::WARN,
        "error" => LevelFilter::ERROR,
        _ => LevelFilter::INFO,
    };

    tracing_subscriber::registry()
        .with(
            fmt::layer()
                // 默认会包含时间戳
                .with_level(true)
                .with_target(false)
        )
        .with(filter)
        .init();

    let config_path = args.path;
    let kvserver = KVServer::new(PathBuf::from(config_path)).await?;
    //let shared = kvserver.shared.clone();
    //let transfer_meta_port = shared.config.transfer_meta_port;
    // let client = KvCacheClient {
    //     shared: shared.clone(),
    // };

    // let (_serve_res, launch_res) = tokio::join!(
    //     kvserver.serve(),
    //     client.launch(),
    //     //transfer_server.run(),
    // );
    kvserver.serve().await;


    
    Ok(())
}