//! rivine Broker 入口
//!
//! 用 Rust 重写 Apache Kafka。
//!
//! 用法：
//! ```bash
//! rivine-broker                       # 使用默认配置启动
//! rivine-broker --config broker.toml  # 使用配置文件
//! RIVINE_BROKER_ID=1 rivine-broker    # 使用环境变量
//! ```

use anyhow::Result;
use rivine::Broker;
use rivine::BrokerConfig;
use std::path::Path;
use tracing_subscriber::EnvFilter;

/// 简易命令行参数解析
struct Args {
    config: Option<String>,
}

impl Args {
    fn parse() -> Self {
        let mut config = None;
        let mut args = std::env::args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--config" | "-c" => config = args.next(),
                "--help" | "-h" => {
                    print_help();
                    std::process::exit(0);
                }
                _ => {}
            }
        }
        Self { config }
    }
}

fn print_help() {
    println!(
        "rivine-broker — 用 Rust 重写 Apache Kafka\n\
         \n\
         用法:\n\
         \x20 rivine-broker [选项]\n\
         \n\
         选项:\n\
         \x20 --config <FILE>  指定 TOML 配置文件\n\
         \x20 -h, --help       显示帮助\n\
         \n\
         环境变量 (前缀 RIVINE_, 如 RIVINE_BROKER_ID=1):\n\
         \x20 RIVINE_HOST, RIVINE_PORT, RIVINE_BROKER_ID, RIVINE_LOG_DIRS, ...\n"
    );
}

#[tokio::main]
async fn main() -> Result<()> {
    // 初始化日志
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    let config_path = args.config.as_deref().map(Path::new);
    let config = BrokerConfig::load(config_path)?;

    tracing::info!(
        "rivine v{} 启动: broker_id={}, listen={}:{}, log_dirs={:?}",
        env!("CARGO_PKG_VERSION"),
        config.broker_id,
        config.host,
        config.port,
        config.log_dirs
    );

    let broker = Broker::new(config);
    broker.run().await?;
    Ok(())
}
