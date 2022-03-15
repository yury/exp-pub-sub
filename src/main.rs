use std::sync::Arc;
use std::time::Duration;

use cloud_pubsub::{error, Topic};
use cloud_pubsub::{Client, EncodedMessage};
use serde_derive::Deserialize;
use tokio::{signal, task, time};
use tracing::{debug, error, info};
use worker::MachineStatsPacket;

use crate::worker::{spawn_worker, TopicMessage};

pub mod worker;

#[derive(Deserialize)]
struct Config {
    topic: String,
    subscription: String,
    google_application_credentials: String,
}

fn schedule_usage_metering(topic: Topic) {
    let dur = Duration::from_secs(2);
    let mut interval = time::interval(dur);
    task::spawn(async move {
        let mut x = 0;
        loop {
            interval.tick().await;
            x += 1;
            let p = MachineStatsPacket {
                id: x,
                secs: dur.as_secs() as _,
            };
            let m = EncodedMessage::new(&p, None);
            match topic.publish_message(m).await {
                Ok(response) => {
                    info!("{:?}", response);
                }
                Err(err) => error!("{}", err),
            }
        }
    });
}

#[tokio::main]
async fn main() -> Result<(), error::Error> {
    // install global collector configured based on RUST_LOG env var.
    tracing_subscriber::fmt::init();

    let parsed_env = envy::from_env::<Config>();
    if let Err(e) = parsed_env {
        error!("ENV is not valid: {}", e);
        std::process::exit(1);
    }
    let config = parsed_env.unwrap();

    let pubsub = match Client::new(config.google_application_credentials).await {
        Err(e) => panic!("Failed to initialize pubsub: {}", e),
        Ok(mut client) => {
            if let Err(e) = client.refresh_token().await {
                panic!("Failed to get token: {}", e);
            } else {
                info!("Got fresh token");
            }
            Arc::new(client)
        }
    };

    pubsub.spawn_token_renew(Duration::from_secs(15 * 60));

    let topic = pubsub.topic(config.topic);

    schedule_usage_metering(topic);

    let ctx = Arc::new(10);

    let worker_task = spawn_worker(
        ctx,
        pubsub.clone(),
        config.subscription,
        |cx, msg: MachineStatsPacket| async move {
            println!("{:?}: {:?}", cx, msg);
            if msg.id % 5 == 0 {
                let tm = TopicMessage::Meter(MachineStatsPacket { id: 1, secs: 0 });
                worker::Next::publish(tm)
            } else {
                worker::Next::ack()
            }
        },
    );

    signal::ctrl_c().await?;
    debug!("Cleaning up");
    pubsub.stop();
    debug!("Waiting for current Pull to finish....");
    worker_task.await.unwrap();
    Ok(())
}
