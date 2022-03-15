use std::sync::Arc;
use std::time::Duration;

use cloud_pubsub::{error, Topic};
use cloud_pubsub::{Client, EncodedMessage, FromPubSubMessage, Subscription};
use serde_derive::{Deserialize, Serialize};
use worker::Worker;
use tokio::{signal, task, time};
use tracing::{debug, error, info};
use tower::{self, Service, ServiceExt};

use crate::worker::spawn_worker;

pub mod worker;

async fn handler(ctx: Arc<u32>, message: String) -> Result<(), ()> {
    println!("{:?}:{:?}", ctx, message);
    Ok(())   
}

pub async fn foo() {
    // let worker = Worker::new(1, |a: u32, m: String| async move {
    //     println!("{:?}{:?}:{}", a, m, "Nice");
    //     Ok::<(), ()>(())
    // });

    let worker = Worker::new(Arc::new(1), handler);

    let f = worker.oneshot("nic".to_string()).await.expect("nice");
}

#[derive(Deserialize)]
struct Config {
    topic: String,
    subscription: String,
    google_application_credentials: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct MachineStatsPacket {
    id: u64,
    secs: u32,
}

impl FromPubSubMessage for MachineStatsPacket {
    fn from(message: EncodedMessage) -> Result<Self, error::Error> {
        match message.decode() {
            Ok(bytes) => {
                serde_json::from_slice::<MachineStatsPacket>(&bytes).map_err(error::Error::from)
            }
            Err(e) => Err(error::Error::from(e)),
        }
    }
}

fn schedule_pubsub_pull(subscription: Arc<Subscription>) {
    task::spawn(async move {
        while subscription.client().is_running() {
            match subscription.get_messages::<MachineStatsPacket>().await {
                Ok(messages) => {
                    for (result, ack_id) in messages {
                        match result {
                            Ok(message) => {
                                info!("recieved {:?}", message);
                                let subscription = Arc::clone(&subscription);
                                task::spawn(async move {
                                    subscription.acknowledge_messages(vec![ack_id]).await;
                                });
                            }
                            Err(e) => error!("Failed converting to UpdatePacket: {}", e),
                        }
                    }
                }
                Err(e) => error!("Failed to pull PubSub messages: {}", e),
            }
        }
        debug!("No longer pulling");
    });
}

fn schedule_usage_metering(topic: Topic) {
    let dur = Duration::from_secs(40);
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

    let subscription = pubsub.subscribe(config.subscription);

    debug!("Subscribed to topic with: {}", subscription.name);
    let sub = Arc::new(subscription);
    
    let ctx = Arc::new(10);

    let worker_task = spawn_worker(ctx, sub.clone(), |cx, m: MachineStatsPacket| async move {
        println!("{:?}: {:?}", cx, m);
        Ok(())
    });


    signal::ctrl_c().await?;
    debug!("Cleaning up");
    pubsub.stop();
    debug!("Waiting for current Pull to finish....");
    worker_task.await.unwrap();
    Ok(())
}
