use std::{future::Future, marker::PhantomData, process::Output, sync::Arc};

use cloud_pubsub::{error, EncodedMessage, FromPubSubMessage};
use serde_derive::{Deserialize, Serialize};
use tokio::task::JoinHandle;
use tracing::error;

#[derive(Debug)]
pub enum Error {
    IO(std::io::Error),
    PubSub(cloud_pubsub::error::Error),
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::IO(e)
    }
}

impl From<cloud_pubsub::error::Error> for Error {
    fn from(e: cloud_pubsub::error::Error) -> Self {
        Error::PubSub(e)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MachineStatsPacket {
    pub id: u64,
    pub secs: u32,
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

#[derive(Serialize, Deserialize)]
#[serde(untagged)]
pub enum TopicMessage {
    Meter(MachineStatsPacket),
}

impl TopicMessage {
    fn topic(&self) -> String {
        "rust-test".to_string()
    }
}

pub enum Next {
    Ack,
    Publish(TopicMessage),
}

impl Next {
    pub fn ack() -> Result<Next, Error> {
        Ok(Self::Ack)
    }

    pub fn publish(to: TopicMessage) -> Result<Next, Error> {
        Ok(Self::Publish(to))
    }

    pub fn nack(error: Error) -> Result<Next, Error> {
        Err(error)
    }
}

pub async fn publish_message(
    pubsub: Arc<cloud_pubsub::Client>,
    msg: TopicMessage,
) -> Result<(), Error> {
    let topic = pubsub.topic(msg.topic());
    let m = EncodedMessage::new(&msg, None);
    _ = topic.publish_message(m).await?;
    Ok(())
}

pub fn spawn<C, W, M, F>(    
    ps: Arc<cloud_pubsub::Client>,
    sub_name: String,
    ctx: C,
    worker_fn: W,
) -> JoinHandle<()>
where
    C: Clone + Send + Sync + 'static,
    M: FromPubSubMessage + Send,
    W: Fn(C, M) -> F + Sync + Send + 'static,
    F: Future<Output = Result<Next, Error>> + Send,
{
    tokio::spawn(async move {
        let subscription = ps.subscribe(sub_name);
        while ps.is_running() {
            let f = subscription.get_messages::<M>().await.unwrap();
            for (result, ack_id) in f {
                let mut res = match result {
                    Ok(message) => worker_fn(ctx.clone(), message).await,
                    Err(e) => {
                        error!("Failed converting to {}: {}", std::any::type_name::<M>(), e);
                        Next::ack()
                    }
                };

                if let Ok(Next::Publish(msg)) = res {
                    res = match publish_message(ps.clone(), msg).await {
                        Ok(_) => Next::ack(),
                        Err(e) => Err(e),
                    }
                }

                match res {
                    Ok(Next::Ack) => subscription.acknowledge_messages(vec![ack_id]).await,
                    Ok(Next::Publish(_)) => panic!("We should handle it before"),
                    Err(e) => {
                        error!("Worker failed: {:?}", e);
                    }
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {

    #[tokio::test]
    async fn test_with_worker_fn() {}
}
