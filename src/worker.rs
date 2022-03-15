use std::{
    future::Future,
    sync::Arc,
    task::{Context, Poll},
};

use cloud_pubsub::{Subscription, FromPubSubMessage};
use tokio::task::JoinHandle;
use tower::{Service, ServiceExt};
use tracing::{error, instrument::WithSubscriber};

// Worker is Service with context. May be we can rename it ContextService
pub struct Worker<Ctx, T> {
    worker_fn: T,
    ctx: Arc<Ctx>,
}

impl<Ctx, F, T, Request, R, E> Service<Request> for Worker<Ctx, T>
where
    T: FnMut(Arc<Ctx>, Request) -> F,
    F: Future<Output = Result<R, E>>,
{
    type Response = R;
    type Error = E;
    type Future = F;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // assume we are always ready
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request) -> Self::Future {
        (self.worker_fn)(self.ctx.clone(), req)
    }
}

impl<Ctx, T> Worker<Ctx, T> {
    pub fn new(ctx: Arc<Ctx>, worker_fn: T) -> Self {
        Worker { ctx, worker_fn }
    }
}

pub struct SubWorker<C, T> {
    subscription: Arc<Subscription>,
    worker: Worker<C, T>,
}

pub fn spawn_sub_worker<C, T, Msg, F>(mut sb: SubWorker<C, T>) -> JoinHandle<()> 
where
    Msg: FromPubSubMessage + Send,
    C: Send + Sync + 'static,
    T: FnMut(Arc<C>, Msg) -> F + Send + 'static + Sync,
    F: Future<Output = Result<(), ()>> + Send,
{
        tokio::spawn( async move {
            while sb.subscription.client().is_running() {
                let f = sb.subscription.get_messages::<Msg>().await.unwrap();
                
                for (result, ack_id) in f {
                    match result {
                        Ok(message) => sb.worker.ready().await.unwrap().call(message).await.unwrap(),
                        Err(e) => error!("Failed converting to UpdatePacket: {}", e)
                    };
                }
            }
        })
}

pub fn spawn_worker<C, W, M, F>(ctx: C, s: Arc<Subscription>, worker_fn: W) -> JoinHandle<()>
where
    C: Clone + Send + Sync + 'static,
    M: FromPubSubMessage + Send,
    W: Fn(C, M) -> F + Sync + Send + 'static,
    F: Future<Output = Result<(), ()>> + Send
{
    tokio::spawn(async move {
        while s.client().is_running() {
            let f = s.get_messages::<M>().await.unwrap(); 
            for (result, ack_id) in f {
                match result {
                    Ok(message) => worker_fn(ctx.clone(), message).await.unwrap(),
                    Err(e) => error!("Failed converting to UpdatePacket: {}", e)
                };
                s.acknowledge_messages(vec![ack_id]).await;
            }
        }
    })
}


#[cfg(test)]
mod tests {
    use std::{sync::Arc, convert::Infallible};
    use tower::{ServiceExt};

    use super::Worker;


    #[tokio::test]
    async fn test_with_closure() {
        let context = Arc::new(10);
        let worker = Worker::new(context, |ctx: Arc<u32>, message: String| async move {
            let mut res = String::new();
            res.push_str(&message);
            res.push_str(&format!("{:?}!", ctx));
            Ok::<String, Infallible>(res) 
        });

        let r = worker.oneshot("closure".to_string()).await.unwrap();

        assert_eq!("closure10!", r)
    }

    async fn worker_fn(context: Arc<u32>, message: String) -> Result<String, Infallible> {
        let mut res = String::new();
        res.push_str(&message);
        res.push_str(&format!("{:?}!", context));
        Ok::<String, Infallible>(res) 
    }

    #[tokio::test]
    async fn test_with_worker_fn() {
        let context = Arc::new(10);
        let worker = Worker::new(context, worker_fn);

        let r = worker.oneshot("nice".to_string()).await.unwrap();

        assert_eq!("nice10!", r)
    }
}