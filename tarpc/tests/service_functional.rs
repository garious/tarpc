use assert_matches::assert_matches;
use futures::{
    future::{ready, Ready},
    prelude::*,
};
use std::io;
use tarpc::{
    client::{self},
    context, serde_transport,
    server::{self, BaseChannel, Channel, Handler},
    transport::channel,
};
use tokio_serde::formats::Json;

#[tarpc_plugins::service]
trait Service {
    async fn add(x: i32, y: i32) -> i32;
    async fn hey(name: String) -> String;
}

#[derive(Clone)]
struct Server;

impl Service for Server {
    type AddFut = Ready<i32>;

    fn add(self, _: context::Context, x: i32, y: i32) -> Self::AddFut {
        ready(x + y)
    }

    type HeyFut = Ready<String>;

    fn hey(self, _: context::Context, name: String) -> Self::HeyFut {
        ready(format!("Hey, {}.", name))
    }
}

#[tokio::test(threaded_scheduler)]
async fn sequential() -> io::Result<()> {
    let _ = env_logger::try_init();

    let (tx, rx) = channel::unbounded();

    tokio::spawn(
        BaseChannel::new(server::Config::default(), rx)
            .respond_with(Server.serve())
            .execute(),
    );

    let mut client = ServiceClient::new(client::Config::default(), tx).spawn()?;

    assert_matches!(client.add(context::current(), 1, 2).await, Ok(3));
    assert_matches!(
        client.hey(context::current(), "Tim".into()).await,
        Ok(ref s) if s == "Hey, Tim.");

    Ok(())
}

#[cfg(feature = "serde1")]
#[tokio::test(threaded_scheduler)]
async fn serde() -> io::Result<()> {
    let _ = env_logger::try_init();

    let transport = serde_transport::tcp::listen("localhost:56789", Json::default).await?;
    let addr = transport.local_addr();
    tokio::spawn(
        tarpc::Server::default()
            .incoming(transport.take(1).filter_map(|r| async { r.ok() }))
            .respond_with(Server.serve()),
    );

    let transport = serde_transport::tcp::connect(addr, Json::default()).await?;
    let mut client = ServiceClient::new(client::Config::default(), transport).spawn()?;

    assert_matches!(client.add(context::current(), 1, 2).await, Ok(3));
    assert_matches!(
        client.hey(context::current(), "Tim".to_string()).await,
        Ok(ref s) if s == "Hey, Tim."
    );

    Ok(())
}

#[tokio::test(threaded_scheduler)]
async fn concurrent() -> io::Result<()> {
    let _ = env_logger::try_init();

    let (tx, rx) = channel::unbounded();
    tokio::spawn(
        tarpc::Server::default()
            .incoming(stream::once(ready(rx)))
            .respond_with(Server.serve()),
    );

    // These RPC calls are only concurrent in that the methods are not async and
    // send the message before returning a future. It then awaits each future
    // sequentially.

    let client = ServiceClient::new(client::Config::default(), tx).spawn()?;
    let mut c = client.clone();
    let req1 = c.add(context::current(), 1, 2);

    let mut c = client.clone();
    let req2 = c.add(context::current(), 3, 4);

    let mut c = client.clone();
    let req3 = c.hey(context::current(), "Tim".to_string());

    assert_matches!(req1.await, Ok(3));
    assert_matches!(req2.await, Ok(7));
    assert_matches!(req3.await, Ok(ref s) if s == "Hey, Tim.");

    Ok(())
}

#[tokio::test(threaded_scheduler)]
async fn concurrent_ideal() -> io::Result<()> {
    // Unlike the above, this test is written as if the client methods were async.
    // The runtime should poll the first future to send the first message. When that
    // future awaits a response, the runtime should poll the second future, which
    // should then send the second message and await its response. After both
    // futures complete, it should return the response.
    // Note: this uses `join_all`, so will only work when all futures return the
    // same type. If not, consider `tokio::join!`.

    let _ = env_logger::try_init();

    let (tx, rx) = channel::unbounded();
    tokio::spawn(
        tarpc::Server::default()
            .incoming(stream::once(ready(rx)))
            .respond_with(Server.serve()),
    );

    let client = ServiceClient::new(client::Config::default(), tx).spawn()?;
    //let req1 = client.add(context::current(), 1, 2); // Why exactly does the client need to be mut? If it does, is interior mutability a possibility?
    //let req2 = client.add(context::current(), 3, 4);
    //futures::future::join_all(vec![req1, req2]).await;

    Ok(())
}

#[tokio::test(threaded_scheduler)]
async fn concurrent_spawn() -> io::Result<()> {
    // This first test calls tokio::spawn() to allude to a fundamental problem. To get this to work
    // may be sufficient for all following examples to fall into place.

    let _ = env_logger::try_init();

    let (tx, rx) = channel::unbounded();
    tokio::spawn(
        tarpc::Server::default()
            .incoming(stream::once(ready(rx)))
            .respond_with(Server.serve()),
    );

    // Attempt to spin off the RPC call
    let mut client = ServiceClient::new(client::Config::default(), tx).spawn()?;
    let req1 = client.add(context::current(), 1, 2);
    //tokio::spawn(req1);  // error: argument requires that `client` is borrowed for `'static`

    Ok(())
}

#[tokio::test(threaded_scheduler)]
async fn concurrent_mut() -> io::Result<()> {
    // This test points out how it's problematic for the client to require mut.

    let _ = env_logger::try_init();

    let (tx, rx) = channel::unbounded();
    tokio::spawn(
        tarpc::Server::default()
            .incoming(stream::once(ready(rx)))
            .respond_with(Server.serve()),
    );

    // Attempt to run two RPC calls concurrently
    let mut client = ServiceClient::new(client::Config::default(), tx).spawn()?;
    let req1 = client.add(context::current(), 1, 2);
    //let req2 = client.add(context::current(), 3, 4); // Doesn't compile because the previous future borrow the mutable client
    //futures::future::join_all(vec![req1, req2]).await;

    Ok(())
}

#[tokio::test(threaded_scheduler)]
async fn concurrent_borrow() -> io::Result<()> {
    // This test highlights how the client.clone() trick doesn't work with join_all()

    let _ = env_logger::try_init();

    let (tx, rx) = channel::unbounded();
    tokio::spawn(
        tarpc::Server::default()
            .incoming(stream::once(ready(rx)))
            .respond_with(Server.serve()),
    );

    // Attempt to run two RPC calls concurrently
    let client = ServiceClient::new(client::Config::default(), tx).spawn()?;
    let mut c = client.clone();
    let req1 = c.add(context::current(), 1, 2);

    let mut c = client.clone();
    let req2 = c.add(context::current(), 3, 4); // Using join_all tells us that this borrow doesn't live long enough
    //futures::future::join_all(vec![req1, req2]).await;

    Ok(())
}

#[tokio::test(threaded_scheduler)]
async fn concurrent_workaround() -> io::Result<()> {
    // This test shows how concurrency is possible in today's code, but only if everything stays in scope

    let _ = env_logger::try_init();

    let (tx, rx) = channel::unbounded();
    tokio::spawn(
        tarpc::Server::default()
            .incoming(stream::once(ready(rx)))
            .respond_with(Server.serve()),
    );

    let client = ServiceClient::new(client::Config::default(), tx).spawn()?;
    let mut c = client.clone(); // Only works because 'c' stays in scope until the call to await
    let req1 = c.add(context::current(), 1, 2);

    let mut c = client.clone();
    let req2 = c.add(context::current(), 3, 4);
    let (resp1, resp2) = tokio::join!(req1, req2);
    resp1?;
    resp2?;

    Ok(())
}

