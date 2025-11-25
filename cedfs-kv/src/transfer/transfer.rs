use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use warp::Filter;
use warp::http::StatusCode;
use bytes::Bytes;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KVPoll {
    Failed = 0,
    Bootstrapping = 1,
    WaitingForInput = 2,
    Transferring = 3,
    Success = 4,
}

#[derive(Clone)]
pub struct KVBootstrapServer {
    port: u16,
    store: Arc<Mutex<HashMap<String, Bytes>>>,
    shutdown_tx: Option<tokio::sync::mpsc::Sender<()>>,
}

impl KVBootstrapServer {
    pub fn new(port: u16) -> Self {
        Self {
            port,
            store: Arc::new(Mutex::new(HashMap::new())),
            shutdown_tx: None,
        }
    }

    pub fn run(&mut self) -> JoinHandle<()> {
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::mpsc::channel::<()>(1);
        self.shutdown_tx = Some(shutdown_tx);

        let port = self.port;
        let store = Arc::clone(&self.store);

        tokio::spawn(async move {
            let store_filter = warp::any().map(move || Arc::clone(&store));

            let metadata_route = warp::path("metadata")
                .and(warp::query::<HashMap<String, String>>())
                .and(store_filter)
                .and(warp::method())
                .and(warp::body::bytes().or(warp::any().map(|| Bytes::new())).unify())
                .and_then(handle_metadata);

            let routes = metadata_route;

            let (addr, server) = warp::serve(routes)
                .bind_with_graceful_shutdown(([0, 0, 0, 0], port), async move {
                    shutdown_rx.recv().await;
                });

            println!("Server running on http://{}", addr);
            server.await;
        })
    }

    pub fn poll(&self) -> KVPoll {
        // Placeholder implementation
        KVPoll::WaitingForInput
    }

    pub async fn close(&self) {
        if let Some(tx) = &self.shutdown_tx {
            let _ = tx.send(()).await;
            println!("Stopping server...");
        }
    }
}

async fn handle_metadata(
    query: HashMap<String, String>,
    store: Arc<Mutex<HashMap<String, Bytes>>>,
    method: warp::http::Method,
    body: Bytes,
) -> Result<warp::http::Response<warp::hyper::Body>, warp::Rejection> {
    let key = query.get("key").map(|s| s.as_str()).unwrap_or("");

    match method {
        warp::http::Method::GET => handle_get(key, store).await,
        warp::http::Method::PUT => handle_put(key, body, store).await,
        warp::http::Method::DELETE => handle_delete(key, store).await,
        _ => Ok(warp::http::Response::builder()
            .status(StatusCode::METHOD_NOT_ALLOWED)
            .header("Content-Type", "text/plain")
            .body(warp::hyper::Body::from("Method not allowed"))
            .unwrap()),
    }
}

async fn handle_get(
    key: &str,
    store: Arc<Mutex<HashMap<String, Bytes>>>,
) -> Result<warp::http::Response<warp::hyper::Body>, warp::Rejection> {
    let store_lock = store.lock().await;
    
    match store_lock.get(key) {
        Some(value) => {
            Ok(warp::http::Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "application/json")
                .body(warp::hyper::Body::from(value.clone()))
                .unwrap())
        }
        None => {
            Ok(warp::http::Response::builder()
                .status(StatusCode::NOT_FOUND)
                .header("Content-Type", "application/json")
                .body(warp::hyper::Body::from("metadata not found"))
                .unwrap())
        }
    }
}

async fn handle_put(
    key: &str,
    data: Bytes,
    store: Arc<Mutex<HashMap<String, Bytes>>>,
) -> Result<warp::http::Response<warp::hyper::Body>, warp::Rejection> {
    let mut store_lock = store.lock().await;

    if key.contains("rpc_meta") && store_lock.contains_key(key) {
        return Ok(warp::http::Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .header("Content-Type", "application/json")
            .body(warp::hyper::Body::from("Duplicate rpc_meta key not allowed"))
            .unwrap());
    }

    store_lock.insert(key.to_string(), data);

    Ok(warp::http::Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/json")
        .body(warp::hyper::Body::from("metadata updated"))
        .unwrap())
}

async fn handle_delete(
    key: &str,
    store: Arc<Mutex<HashMap<String, Bytes>>>,
) -> Result<warp::http::Response<warp::hyper::Body>, warp::Rejection> {
    let mut store_lock = store.lock().await;

    if store_lock.remove(key).is_none() {
        return Ok(warp::http::Response::builder()
            .status(StatusCode::NOT_FOUND)
            .header("Content-Type", "application/json")
            .body(warp::hyper::Body::from("metadata not found"))
            .unwrap());
    }

    Ok(warp::http::Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/json")
        .body(warp::hyper::Body::from("metadata deleted"))
        .unwrap())
}

// #[tokio::main]
// async fn main() {
//     let mut server = KVBootstrapServer::new(8080);
//     server.run();

//     println!("Press Ctrl+C to stop the server...");
    
//     tokio::signal::ctrl_c()
//         .await
//         .expect("Failed to listen for Ctrl+C");

//     server.close().await;
    
//     // Give server time to shutdown gracefully
//     tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
//     println!("Server stopped");
// }