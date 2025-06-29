use crate::mempool::MempoolClient;
use crate::store::MhinStore;
use crate::web::handlers::{
    balances_page, compose_transaction_endpoint, get_address_balance, get_nicehashes, get_stats, 
    home_page, protocol_page, wallet_page, AppState,
};

use actix::prelude::*;
use actix_cors::Cors;
use actix_files::Files;
use actix_web::{middleware::Logger, web, App, HttpServer};
use log::info;
use std::sync::Arc;
use tera::Tera;
use tokio::sync::oneshot;

#[derive(Message)]
#[rtype(result = "()")]
pub struct StartWebServer {
    pub host: String,
    pub port: u16,
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct StopWebServer {
    pub confirm_tx: Option<oneshot::Sender<()>>,
}

impl StopWebServer {
    pub fn new() -> Self {
        Self { confirm_tx: None }
    }

    pub fn with_confirmation() -> (Self, oneshot::Receiver<()>) {
        let (tx, rx) = oneshot::channel();
        (
            Self {
                confirm_tx: Some(tx),
            },
            rx,
        )
    }
}

pub struct WebServerActor {
    mhin_store: Arc<MhinStore>,
    server_handle: Option<actix_web::dev::ServerHandle>,
}

impl WebServerActor {
    pub fn new(mhin_store: Arc<MhinStore>) -> Self {
        Self {
            mhin_store,
            server_handle: None,
        }
    }
}

impl Actor for WebServerActor {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        info!("WebServerActor started");
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        info!("WebServerActor stopped");
    }
}

impl Handler<StartWebServer> for WebServerActor {
    type Result = ResponseActFuture<Self, ()>;

    fn handle(&mut self, msg: StartWebServer, _ctx: &mut Self::Context) -> Self::Result {
        let mhin_store = self.mhin_store.clone();
        let host = msg.host;
        let port = msg.port;

        Box::pin(
            async move {
                info!("Starting web server on {}:{}", host, port);

                // Initialize Tera template engine with absolute path
                let templates_path = std::env::current_dir().unwrap().join("templates/**/*");
                let tera = Tera::new(templates_path.to_str().unwrap())
                    .expect("Failed to initialize Tera template engine");

                let mempool_client = MempoolClient::new();
                let app_state = web::Data::new(AppState {
                    mhin_store,
                    mempool_client,
                    tera,
                });

                // Get absolute path for static files
                let static_path = std::env::current_dir().unwrap().join("static");

                let server = HttpServer::new(move || {
                    App::new()
                        .app_data(app_state.clone())
                        .wrap(
                            Cors::default()
                                .allow_any_origin()
                                .allow_any_method()
                                .allow_any_header(),
                        )
                        .wrap(Logger::default())
                        // Static files with absolute path
                        .service(Files::new("/static", static_path.clone()).show_files_listing())
                        // HTML pages
                        .route("/", web::get().to(home_page))
                        .route("/balances", web::get().to(balances_page))
                        .route("/protocol", web::get().to(protocol_page))
                        .route("/wallet", web::get().to(wallet_page)) 
                        // API endpoints
                        .route("/nicehashes", web::get().to(get_nicehashes))
                        .route("/stats", web::get().to(get_stats))
                        .route(
                            "/addresses/{address}/balance",
                            web::get().to(get_address_balance),
                        )
                        .route(
                            "/addresses/{address}/compose",
                            web::get().to(compose_transaction_endpoint),
                        )
                })
                .bind(format!("{}:{}", host, port))
                .expect("Failed to bind web server")
                .run();

                let handle = server.handle();

                tokio::spawn(server);

                (handle,)
            }
            .into_actor(self)
            .map(|(handle,), actor, _ctx| {
                actor.server_handle = Some(handle);
                info!("Web server started successfully");
            }),
        )
    }
}

impl Handler<StopWebServer> for WebServerActor {
    type Result = ResponseActFuture<Self, ()>;

    fn handle(&mut self, msg: StopWebServer, _ctx: &mut Self::Context) -> Self::Result {
        info!("Stopping web server...");

        let server_handle = self.server_handle.take();
        let confirm_tx = msg.confirm_tx;

        Box::pin(
            async move {
                if let Some(handle) = server_handle {
                    handle.stop(true).await;
                    info!("Web server stopped");
                }

                if let Some(tx) = confirm_tx {
                    let _ = tx.send(());
                }
            }
            .into_actor(self),
        )
    }
}
