use axum::{Json, Router, response::IntoResponse, routing::get};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::{OpenApi, ToSchema};
use utoipa_swagger_ui::SwaggerUi;

#[derive(Serialize, Deserialize, ToSchema, Debug)]
struct RemoteActorDetail {
    name: String,
    hostname: String,
    port: u16,
}

#[utoipa::path(
    post,
    path = "/start_actor",
    request_body(
        content = Value,
        description = "Actor arguments as arbitrary JSON",
        content_type = "application/json"
    ),
    responses(
        (status = 201, description = "Start a new actor", body = RemoteActorDetail)
    )
)]
async fn start_actor(Json(actor_args): Json<Value>) -> impl IntoResponse {
    let detail = RemoteActorDetail {
        name: "".to_string(),
        hostname: "".to_string(),
        port: 7000,
    };
    (axum::http::StatusCode::CREATED, Json(detail))
}

#[utoipa::path(
    post,
    path = "/actor_added",
    request_body(
        content = RemoteActorDetail,
        description = "Remote Actor Detail",
        content_type = "application/json"
    ),
    responses(
        (status = 201, description = "Notify actor start on remote")
    )
)]
async fn actor_added(Json(actor_args): Json<RemoteActorDetail>) -> impl IntoResponse {
    println!("Received actor detail: {:?}", actor_args);
    (axum::http::StatusCode::CREATED, "Actor started!")
}

#[derive(OpenApi)]
#[openapi(paths(start_actor))]
struct ApiDoc;

pub async fn webserver() {
    let app = Router::new()
        .route("/hello", get(start_actor))
        .merge(SwaggerUi::new("/docs").url("/api-doc/openapi.json", ApiDoc::openapi()));

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
