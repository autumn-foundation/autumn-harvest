//! JSON-RPC client for remote MCP and A2A tasks (issue #2006).

use std::collections::HashMap;

use autumn_harvest::remote_task::{
    RemoteFuture, RemoteProtocol, RemoteTaskError, RemoteTaskHandle, RemoteTaskRequest,
    RemoteTaskStart, RemoteTaskState, RemoteTaskTransport,
};

/// One remote server.
#[derive(Debug, Clone)]
pub struct RemoteServer {
    endpoint: String,
    protocol: RemoteProtocol,
    headers: Vec<(String, String)>,
}

impl RemoteServer {
    /// An MCP server at `endpoint`.
    #[must_use]
    pub fn mcp(endpoint: &str) -> Self {
        let _ = endpoint;
        todo!("issue #2006")
    }

    /// An A2A server at `endpoint`.
    #[must_use]
    pub fn a2a(endpoint: &str) -> Self {
        let _ = endpoint;
        todo!("issue #2006")
    }

    /// Send `Authorization: Bearer <token>`.
    #[must_use]
    pub fn bearer_token(self, token: &str) -> Self {
        let _ = token;
        todo!("issue #2006")
    }
}

/// A [`RemoteTaskTransport`] over JSON-RPC and HTTP.
#[derive(Debug, Clone, Default)]
pub struct HttpRemoteTasks {
    servers: HashMap<String, RemoteServer>,
}

impl HttpRemoteTasks {
    /// A client with no servers.
    #[must_use]
    pub fn new() -> Self {
        todo!("issue #2006")
    }

    /// Add the server `name`.
    #[must_use]
    pub fn server(self, name: &str, server: RemoteServer) -> Self {
        let _ = (name, server);
        todo!("issue #2006")
    }
}

impl RemoteTaskTransport for HttpRemoteTasks {
    fn start<'a>(
        &'a self,
        request: &'a RemoteTaskRequest,
        idempotency_key: &'a str,
    ) -> RemoteFuture<'a, Result<RemoteTaskStart, RemoteTaskError>> {
        let _ = (request, idempotency_key, &self.servers);
        todo!("issue #2006")
    }

    fn get<'a>(
        &'a self,
        handle: &'a RemoteTaskHandle,
    ) -> RemoteFuture<'a, Result<RemoteTaskState, RemoteTaskError>> {
        let _ = handle;
        todo!("issue #2006")
    }
}
