use std::net::SocketAddr;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, warn};

pub struct ProxyHandle {
    pub task: tokio::task::JoinHandle<()>,
}

pub async fn start_proxy(allowlist: Vec<String>) -> std::io::Result<(SocketAddr, ProxyHandle)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let task = tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((socket, _peer)) => {
                    let allowlist = allowlist.clone();
                    tokio::spawn(async move {
                        if let Err(error) = handle_connection(socket, allowlist).await {
                            debug!(%error, "egress connection closed");
                        }
                    });
                }
                Err(error) => warn!(%error, "egress proxy accept failed"),
            }
        }
    });

    Ok((addr, ProxyHandle { task }))
}

async fn handle_connection(socket: TcpStream, allowlist: Vec<String>) -> std::io::Result<()> {
    let (reader, mut writer) = socket.into_split();
    let mut reader = BufReader::new(reader);
    let mut request_line = String::new();

    reader.read_line(&mut request_line).await?;
    let parts: Vec<&str> = request_line.split_whitespace().collect();
    if parts.len() < 3 || !parts[0].eq_ignore_ascii_case("CONNECT") {
        writer
            .write_all(b"HTTP/1.1 405 Method Not Allowed\r\n\r\n")
            .await?;
        return Ok(());
    }

    let target = parts[1];
    let (host, port) = parse_connect_target(target);

    loop {
        let mut line = String::new();
        let read = reader.read_line(&mut line).await?;
        if read == 0 || line == "\r\n" || line == "\n" {
            break;
        }
    }

    if !host_allowed(host, &allowlist) {
        warn!(host, "egress denied");
        writer.write_all(b"HTTP/1.1 403 Forbidden\r\n\r\n").await?;
        return Ok(());
    }

    let mut upstream = match TcpStream::connect((host, port)).await {
        Ok(stream) => stream,
        Err(_) => {
            writer
                .write_all(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
                .await?;
            return Ok(());
        }
    };

    writer
        .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
        .await?;

    let reader = reader.into_inner();
    let mut client = reader
        .reunite(writer)
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    tokio::io::copy_bidirectional(&mut client, &mut upstream).await?;
    Ok(())
}

fn parse_connect_target(target: &str) -> (&str, u16) {
    match target.rsplit_once(':') {
        Some((host, port)) => (host, port.parse().unwrap_or(443)),
        None => (target, 443),
    }
}

fn host_allowed(host: &str, allowlist: &[String]) -> bool {
    let host = host.trim_end_matches('.');

    allowlist.iter().any(|allowed| {
        let allowed = allowed.trim().trim_end_matches('.');
        host == allowed || host.ends_with(&format!(".{allowed}"))
    })
}

#[cfg(test)]
mod tests {
    use super::host_allowed;

    #[test]
    fn host_allowlist_matches_exact_and_subdomain() {
        let allowlist = vec!["example.com".to_string()];
        assert!(host_allowed("example.com", &allowlist));
        assert!(host_allowed("api.example.com", &allowlist));
        assert!(!host_allowed("badexample.com", &allowlist));
    }
}
