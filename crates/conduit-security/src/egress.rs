use conduit_core::adapter::SecurityPolicy;
use std::collections::HashMap;
use std::net::SocketAddr;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{timeout, Duration};
use tracing::{debug, warn};

const MAX_CONNECT_HEADER_BYTES: usize = 8 * 1024;
const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(5);

pub struct ProxyHandle {
    task: tokio::task::JoinHandle<()>,
}

impl Drop for ProxyHandle {
    fn drop(&mut self) {
        self.task.abort();
    }
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

pub async fn start_proxy_for_policy(
    policy: &SecurityPolicy,
) -> std::io::Result<(HashMap<String, String>, Option<ProxyHandle>)> {
    validate_policy_supported(policy)?;
    if policy.egress_allowlist.is_empty() {
        return Ok((HashMap::new(), None));
    }

    let (addr, handle) = start_proxy(policy.egress_allowlist.clone()).await?;
    Ok((proxy_env(addr), Some(handle)))
}

pub fn validate_policy_supported(policy: &SecurityPolicy) -> std::io::Result<()> {
    validate_policy_supported_for_os(policy, std::env::consts::OS)
}

fn validate_policy_supported_for_os(policy: &SecurityPolicy, os: &str) -> std::io::Result<()> {
    if policy.egress_allowlist.is_empty() || os == "macos" {
        return Ok(());
    }

    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        format!(
            "egress allowlists are not enforceable on {os} with the current sandbox proxy design"
        ),
    ))
}

pub fn proxy_env(addr: SocketAddr) -> HashMap<String, String> {
    let proxy = format!("http://{addr}");
    HashMap::from([
        ("HTTPS_PROXY".to_string(), proxy.clone()),
        ("HTTP_PROXY".to_string(), proxy.clone()),
        ("https_proxy".to_string(), proxy.clone()),
        ("http_proxy".to_string(), proxy),
    ])
}

async fn handle_connection(socket: TcpStream, allowlist: Vec<String>) -> std::io::Result<()> {
    let (reader, mut writer) = socket.into_split();
    let mut reader = BufReader::new(reader);
    let mut remaining_header_bytes = MAX_CONNECT_HEADER_BYTES;

    let request_line = match read_line_capped(&mut reader, &mut remaining_header_bytes).await {
        Ok(Some(line)) => line,
        Ok(None) => return Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::InvalidData => {
            writer
                .write_all(b"HTTP/1.1 431 Request Header Fields Too Large\r\n\r\n")
                .await?;
            return Ok(());
        }
        Err(error) => return Err(error),
    };
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
        match read_line_capped(&mut reader, &mut remaining_header_bytes).await {
            Ok(Some(line)) if line == "\r\n" || line == "\n" => break,
            Ok(Some(_)) => {}
            Ok(None) => break,
            Err(error) if error.kind() == std::io::ErrorKind::InvalidData => {
                writer
                    .write_all(b"HTTP/1.1 431 Request Header Fields Too Large\r\n\r\n")
                    .await?;
                return Ok(());
            }
            Err(error) => return Err(error),
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

async fn read_line_capped<R>(
    reader: &mut R,
    remaining: &mut usize,
) -> std::io::Result<Option<String>>
where
    R: AsyncBufRead + Unpin,
{
    if *remaining == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "proxy header exceeds byte limit",
        ));
    }

    let mut bytes = Vec::new();
    let read = timeout(
        HEADER_READ_TIMEOUT,
        reader
            .take((*remaining + 1) as u64)
            .read_until(b'\n', &mut bytes),
    )
    .await
    .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "proxy header timeout"))??;

    if read == 0 {
        return Ok(None);
    }
    if read > *remaining {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "proxy header exceeds byte limit",
        ));
    }

    *remaining -= read;
    String::from_utf8(bytes)
        .map(Some)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))
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
    use super::{host_allowed, validate_policy_supported_for_os};
    use conduit_core::adapter::SecurityPolicy;

    #[test]
    fn host_allowlist_matches_exact_and_subdomain() {
        let allowlist = vec!["example.com".to_string()];
        assert!(host_allowed("example.com", &allowlist));
        assert!(host_allowed("api.example.com", &allowlist));
        assert!(!host_allowed("badexample.com", &allowlist));
    }

    #[test]
    fn allowlisted_egress_is_supported_on_macos() {
        let policy = SecurityPolicy {
            egress_allowlist: vec!["api.openai.com".to_string()],
            ..Default::default()
        };
        assert!(validate_policy_supported_for_os(&policy, "macos").is_ok());
    }

    #[test]
    fn allowlisted_egress_fails_closed_on_linux() {
        let policy = SecurityPolicy {
            egress_allowlist: vec!["api.openai.com".to_string()],
            ..Default::default()
        };
        let error = validate_policy_supported_for_os(&policy, "linux").unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::Unsupported);
    }

    #[test]
    fn empty_allowlist_is_supported_on_linux_as_no_network() {
        let policy = SecurityPolicy::default();
        assert!(validate_policy_supported_for_os(&policy, "linux").is_ok());
    }
}
