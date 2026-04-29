use conduit_core::adapter::SecurityPolicy;
use conduit_security::egress::{start_proxy, start_proxy_for_policy};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[tokio::test]
async fn denies_host_not_in_allowlist() {
    let allowlist = vec!["api.openai.com".to_string()];
    let (addr, _handle) = start_proxy(allowlist).await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    stream
        .write_all(b"CONNECT evil.com:443 HTTP/1.1\r\nHost: evil.com:443\r\n\r\n")
        .await
        .unwrap();

    let mut buffer = [0_u8; 64];
    let read = stream.read(&mut buffer).await.unwrap();
    let response = std::str::from_utf8(&buffer[..read]).unwrap();
    assert!(response.starts_with("HTTP/1.1 403"));
}

#[tokio::test]
async fn allows_host_in_allowlist_then_tunnels() {
    let allowlist = vec!["127.0.0.1".to_string()];
    let (addr, _handle) = start_proxy(allowlist).await.unwrap();
    let echo = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let echo_addr = echo.local_addr().unwrap();

    tokio::spawn(async move {
        let (mut stream, _) = echo.accept().await.unwrap();
        let mut buffer = [0_u8; 5];
        let _ = stream.read_exact(&mut buffer).await;
        let _ = stream.write_all(&buffer).await;
    });

    let mut stream = TcpStream::connect(addr).await.unwrap();
    let request = format!(
        "CONNECT 127.0.0.1:{} HTTP/1.1\r\nHost: 127.0.0.1:{}\r\n\r\n",
        echo_addr.port(),
        echo_addr.port()
    );
    stream.write_all(request.as_bytes()).await.unwrap();

    let mut buffer = [0_u8; 128];
    let read = stream.read(&mut buffer).await.unwrap();
    assert!(std::str::from_utf8(&buffer[..read])
        .unwrap()
        .starts_with("HTTP/1.1 200"));

    stream.write_all(b"hello").await.unwrap();
    let mut echo_buffer = [0_u8; 5];
    stream.read_exact(&mut echo_buffer).await.unwrap();
    assert_eq!(&echo_buffer, b"hello");
}

#[tokio::test]
async fn rejects_oversized_connect_headers() {
    let allowlist = vec!["api.openai.com".to_string()];
    let (addr, _handle) = start_proxy(allowlist).await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let request = format!(
        "CONNECT api.openai.com:443 HTTP/1.1\r\nX-Fill: {}\r\n\r\n",
        "x".repeat(9 * 1024)
    );

    stream.write_all(request.as_bytes()).await.unwrap();

    let mut buffer = [0_u8; 64];
    let read = stream.read(&mut buffer).await.unwrap();
    let response = std::str::from_utf8(&buffer[..read]).unwrap();
    assert!(response.starts_with("HTTP/1.1 431"));
}

#[tokio::test]
async fn policy_allowlist_starts_proxy_and_returns_proxy_env() {
    let policy = SecurityPolicy {
        egress_allowlist: vec!["api.openai.com".to_string()],
        ..Default::default()
    };

    let (env, handle) = start_proxy_for_policy(&policy).await.unwrap();

    assert!(handle.is_some());
    assert!(env["HTTPS_PROXY"].starts_with("http://127.0.0.1:"));
    assert_eq!(env["HTTP_PROXY"], env["HTTPS_PROXY"]);
}
