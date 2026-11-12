use bytes::Bytes;
use iroh::endpoint::{Connection, RecvStream, SendStream};
use iroh::{Endpoint, NodeId};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tauri::State;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream, UdpSocket};

const ALPN: &[u8] = b"p2p_connect/tunnel/1";
const UDP_IDLE: Duration = Duration::from_secs(120);

#[derive(Default)]
struct Tasks(Mutex<Vec<tauri::async_runtime::JoinHandle<()>>>);

fn want(proto: &str) -> Result<(bool, bool), String> {
    match proto {
        "tcp" => Ok((true, false)),
        "udp" => Ok((false, true)),
        "both" => Ok((true, true)),
        _ => Err("プロトコルが不正です".into()),
    }
}

// ---------- TCP ----------
async fn pipe(tcp: TcpStream, mut send: SendStream, mut recv: RecvStream) {
    let (mut r, mut w) = tcp.into_split();
    let a = async {
        let _ = tokio::io::copy(&mut r, &mut send).await;
        let _ = send.finish();
    };
    let b = async {
        let _ = tokio::io::copy(&mut recv, &mut w).await;
        let _ = w.shutdown().await;
    };
    tokio::join!(a, b);
}

async fn host_tcp(conn: Connection, target: String) {
    while let Ok((send, mut recv)) = conn.accept_bi().await {
        let target = target.clone();
        tokio::spawn(async move {
            let mut hello = [0u8; 1];
            if recv.read_exact(&mut hello).await.is_err() {
                return;
            }
            if let Ok(tcp) = TcpStream::connect(&target).await {
                pipe(tcp, send, recv).await;
            }
        });
    }
}

// ---------- UDP (QUIC datagram: [flow_id u32 BE][payload]) ----------
type Flows = Arc<tokio::sync::Mutex<HashMap<u32, Arc<UdpSocket>>>>;

async fn host_udp(conn: Connection, target: String) {
    let flows: Flows = Default::default();
    while let Ok(d) = conn.read_datagram().await {
        if d.len() < 4 {
            continue;
        }
        let id = u32::from_be_bytes([d[0], d[1], d[2], d[3]]);
        let sock = {
            let mut m = flows.lock().await;
            if let Some(s) = m.get(&id) {
                s.clone()
            } else {
                let Ok(s) = UdpSocket::bind("0.0.0.0:0").await else { continue };
                if s.connect(&target).await.is_err() {
                    continue;
                }
                let s = Arc::new(s);
                m.insert(id, s.clone());
                let (s2, c2, f2) = (s.clone(), conn.clone(), flows.clone());
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 65535];
                    loop {
                        match tokio::time::timeout(UDP_IDLE, s2.recv(&mut buf)).await {
                            Ok(Ok(n)) => {
                                let mut out = Vec::with_capacity(n + 4);
                                out.extend_from_slice(&id.to_be_bytes());
                                out.extend_from_slice(&buf[..n]);
                                if c2.send_datagram(Bytes::from(out)).is_err() {
                                    break;
                                }
                            }
                            Ok(Err(_)) => tokio::task::yield_now().await,
                            Err(_) => break, // アイドル
                        }
                    }
                    f2.lock().await.remove(&id);
                });
                s
            }
        };
        let _ = sock.send(&d[4..]).await;
    }
}

async fn client_udp(conn: Connection, sock: UdpSocket) {
    let mut buf = vec![0u8; 65535];
    let mut next = 1u32;
    let mut a2i: HashMap<SocketAddr, u32> = HashMap::new();
    let mut i2a: HashMap<u32, SocketAddr> = HashMap::new();
    loop {
        tokio::select! {
            r = sock.recv_from(&mut buf) => {
                let Ok((n, addr)) = r else { tokio::task::yield_now().await; continue };
                let id = match a2i.get(&addr) {
                    Some(i) => *i,
                    None => { let i = next; next += 1; a2i.insert(addr, i); i2a.insert(i, addr); i }
                };
                let mut out = Vec::with_capacity(n + 4);
                out.extend_from_slice(&id.to_be_bytes());
                out.extend_from_slice(&buf[..n]);
                let _ = conn.send_datagram(Bytes::from(out));
            }
            d = conn.read_datagram() => {
                let Ok(d) = d else { break };
                if d.len() < 4 { continue; }
                let id = u32::from_be_bytes([d[0], d[1], d[2], d[3]]);
                if let Some(a) = i2a.get(&id) { let _ = sock.send_to(&d[4..], a).await; }
            }
        }
    }
}

// ---------- commands ----------
/// ホスト側: proto = tcp / udp / both。接続IDを返す
#[tauri::command]
async fn start_host(target: String, proto: String, tasks: State<'_, Tasks>) -> Result<String, String> {
    let (tcp, udp) = want(&proto)?;
    let ep = Endpoint::builder()
        .alpns(vec![ALPN.to_vec()])
        .discovery_n0()
        .bind()
        .await
        .map_err(|e| e.to_string())?;
    let id = ep.node_id().to_string();

    let h = tauri::async_runtime::spawn(async move {
        while let Some(incoming) = ep.accept().await {
            let target = target.clone();
            tokio::spawn(async move {
                let Ok(conn) = incoming.await else { return };
                if udp {
                    tokio::spawn(host_udp(conn.clone(), target.clone()));
                }
                if tcp {
                    host_tcp(conn, target).await;
                } else {
                    conn.closed().await;
                }
            });
        }
    });
    tasks.0.lock().unwrap().push(h);
    Ok(id)
}

/// クライアント側: localhost:listen_port をTCP/UDPの入口にする
#[tauri::command]
async fn start_client(id: String, listen_port: u16, proto: String, tasks: State<'_, Tasks>) -> Result<(), String> {
    let (tcp, udp) = want(&proto)?;
    let node_id: NodeId = id.trim().parse().map_err(|e| format!("IDが不正です: {e}"))?;

    // 先にポートを確保してエラーを早く返す
    let listener = if tcp {
        Some(TcpListener::bind(("127.0.0.1", listen_port)).await
            .map_err(|e| format!("TCPポート{listen_port}を開けません: {e}"))?)
    } else { None };
    let usock = if udp {
        Some(UdpSocket::bind(("127.0.0.1", listen_port)).await
            .map_err(|e| format!("UDPポート{listen_port}を開けません: {e}"))?)
    } else { None };

    let ep = Endpoint::builder().discovery_n0().bind().await.map_err(|e| e.to_string())?;
    let conn = ep.connect(node_id, ALPN).await.map_err(|e| format!("接続に失敗しました: {e}"))?;

    let mut hs = Vec::new();
    if let Some(listener) = listener {
        let (conn, ep) = (conn.clone(), ep.clone());
        hs.push(tauri::async_runtime::spawn(async move {
            let _ep = ep;
            while let Ok((tcp, _)) = listener.accept().await {
                let conn = conn.clone();
                tokio::spawn(async move {
                    if let Ok((mut send, recv)) = conn.open_bi().await {
                        if send.write_all(&[0u8]).await.is_ok() {
                            pipe(tcp, send, recv).await;
                        }
                    }
                });
            }
        }));
    }
    if let Some(usock) = usock {
        let ep = ep.clone();
        hs.push(tauri::async_runtime::spawn(async move {
            let _ep = ep;
            client_udp(conn, usock).await;
        }));
    }
    tasks.0.lock().unwrap().extend(hs);
    Ok(())
}

#[tauri::command]
fn stop_all(tasks: State<'_, Tasks>) {
    for h in tasks.0.lock().unwrap().drain(..) {
        h.abort();
    }
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .manage(Tasks::default())
        .invoke_handler(tauri::generate_handler![start_host, start_client, stop_all])
        .run(tauri::generate_context!())
        .expect("起動に失敗しました");
}
