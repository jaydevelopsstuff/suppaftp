//! Wire ordering of positioned Tokio transfers. No external FTP server needed.
#![cfg(feature = "tokio")]

use std::future::Future;
use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;

use suppaftp::tokio::AsyncFtpStream;
use suppaftp::types::{FtpError, Mode};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

fn run(future: impl Future<Output = ()>) {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            tokio::time::timeout(Duration::from_secs(10), future)
                .await
                .expect("FTP fixture timed out");
        });
}

async fn peer(reject_rest: bool) -> (SocketAddr, tokio::task::JoinHandle<Vec<String>>) {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let (read, mut write) = socket.into_split();
        let mut read = BufReader::new(read);
        let mut passive = None;
        let mut active = None;
        let mut commands = Vec::new();
        write.write_all(b"220 Fixture\r\n").await.unwrap();
        loop {
            let mut line = String::new();
            if read.read_line(&mut line).await.unwrap() == 0 {
                break;
            }
            let line = line.trim_end().to_owned();
            let (cmd, arg) = line.split_once(' ').unwrap_or((&line, ""));
            match cmd {
                "PASV" | "EPSV" => {
                    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
                    let port = listener.local_addr().unwrap().port();
                    passive = Some(listener);
                    let response = if cmd == "PASV" {
                        format!("227 Passive (127,0,0,1,{},{})\r\n", port / 256, port % 256)
                    } else {
                        format!("229 Extended Passive (|||{port}|)\r\n")
                    };
                    write.write_all(response.as_bytes()).await.unwrap();
                }
                "PORT" => {
                    let parts: Vec<u16> = arg.split(',').map(|n| n.parse().unwrap()).collect();
                    active = Some(SocketAddr::from((
                        Ipv4Addr::LOCALHOST,
                        parts[4] * 256 + parts[5],
                    )));
                    write.write_all(b"200 Active ready\r\n").await.unwrap();
                }
                "REST" => {
                    assert!(commands.last().is_some_and(|last: &String| {
                        ["PASV", "EPSV", "PORT"]
                            .iter()
                            .any(|setup| last.starts_with(setup))
                    }));
                    let response: &[u8] = if reject_rest {
                        b"502 REST unsupported\r\n"
                    } else {
                        b"350 Restart ready\r\n"
                    };
                    write.write_all(response).await.unwrap();
                }
                "RETR" | "STOR" => {
                    if arg == "resumed" {
                        assert!(commands.last().unwrap().starts_with("REST "));
                        assert!(!reject_rest);
                    }
                    write.write_all(b"150 Opening data\r\n").await.unwrap();
                    let mut data = if let Some(listener) = passive.take() {
                        listener.accept().await.unwrap().0
                    } else {
                        TcpStream::connect(active.take().unwrap()).await.unwrap()
                    };
                    if cmd == "RETR" {
                        data.write_all(b"end").await.unwrap();
                    } else {
                        let mut bytes = Vec::new();
                        data.read_to_end(&mut bytes).await.unwrap();
                        assert_eq!(bytes, b"end");
                    }
                    drop(data);
                    write.write_all(b"226 Complete\r\n").await.unwrap();
                }
                "NOOP" => write
                    .write_all(b"200 Still synchronized\r\n")
                    .await
                    .unwrap(),
                "QUIT" => {
                    write.write_all(b"221 Bye\r\n").await.unwrap();
                    commands.push(line);
                    break;
                }
                _ => panic!("unexpected command: {line}"),
            }
            commands.push(line);
        }
        commands
    });
    (address, task)
}

#[test]
fn positioned_transfers_order_rest_after_setup_and_finish_cleanly() {
    run(async {
        for mode in [Mode::Passive, Mode::ExtendedPassive, Mode::Active] {
            let (address, server) = peer(false).await;
            let mut client = AsyncFtpStream::connect(address).await.unwrap();
            client.set_mode(mode);
            for offset in [0, (8_u64 << 30) as usize + 3] {
                let mut reader = client.retr_as_stream_at("resumed", offset).await.unwrap();
                let mut bytes = Vec::new();
                reader.read_to_end(&mut bytes).await.unwrap();
                assert_eq!(bytes, b"end");
                reader.finish().await.unwrap();
                client.noop().await.unwrap();
                let mut writer = client.put_with_stream_at("resumed", offset).await.unwrap();
                writer.write_all(b"end").await.unwrap();
                writer.finish().await.unwrap();
                client.noop().await.unwrap();
            }
            // The existing fresh openers must not start issuing REST.
            let mut writer = client.put_with_stream("fresh").await.unwrap();
            writer.write_all(b"end").await.unwrap();
            writer.finish().await.unwrap();
            let mut reader = client.retr_as_stream("fresh").await.unwrap();
            reader.read_to_end(&mut Vec::new()).await.unwrap();
            assert!(matches!(
                client.put_with_stream_at("resumed", 42).await,
                Err(FtpError::DataConnectionAlreadyOpen)
            ));
            reader.finish().await.unwrap();
            client.quit().await.unwrap();
            let commands = server.await.unwrap();
            assert_eq!(
                commands
                    .iter()
                    .filter(|cmd| cmd.starts_with("REST "))
                    .count(),
                4
            );
            assert_eq!(commands.iter().filter(|cmd| *cmd == "REST 0").count(), 2);
            assert_eq!(
                commands
                    .iter()
                    .filter(|cmd| *cmd == &format!("REST {}", (8_u64 << 30) + 3))
                    .count(),
                2
            );
        }
    });
}

#[test]
fn refused_rest_never_sends_the_transfer_command() {
    run(async {
        for upload in [false, true] {
            let (address, server) = peer(true).await;
            let mut client = AsyncFtpStream::connect(address).await.unwrap();
            let result = if upload {
                client.put_with_stream_at("resumed", 42).await
            } else {
                client.retr_as_stream_at("resumed", 42).await
            };
            assert!(matches!(result, Err(FtpError::UnexpectedResponse(_))));
            drop(client);
            let commands = server.await.unwrap();
            assert_eq!(commands, ["PASV", "REST 42"]);
        }
    });
}

#[test]
fn invalid_names_are_rejected_before_setting_a_restart_marker() {
    run(async {
        let (address, server) = peer(false).await;
        let mut client = AsyncFtpStream::connect(address).await.unwrap();
        assert!(
            client
                .retr_as_stream_at("bad\r\nDELE other", 42)
                .await
                .is_err()
        );
        assert!(
            client
                .put_with_stream_at("bad\nDELE other", 42)
                .await
                .is_err()
        );
        client.noop().await.unwrap();
        client.quit().await.unwrap();
        assert_eq!(server.await.unwrap(), ["NOOP", "QUIT"]);
    });
}

#[test]
fn control_eof_is_transport_loss_and_malformed_reply_stays_bad_response() {
    run(async {
        for (reply, eof) in [
            (b"".as_slice(), true),
            (b"200-Opening\r\n".as_slice(), true),
            (b"oops\r\n".as_slice(), false),
        ] {
            let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                socket.write_all(b"220 Fixture\r\n").await.unwrap();
                let mut reader = BufReader::new(socket);
                let mut command = String::new();
                reader.read_line(&mut command).await.unwrap();
                assert_eq!(command, "NOOP\r\n");
                reader.get_mut().write_all(reply).await.unwrap();
            });
            let mut client = AsyncFtpStream::connect(address).await.unwrap();
            let error = client.noop().await.unwrap_err();
            if eof {
                assert!(
                    matches!(error, FtpError::ConnectionError(ref io) if io.kind() == std::io::ErrorKind::UnexpectedEof),
                    "{error:?}"
                );
            } else {
                assert!(matches!(error, FtpError::BadResponse), "{error:?}");
            }
            server.await.unwrap();
        }
    });
}
