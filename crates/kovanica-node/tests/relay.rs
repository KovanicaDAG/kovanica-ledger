//! Long-lived TCP relay: the connection stays open across several messages,
//! and blocks/txs applied over it converge two nodes.

use std::net::TcpListener;
use std::thread;
use std::time::Duration;

use kovanica_node::{apply_relay, Node, RelayMsg, RelaySession};

fn genesis_node() -> Node {
    let mut node = Node::new();
    node.set_now_ms(1_000);
    node.genesis(3, 1000, 1000, 1).unwrap();
    node
}

#[test]
fn session_stays_open_for_hello_then_block_then_reply() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let handle = thread::spawn(move || {
        let mut server = RelaySession::accept(&listener).unwrap();
        server
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        server
            .send(&RelayMsg::Hello {
                from: "alpha".into(),
                advertised: vec!["beta".into()],
            })
            .unwrap();
        // Second message on the same socket — this is the long-lived part.
        let ping = server.recv().unwrap();
        match ping {
            RelayMsg::Hello { from, .. } => assert_eq!(from, "gamma"),
            other => panic!("expected hello, got {other:?}"),
        }
        server
            .send(&RelayMsg::Hello {
                from: "alpha".into(),
                advertised: vec![],
            })
            .unwrap();
    });

    let mut client = RelaySession::connect(addr).unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let first = client.recv().unwrap();
    match first {
        RelayMsg::Hello { from, advertised } => {
            assert_eq!(from, "alpha");
            assert_eq!(advertised, vec!["beta".to_string()]);
        }
        other => panic!("expected hello, got {other:?}"),
    }
    client
        .send(&RelayMsg::Hello {
            from: "gamma".into(),
            advertised: vec![],
        })
        .unwrap();
    let ack = client.recv().unwrap();
    match ack {
        RelayMsg::Hello { from, .. } => assert_eq!(from, "alpha"),
        other => panic!("expected ack hello, got {other:?}"),
    }
    handle.join().unwrap();
}

#[test]
fn a_produced_block_converges_over_a_live_session() {
    let mut producer = genesis_node();
    let sent = producer.send(1, 400, 2).unwrap();
    let record = producer.block_record(&sent.block).unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = thread::spawn(move || {
        let mut server = RelaySession::accept(&listener).unwrap();
        server
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        server.send(&RelayMsg::Block(record)).unwrap();
        // Keep the session open and send a second block after the first.
        server.recv().unwrap()
    });

    let mut receiver = genesis_node();
    let mut client = RelaySession::connect(addr).unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let msg = client.recv().unwrap();
    apply_relay(&mut receiver, msg).unwrap();
    assert_eq!(receiver.balance(&Node::address(2)).unwrap(), 400);
    assert_eq!(
        receiver.selected_tip().unwrap(),
        producer.selected_tip().unwrap()
    );

    // Reply on the same connection (proves it was not closed after one block).
    client
        .send(&RelayMsg::Hello {
            from: "receiver".into(),
            advertised: vec![],
        })
        .unwrap();
    let echoed = handle.join().unwrap();
    match echoed {
        RelayMsg::Hello { from, .. } => assert_eq!(from, "receiver"),
        other => panic!("expected hello reply, got {other:?}"),
    }
}

#[test]
fn a_pooled_tx_relays_over_tcp_and_is_produced() {
    let mut sender = genesis_node();
    let tx_id = sender.pool(1, 250, 3).unwrap();
    let tx = sender.mempool_tx(&tx_id).unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = thread::spawn(move || {
        let mut server = RelaySession::accept(&listener).unwrap();
        server.send(&RelayMsg::Tx(tx)).unwrap();
    });

    let mut receiver = genesis_node();
    let mut client = RelaySession::connect(addr).unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let msg = client.recv().unwrap();
    apply_relay(&mut receiver, msg).unwrap();
    assert!(receiver.mempool_tx(&tx_id).is_some());
    receiver.produce_block().unwrap();
    assert_eq!(receiver.balance(&Node::address(3)).unwrap(), 250);
    handle.join().unwrap();
}
