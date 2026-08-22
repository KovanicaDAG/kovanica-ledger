//! Integration tests for multi-node block dissemination: nodes exchange blocks
//! and converge on the identical DAG and UTXO state — in-process and over TCP.

use std::net::TcpListener;
use std::thread;

use kovanica_node::{net, Node};

/// A node with the standard genesis (mints 1000 to actor 1). All nodes in a test
/// share this genesis, since it is deterministic.
fn genesis_node() -> Node {
    let mut node = Node::new();
    node.genesis(3, 1000, 1000, 1).unwrap();
    node
}

#[test]
fn a_receiver_matches_the_producer_after_gossip() {
    let mut producer = genesis_node();
    let mut receiver = genesis_node();

    producer.send(1, 400, 2).unwrap();
    producer.send(1, 100, 3).unwrap(); // spends the 600 change

    let applied = net::gossip(&producer, &mut receiver).unwrap();
    assert_eq!(applied, 2);

    // The receiver now sees the producer's balances.
    assert_eq!(receiver.balance(&Node::address(2)).unwrap(), 400);
    assert_eq!(receiver.balance(&Node::address(3)).unwrap(), 100);
    assert_eq!(
        receiver.selected_tip().unwrap(),
        producer.selected_tip().unwrap()
    );
}

#[test]
fn independent_conflicting_spends_converge_to_one_winner() {
    // Two nodes each spend actor 1's genesis coinbase differently. After they
    // exchange blocks both hold both (parallel) blocks and resolve the conflict
    // identically — exactly one recipient is paid, and both nodes agree.
    let mut a = genesis_node();
    let mut b = genesis_node();

    a.send(1, 400, 2).unwrap(); // A: 1 -> 2
    b.send(1, 300, 3).unwrap(); // B: 1 -> 3 (spends the same output)

    // Exchange both ways.
    net::gossip(&a, &mut b).unwrap();
    net::gossip(&b, &mut a).unwrap();

    // Both nodes hold both parallel blocks now.
    assert_eq!(a.tips().unwrap().len(), 2);
    assert_eq!(b.tips().unwrap().len(), 2);

    let a2 = a.balance(&Node::address(2)).unwrap();
    let a3 = a.balance(&Node::address(3)).unwrap();
    let b2 = b.balance(&Node::address(2)).unwrap();
    let b3 = b.balance(&Node::address(3)).unwrap();
    assert_eq!((a2, a3), (b2, b3), "nodes disagree on the winner");
    assert!((a2 == 400 && a3 == 0) || (a2 == 0 && a3 == 300));
}

#[test]
fn tcp_pull_sync_converges_two_nodes() {
    // Server node has some blocks; a client pulls them over a real TCP socket.
    let mut server = genesis_node();
    server.send(1, 400, 2).unwrap();
    server.send(1, 100, 3).unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    // Serve from the exported records on another thread (a whole Node is not
    // `Send`, but its exported records are).
    let records = server.export();
    let handle = thread::spawn(move || {
        net::serve_records(&listener, &records).unwrap();
    });

    let mut client = genesis_node();
    let applied = net::pull_blocks(addr, &mut client).unwrap();
    handle.join().unwrap();

    assert_eq!(applied, 2);
    assert_eq!(client.balance(&Node::address(2)).unwrap(), 400);
    assert_eq!(client.balance(&Node::address(3)).unwrap(), 100);
}

#[test]
fn tcp_exchange_merges_divergent_chains() {
    use std::io::Write;
    use std::time::Duration;

    let mut server = genesis_node();
    server.send(1, 400, 2).unwrap();
    let mut client = genesis_node();
    client.send(1, 300, 3).unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let server_bytes = net::encode_records(&server.export());
    let client_bytes = net::encode_records(&client.export());

    // Seed side: serve our dump, then read the client's reply — exactly what
    // `serve_exchange` does on the explorer loop.
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream.write_all(&server_bytes).unwrap();
        stream.flush().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        net::read_records_from(&mut stream).unwrap()
    });

    let mut stream = std::net::TcpStream::connect(addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let from_server = net::read_records_from(&mut stream).unwrap();
    stream.write_all(&client_bytes).unwrap();
    stream.flush().unwrap();
    let from_client = handle.join().unwrap();

    for rec in from_server {
        client.receive_block(rec).unwrap();
    }
    for rec in from_client {
        server.receive_block(rec).unwrap();
    }

    assert_eq!(server.tips().unwrap().len(), 2);
    assert_eq!(client.tips().unwrap().len(), 2);
}
