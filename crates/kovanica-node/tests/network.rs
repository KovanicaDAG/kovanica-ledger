//! Integration tests for multi-node block dissemination: nodes exchange blocks
//! and converge on the identical DAG and UTXO state — in-process and over TCP.

use std::net::TcpListener;
use std::thread;

use kovanica_node::{net, p2p::Mesh, Node};

/// A node with the standard genesis (mints 1000 to actor 1). All nodes in a test
/// share this genesis, since it is deterministic.
fn genesis_node() -> Node {
    let mut node = Node::new();
    node.genesis(3, 1000, 1000, 1).unwrap();
    node
}

/// Build a node with the standard genesis plus some extra blocks.
fn genesis_with_blocks(sends: &[(u64, u64, u64)]) -> Node {
    let mut node = genesis_node();
    for (from, amount, to) in sends {
        node.send(*from, *amount, *to).unwrap();
    }
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

#[test]
fn mesh_sync_headers_first_converges_two_nodes() {
    // In-process: use Mesh to sync headers-first between two nodes.
    let mut mesh = Mesh::new();
    mesh.add("server", genesis_with_blocks(&[(1, 400, 2), (1, 100, 3)]));
    mesh.add("client", genesis_node());

    // Sync from server to client
    let applied = mesh.sync_headers_first("server", "client").unwrap();
    assert!(applied > 0, "at least one block should be applied");

    let client_node = mesh.node("client").unwrap();
    assert_eq!(client_node.balance(&Node::address(2)).unwrap(), 400);
    assert_eq!(client_node.balance(&Node::address(3)).unwrap(), 100);
    assert_eq!(
        client_node.selected_tip().unwrap(),
        mesh.node("server").unwrap().selected_tip().unwrap()
    );
}

#[test]
fn mesh_sync_headers_first_bidirectional_merges_divergent_chains() {
    // In-process: two nodes with diverging chains sync bidirectionally and converge.
    let mut mesh = Mesh::new();
    mesh.add("server", genesis_with_blocks(&[(1, 400, 2)]));
    mesh.add("client", genesis_with_blocks(&[(1, 300, 3)]));

    // First sync: server -> client
    let applied1 = mesh.sync_headers_first("server", "client").unwrap();
    assert!(applied1 > 0);
    let client_tips = mesh.node("client").unwrap().tips().unwrap().len();
    assert_eq!(client_tips, 2); // client keeps its own block and gained server's

    // Second sync: client -> server (client now has both blocks)
    let applied2 = mesh.sync_headers_first("client", "server").unwrap();
    assert!(applied2 > 0);

    let server_tips = mesh.node("server").unwrap().tips().unwrap().len();
    let client_tips = mesh.node("client").unwrap().tips().unwrap().len();
    assert_eq!(server_tips, 2);
    assert_eq!(client_tips, 2);

    // Both agree on final state
    let s2 = mesh
        .node("server")
        .unwrap()
        .balance(&Node::address(2))
        .unwrap();
    let s3 = mesh
        .node("server")
        .unwrap()
        .balance(&Node::address(3))
        .unwrap();
    let c2 = mesh
        .node("client")
        .unwrap()
        .balance(&Node::address(2))
        .unwrap();
    let c3 = mesh
        .node("client")
        .unwrap()
        .balance(&Node::address(3))
        .unwrap();
    assert_eq!(
        (s2, s3),
        (c2, c3),
        "nodes disagree after bidirectional sync"
    );
    assert!((s2 == 400 && s3 == 0) || (s2 == 0 && s3 == 300));
}
