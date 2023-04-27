def test_expiration_log_entry(cluster):
    cluster.create(3)

    cluster.execute("set", "x", 1)
    cluster.execute("set", "y", 2)
    cluster.execute("expire", "x", 1000)
    cluster.execute("expire", "y", 1000)

    # first test with a ttl that won't match
    cluster.execute("raft.debug", "expire", "x", "y", 1)
    cluster.wait_for_unanimity()

    for _, n in cluster.nodes.items():
        assert n.execute("raft.debug", "exec", "get", "x") == b'1'
        assert n.execute("raft.debug", "exec", "get", "y") == b'2'

    # second test with a ttl that will match
    cluster.execute("raft.debug", "expire", "x", "y", 0)
    cluster.wait_for_unanimity()

    for _, n in cluster.nodes.items():
        assert n.execute("raft.debug", "exec", "get", "x") is None
        assert n.execute("raft.debug", "exec", "get", "y") is None
