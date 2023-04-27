def test_expiration_log_entry(cluster):
    cluster.create(3)

    cluster.execute("set", "x", 1)
    cluster.execute("set", "y", 2)
    cluster.execute("expire", "x", 1000)
    cluster.execute("expire", "y", 1000)

    cluster.execute("raft.debug", "expire", "x", "y", 1)

    cluster.wait_for_unanimity()

    for i in range(1,4):
        assert cluster.node(i).execute("raft.debug", "exec", "get", "x") == b'1'
        assert cluster.node(i).execute("raft.debug", "exec", "get", "y") == b'2'

    cluster.execute("raft.debug", "expire", "x", "y", 0)

    cluster.wait_for_unanimity()

    for i in range(1,4):
        assert cluster.node(i).execute("raft.debug", "exec", "get", "x") == None
        assert cluster.node(i).execute("raft.debug", "exec", "get", "y") == None
