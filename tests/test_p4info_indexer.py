def test_index_all(p4info_indexer) -> None:
    """Test P4InfoIndexer all."""
    assert p4info_indexer
    assert list(p4info_indexer.tables.keys()) == [
        "IngressImpl.smac",
        "IngressImpl.dmac",
    ]
    assert list(p4info_indexer.actions.keys()) == [
        "NoAction",
        "IngressImpl.drop",
        "IngressImpl.learn_mac",
        "IngressImpl.fwd",
        "IngressImpl.broadcast",
    ]
    assert list(p4info_indexer.counters.keys()) == [
        "igPortsCounts",
        "egPortsCounts",
    ]
    assert list(p4info_indexer.digests.keys()) == [
        "digest_t",
    ]
