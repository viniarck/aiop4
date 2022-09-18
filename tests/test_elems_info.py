def test_index_all(elems_info) -> None:
    """Test ElementsP4Info all."""
    assert elems_info
    assert list(elems_info.tables.keys()) == [
        "IngressImpl.smac",
        "IngressImpl.dmac",
    ]
    assert list(elems_info.actions.keys()) == [
        "NoAction",
        "IngressImpl.drop",
        "IngressImpl.learn_mac",
        "IngressImpl.fwd",
        "IngressImpl.broadcast",
    ]
    assert list(elems_info.counters.keys()) == [
        "igPortsCounts",
        "egPortsCounts",
    ]
    assert list(elems_info.digests.keys()) == [
        "digest_t",
    ]
