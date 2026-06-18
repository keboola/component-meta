"""Unit tests for the Configuration model (CFTL-630 v1_compatibility flag)."""

from configuration import Configuration


def test_v1_compatibility_defaults_to_false():
    cfg = Configuration(**{"accounts": {}, "queries": []})
    assert cfg.v1_compatibility is False


def test_v1_compatibility_reads_true_from_parameters():
    cfg = Configuration(**{"accounts": {}, "queries": [], "v1_compatibility": True})
    assert cfg.v1_compatibility is True
