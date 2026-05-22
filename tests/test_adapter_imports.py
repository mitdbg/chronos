def test_langchain_adapter_imports() -> None:
    import langchain_chronos

    assert hasattr(langchain_chronos, "ChronosContext")
    assert hasattr(langchain_chronos, "ChronosSQLite")


def test_langgraph_adapter_imports() -> None:
    import chronos_langgraph
    from chronos_langgraph.transactional import create_transaction_tools

    assert hasattr(chronos_langgraph, "BranchAwareStore")
    assert callable(create_transaction_tools)

