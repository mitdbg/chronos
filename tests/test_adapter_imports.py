def test_langchain_adapter_imports() -> None:
    import langchain_janus

    assert hasattr(langchain_janus, "JanusContext")
    assert hasattr(langchain_janus, "JanusSQLite")


def test_langgraph_adapter_imports() -> None:
    import janus_langgraph
    from janus_langgraph.transactional import create_transaction_tools

    assert hasattr(janus_langgraph, "BranchAwareStore")
    assert callable(create_transaction_tools)

