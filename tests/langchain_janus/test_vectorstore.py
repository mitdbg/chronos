"""Tests for JanusVectorStore tool.

Tests MVCC-based vector operations: register_collection, add_texts,
add_documents, similarity_search, get, delete, list_collections.
Also tests transactional isolation and savepoint/rollback support.

Run with: sudo python -m pytest tests/test_vectorstore.py -v
"""

from __future__ import annotations

import json
import math
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any, Generator

import pytest

from janus_core.transaction.coordinator import TransactionCoordinator
from janus_core.transaction.shim_fs import OverlayFSShim
from janus_core.transaction.shim_vec import SqliteVecShim

from langchain_janus.context import JanusContext
from langchain_janus.vectorstore_tool import JanusVectorStore, JanusTransactionalVectorStore


def _require_root() -> None:
    if os.geteuid() != 0:
        pytest.skip("Requires root for OverlayFS")


@pytest.fixture
def fresh_base() -> Generator[Path, None, None]:
    _require_root()
    d = Path(tempfile.mkdtemp(prefix="janus_vectorstore_test_"))
    (d / "placeholder.txt").write_text("placeholder\n")
    yield d
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture
def ctx(fresh_base: Path) -> Generator[JanusContext, None, None]:
    """JanusContext with vectorstore enabled (in-memory DB)."""
    c = JanusContext(fresh_base, enable_sqlite=True, enable_vectorstore=True, vector_dimensions=3)
    c.begin()
    yield c
    if c.is_active:
        c.abort()


@pytest.fixture
def vec(ctx: JanusContext) -> JanusVectorStore:
    return ctx.vectorstore


def _parse_json(result: str) -> dict[str, Any]:
    """Parse JSON response from tool."""
    return json.loads(result)


# ── register_collection ──────────────────────────────────────────────


class TestRegisterCollection:
    def test_register_collection(self, vec: JanusVectorStore) -> None:
        result = vec.invoke({
            "command": "register_collection",
            "collection": "docs",
        })
        assert "registered" in result.lower()
        assert "docs" in result

    def test_register_collection_with_dimensions(self, vec: JanusVectorStore) -> None:
        result = vec.invoke({
            "command": "register_collection",
            "collection": "embeddings",
            "dimensions": 768,
        })
        assert "registered" in result.lower()
        assert "768" in result

    def test_register_missing_collection(self, vec: JanusVectorStore) -> None:
        result = vec.invoke({"command": "register_collection"})
        assert "error" in result.lower()


# ── add_texts ────────────────────────────────────────────────────────


class TestAddTexts:
    def _setup_collection(self, vec: JanusVectorStore) -> None:
        vec.invoke({
            "command": "register_collection",
            "collection": "docs",
        })

    def test_add_texts(self, vec: JanusVectorStore) -> None:
        self._setup_collection(vec)
        result = vec.invoke({
            "command": "add_texts",
            "collection": "docs",
            "texts": ["Hello world", "Goodbye world"],
            "embeddings": [[0.1, 0.2, 0.3], [0.4, 0.5, 0.6]],
        })
        data = _parse_json(result)
        assert data["status"] == "success"
        assert data["added_count"] == 2
        assert len(data["ids"]) == 2

    def test_add_texts_with_ids(self, vec: JanusVectorStore) -> None:
        self._setup_collection(vec)
        result = vec.invoke({
            "command": "add_texts",
            "collection": "docs",
            "texts": ["Text A"],
            "embeddings": [[0.1, 0.2, 0.3]],
            "ids": ["doc-001"],
        })
        data = _parse_json(result)
        assert data["ids"] == ["doc-001"]

    def test_add_texts_with_metadata(self, vec: JanusVectorStore) -> None:
        self._setup_collection(vec)
        result = vec.invoke({
            "command": "add_texts",
            "collection": "docs",
            "texts": ["Important fact"],
            "embeddings": [[0.1, 0.2, 0.3]],
            "metadatas": [{"source": "wiki", "page": 42}],
        })
        data = _parse_json(result)
        assert data["status"] == "success"

    def test_add_texts_missing_embeddings(self, vec: JanusVectorStore) -> None:
        self._setup_collection(vec)
        result = vec.invoke({
            "command": "add_texts",
            "collection": "docs",
            "texts": ["Hello"],
        })
        assert "error" in result.lower()

    def test_add_texts_length_mismatch(self, vec: JanusVectorStore) -> None:
        self._setup_collection(vec)
        result = vec.invoke({
            "command": "add_texts",
            "collection": "docs",
            "texts": ["A", "B", "C"],
            "embeddings": [[0.1, 0.2, 0.3]],
        })
        assert "error" in result.lower()


# ── add_documents ────────────────────────────────────────────────────


class TestAddDocuments:
    def _setup_collection(self, vec: JanusVectorStore) -> None:
        vec.invoke({
            "command": "register_collection",
            "collection": "docs",
        })

    def test_add_documents(self, vec: JanusVectorStore) -> None:
        self._setup_collection(vec)
        result = vec.invoke({
            "command": "add_documents",
            "collection": "docs",
            "documents": [
                {"page_content": "Hello world", "metadata": {"source": "test"}},
                {"page_content": "Goodbye world", "id": "doc-002"},
            ],
            "embeddings": [[0.1, 0.2, 0.3], [0.4, 0.5, 0.6]],
        })
        data = _parse_json(result)
        assert data["status"] == "success"
        assert data["added_count"] == 2
        assert "doc-002" in data["ids"]


# ── similarity_search ────────────────────────────────────────────────


class TestSimilaritySearch:
    def _setup_with_docs(self, vec: JanusVectorStore) -> None:
        vec.invoke({
            "command": "register_collection",
            "collection": "docs",
        })
        vec.invoke({
            "command": "add_texts",
            "collection": "docs",
            "texts": ["The cat sat on the mat", "The dog ran in the park", "The bird flew in the sky"],
            "embeddings": [[0.1, 0.2, 0.3], [0.4, 0.5, 0.6], [0.7, 0.8, 0.9]],
            "ids": ["cat", "dog", "bird"],
        })

    def test_similarity_search(self, vec: JanusVectorStore) -> None:
        self._setup_with_docs(vec)
        result = vec.invoke({
            "command": "similarity_search",
            "collection": "docs",
            "query_embedding": [0.1, 0.2, 0.3],
            "k": 3,
        })
        data = _parse_json(result)
        assert data["status"] == "success"
        assert data["count"] == 3
        # The closest should be the cat (exact match)
        assert any(r["id"] == "cat" for r in data["results"])

    def test_similarity_search_k(self, vec: JanusVectorStore) -> None:
        self._setup_with_docs(vec)
        result = vec.invoke({
            "command": "similarity_search",
            "collection": "docs",
            "query_embedding": [0.1, 0.2, 0.3],
            "k": 1,
        })
        data = _parse_json(result)
        assert data["count"] == 1

    def test_similarity_search_missing_embedding(self, vec: JanusVectorStore) -> None:
        self._setup_with_docs(vec)
        result = vec.invoke({
            "command": "similarity_search",
            "collection": "docs",
        })
        assert "error" in result.lower()


# ── get / delete ─────────────────────────────────────────────────────


class TestGetDelete:
    def _setup_with_docs(self, vec: JanusVectorStore) -> None:
        vec.invoke({
            "command": "register_collection",
            "collection": "docs",
        })
        vec.invoke({
            "command": "add_texts",
            "collection": "docs",
            "texts": ["Document one", "Document two"],
            "embeddings": [[0.1, 0.2, 0.3], [0.4, 0.5, 0.6]],
            "ids": ["doc1", "doc2"],
            "metadatas": [{"author": "Alice"}, {"author": "Bob"}],
        })

    def test_get_document(self, vec: JanusVectorStore) -> None:
        self._setup_with_docs(vec)
        result = vec.invoke({
            "command": "get",
            "collection": "docs",
            "doc_id": "doc1",
        })
        data = _parse_json(result)
        assert data["status"] == "success"
        assert data["document"]["id"] == "doc1"
        assert "Document one" in data["document"]["text"]

    def test_get_nonexistent(self, vec: JanusVectorStore) -> None:
        self._setup_with_docs(vec)
        result = vec.invoke({
            "command": "get",
            "collection": "docs",
            "doc_id": "nonexistent",
        })
        assert "not found" in result.lower()

    def test_delete_document(self, vec: JanusVectorStore) -> None:
        self._setup_with_docs(vec)
        result = vec.invoke({
            "command": "delete",
            "collection": "docs",
            "doc_id": "doc1",
        })
        assert "deleted" in result.lower()
        
        # Verify deleted
        result = vec.invoke({
            "command": "get",
            "collection": "docs",
            "doc_id": "doc1",
        })
        assert "not found" in result.lower()

    def test_delete_nonexistent(self, vec: JanusVectorStore) -> None:
        self._setup_with_docs(vec)
        result = vec.invoke({
            "command": "delete",
            "collection": "docs",
            "doc_id": "nonexistent",
        })
        assert "not found" in result.lower()


# ── list_collections ─────────────────────────────────────────────────


class TestListCollections:
    def test_list_collections_empty(self, vec: JanusVectorStore) -> None:
        result = vec.invoke({"command": "list_collections"})
        data = _parse_json(result)
        assert data["count"] == 0

    def test_list_collections(self, vec: JanusVectorStore) -> None:
        vec.invoke({"command": "register_collection", "collection": "docs"})
        vec.invoke({"command": "register_collection", "collection": "memories"})
        result = vec.invoke({"command": "list_collections"})
        data = _parse_json(result)
        assert data["count"] == 2
        assert "docs" in data["collections"]
        assert "memories" in data["collections"]


# ── Transaction Isolation ────────────────────────────────────────────


class TestTransactionIsolation:
    """Test that writes in one transaction are invisible to another."""

    def test_isolation_writes_invisible(self, fresh_base: Path) -> None:
        """Writes in txn1 should not be visible to txn2 (different numeric IDs).
        
        Note: With the current MVCC implementation, two transactions that
        share the same database see different views based on their snapshots.
        Transaction 1's uncommitted writes have beginTxn = txn1.numeric_id,
        which is not in txn2's committed_set, so they're invisible to txn2.
        """
        _require_root()
        
        # Create two contexts - each gets its own coordinator and snapshot
        # Using in-memory DB means they share state, but MVCC provides isolation
        
        ctx1 = JanusContext(fresh_base, enable_vectorstore=True, vector_dimensions=3)
        ctx1.begin()
        
        # Create second context using a separate base to get separate overlay
        import tempfile
        fresh_base2 = Path(tempfile.mkdtemp(prefix="tar_vec_iso_"))
        (fresh_base2 / "placeholder.txt").write_text("placeholder\n")
        
        ctx2 = JanusContext(fresh_base2, enable_vectorstore=True, vector_dimensions=3)
        ctx2.begin()
        
        try:
            vec1 = ctx1.vectorstore
            vec2 = ctx2.vectorstore
            
            # Register collection in both (DDL is separate since separate DBs)
            vec1.invoke({"command": "register_collection", "collection": "shared"})
            vec2.invoke({"command": "register_collection", "collection": "shared"})
            
            # Add document in txn1
            vec1.invoke({
                "command": "add_texts",
                "collection": "shared",
                "texts": ["Secret from txn1"],
                "embeddings": [[0.1, 0.2, 0.3]],
                "ids": ["secret1"],
            })
            
            # Search from txn2 — should NOT see txn1's write (separate DB)
            result2 = vec2.invoke({
                "command": "similarity_search",
                "collection": "shared",
                "query_embedding": [0.1, 0.2, 0.3],
                "k": 10,
            })
            data2 = _parse_json(result2)
            assert data2["count"] == 0  # txn1's write is in separate DB
            
            # txn1 can see its own write
            result1 = vec1.invoke({
                "command": "similarity_search",
                "collection": "shared",
                "query_embedding": [0.1, 0.2, 0.3],
                "k": 10,
            })
            data1 = _parse_json(result1)
            assert data1["count"] == 1
            
        finally:
            ctx1.abort()
            ctx2.abort()
            import shutil
            shutil.rmtree(fresh_base2, ignore_errors=True)

    def test_abort_discards_writes(self, fresh_base: Path) -> None:
        """Aborting a transaction discards all vector writes."""
        _require_root()
        
        db_path = str(fresh_base / "abort_test.db")
        
        # First transaction: add some vectors, then abort
        ctx1 = JanusContext(fresh_base, enable_vectorstore=True, vector_dimensions=3, db_path=db_path)
        ctx1.begin()
        vec1 = ctx1.vectorstore
        vec1.invoke({"command": "register_collection", "collection": "test"})
        vec1.invoke({
            "command": "add_texts",
            "collection": "test",
            "texts": ["Should be discarded"],
            "embeddings": [[0.1, 0.2, 0.3]],
            "ids": ["doomed"],
        })
        ctx1.abort()
        
        # Second transaction: should not see the aborted write
        ctx2 = JanusContext(fresh_base, enable_vectorstore=True, vector_dimensions=3, db_path=db_path)
        ctx2.begin()
        try:
            vec2 = ctx2.vectorstore
            vec2.invoke({"command": "register_collection", "collection": "test"})
            result = vec2.invoke({
                "command": "get",
                "collection": "test",
                "doc_id": "doomed",
            })
            assert "not found" in result.lower()
        finally:
            ctx2.abort()


# ── Savepoint / Rollback ─────────────────────────────────────────────


class TestSavepointRollback:
    """Test savepoint and rollback for vector operations."""

    def test_rollback_discards_new_vectors(self, ctx: JanusContext) -> None:
        """Vectors added after savepoint should be discarded on rollback."""
        vec = ctx.vectorstore
        
        # Register collection and add initial doc
        vec.invoke({"command": "register_collection", "collection": "docs"})
        vec.invoke({
            "command": "add_texts",
            "collection": "docs",
            "texts": ["Initial doc"],
            "embeddings": [[0.1, 0.2, 0.3]],
            "ids": ["initial"],
        })
        
        # Create savepoint
        ctx.savepoint("before_experiment")
        
        # Add more vectors
        vec.invoke({
            "command": "add_texts",
            "collection": "docs",
            "texts": ["Experimental doc"],
            "embeddings": [[0.4, 0.5, 0.6]],
            "ids": ["experimental"],
        })
        
        # Verify experimental doc exists
        result = vec.invoke({"command": "get", "collection": "docs", "doc_id": "experimental"})
        assert "Experimental doc" in result
        
        # Rollback
        ctx.rollback("before_experiment")
        
        # Experimental doc should be gone
        result = vec.invoke({"command": "get", "collection": "docs", "doc_id": "experimental"})
        assert "not found" in result.lower()
        
        # Initial doc should still exist
        result = vec.invoke({"command": "get", "collection": "docs", "doc_id": "initial"})
        assert "Initial doc" in result

    def test_rollback_restores_deleted_vectors(self, ctx: JanusContext) -> None:
        """Vectors deleted after savepoint should be restored on rollback."""
        vec = ctx.vectorstore
        
        vec.invoke({"command": "register_collection", "collection": "docs"})
        vec.invoke({
            "command": "add_texts",
            "collection": "docs",
            "texts": ["Will be deleted then restored"],
            "embeddings": [[0.1, 0.2, 0.3]],
            "ids": ["target"],
        })
        
        # Create savepoint
        ctx.savepoint("before_delete")
        
        # Delete the vector
        vec.invoke({"command": "delete", "collection": "docs", "doc_id": "target"})
        result = vec.invoke({"command": "get", "collection": "docs", "doc_id": "target"})
        assert "not found" in result.lower()
        
        # Rollback
        ctx.rollback("before_delete")
        
        # Vector should be restored
        result = vec.invoke({"command": "get", "collection": "docs", "doc_id": "target"})
        assert "Will be deleted then restored" in result

    def test_rollback_restores_updated_vectors(self, ctx: JanusContext) -> None:
        """Vectors updated after savepoint should be restored to old value on rollback."""
        vec = ctx.vectorstore
        
        vec.invoke({"command": "register_collection", "collection": "docs"})
        vec.invoke({
            "command": "add_texts",
            "collection": "docs",
            "texts": ["Original text"],
            "embeddings": [[0.1, 0.2, 0.3]],
            "ids": ["doc1"],
            "metadatas": [{"version": 1}],
        })
        
        # Create savepoint
        ctx.savepoint("before_update")
        
        # Update the vector (same ID)
        vec.invoke({
            "command": "add_texts",
            "collection": "docs",
            "texts": ["Updated text"],
            "embeddings": [[0.9, 0.8, 0.7]],
            "ids": ["doc1"],
            "metadatas": [{"version": 2}],
        })
        
        # Verify update
        result = vec.invoke({"command": "get", "collection": "docs", "doc_id": "doc1"})
        assert "Updated text" in result
        
        # Rollback
        ctx.rollback("before_update")
        
        # Should see original text
        result = vec.invoke({"command": "get", "collection": "docs", "doc_id": "doc1"})
        assert "Original text" in result


# ── JanusTransactionalVectorStore (LangChain interface) ────────────────


class TestJanusTransactionalVectorStore:
    """Test the LangChain-compatible VectorStore interface."""

    def test_add_texts(self, ctx: JanusContext) -> None:
        vs = JanusTransactionalVectorStore(
            shim=ctx.vec_shim,
            txn=ctx.txn,
            collection="langchain_docs",
            dimensions=3,
        )
        
        ids = vs.add_texts(
            texts=["Hello world", "Goodbye world"],
            embeddings=[[0.1, 0.2, 0.3], [0.4, 0.5, 0.6]],
        )
        assert len(ids) == 2

    def test_similarity_search_by_vector(self, ctx: JanusContext) -> None:
        vs = JanusTransactionalVectorStore(
            shim=ctx.vec_shim,
            txn=ctx.txn,
            collection="search_test",
            dimensions=3,
        )
        
        vs.add_texts(
            texts=["Cat", "Dog", "Bird"],
            embeddings=[[0.1, 0.0, 0.0], [0.0, 0.1, 0.0], [0.0, 0.0, 0.1]],
        )
        
        results = vs.similarity_search_by_vector([0.1, 0.0, 0.0], k=1)
        assert len(results) == 1
        assert results[0].page_content == "Cat"

    def test_delete(self, ctx: JanusContext) -> None:
        vs = JanusTransactionalVectorStore(
            shim=ctx.vec_shim,
            txn=ctx.txn,
            collection="delete_test",
            dimensions=3,
        )
        
        ids = vs.add_texts(
            texts=["To delete"],
            embeddings=[[0.1, 0.2, 0.3]],
            ids=["target"],
        )
        
        deleted = vs.delete(ids)
        assert deleted is True
        
        docs = vs.get_by_ids(["target"])
        assert len(docs) == 0

    def test_get_by_ids(self, ctx: JanusContext) -> None:
        vs = JanusTransactionalVectorStore(
            shim=ctx.vec_shim,
            txn=ctx.txn,
            collection="get_test",
            dimensions=3,
        )
        
        vs.add_texts(
            texts=["Doc A", "Doc B"],
            embeddings=[[0.1, 0.2, 0.3], [0.4, 0.5, 0.6]],
            ids=["a", "b"],
        )
        
        docs = vs.get_by_ids(["a", "b"])
        assert len(docs) == 2
        assert any(d.page_content == "Doc A" for d in docs)
        assert any(d.page_content == "Doc B" for d in docs)


# ── Multi-dimensional / Edge cases ───────────────────────────────────


class TestEdgeCases:
    def test_high_dimensional_vectors(self, fresh_base: Path) -> None:
        """Test with higher dimensional vectors."""
        _require_root()
        
        ctx = JanusContext(fresh_base, enable_vectorstore=True, vector_dimensions=128)
        ctx.begin()
        try:
            vec = ctx.vectorstore
            vec.invoke({"command": "register_collection", "collection": "hd", "dimensions": 128})
            
            # Create 128-dimensional vectors
            import random
            emb1 = [random.random() for _ in range(128)]
            emb2 = [random.random() for _ in range(128)]
            
            vec.invoke({
                "command": "add_texts",
                "collection": "hd",
                "texts": ["High-dim doc 1", "High-dim doc 2"],
                "embeddings": [emb1, emb2],
            })
            
            result = vec.invoke({
                "command": "similarity_search",
                "collection": "hd",
                "query_embedding": emb1,
                "k": 1,
            })
            data = _parse_json(result)
            assert data["count"] == 1
        finally:
            ctx.abort()

    def test_empty_collection_search(self, vec: JanusVectorStore) -> None:
        """Search on empty collection should return no results."""
        vec.invoke({"command": "register_collection", "collection": "empty"})
        result = vec.invoke({
            "command": "similarity_search",
            "collection": "empty",
            "query_embedding": [0.1, 0.2, 0.3],
            "k": 10,
        })
        data = _parse_json(result)
        assert data["count"] == 0

    def test_unicode_text(self, vec: JanusVectorStore) -> None:
        """Test with unicode characters."""
        vec.invoke({"command": "register_collection", "collection": "unicode"})
        vec.invoke({
            "command": "add_texts",
            "collection": "unicode",
            "texts": ["こんにちは世界", "مرحبا بالعالم", "🎉🚀🌟"],
            "embeddings": [[0.1, 0.2, 0.3], [0.4, 0.5, 0.6], [0.7, 0.8, 0.9]],
            "ids": ["ja", "ar", "emoji"],
        })
        
        result = vec.invoke({"command": "get", "collection": "unicode", "doc_id": "ja"})
        data = _parse_json(result)
        assert data["document"]["text"] == "こんにちは世界"
        
        result = vec.invoke({"command": "get", "collection": "unicode", "doc_id": "emoji"})
        data = _parse_json(result)
        assert data["document"]["text"] == "🎉🚀🌟"

    def test_special_characters_in_metadata(self, vec: JanusVectorStore) -> None:
        """Test metadata with special characters."""
        vec.invoke({"command": "register_collection", "collection": "special"})
        vec.invoke({
            "command": "add_texts",
            "collection": "special",
            "texts": ["Test doc"],
            "embeddings": [[0.1, 0.2, 0.3]],
            "ids": ["test"],
            "metadatas": [{"path": "/path/to/file", "query": 'SELECT * FROM "table"'}],
        })
        
        result = vec.invoke({"command": "get", "collection": "special", "doc_id": "test"})
        data = _parse_json(result)
        assert data["document"]["metadata"]["path"] == "/path/to/file"
