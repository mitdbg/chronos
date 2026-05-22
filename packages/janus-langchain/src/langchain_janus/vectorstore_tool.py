"""Janus VectorStore tool — transactional MVCC vector storage for RAG.

Wraps ``SqliteVecShim`` from Janus's transaction layer to provide
a LangChain-compatible vector store tool with full transactional support:
  - **MVCC isolation**: reads see a consistent snapshot
  - **Subtransactions**: savepoint/rollback support
  - **2PC coordination**: participates in the same commit protocol as FS/DB

Operations:
  - **register_collection** — define a vector collection (required first)
  - **add_texts** — add documents with embeddings
  - **add_documents** — add Document objects
  - **similarity_search** — find similar documents by query embedding
  - **delete** — delete a document by ID
  - **get** — retrieve a document by ID
  - **list_collections** — show registered collections

The tool is designed for long-term memory and RAG pipelines where agents
need consistent, rollback-safe vector storage.

Example::

    vec = JanusVectorStore(shim=vec_shim, txn=txn)
    vec.invoke({
        "command": "register_collection",
        "collection": "docs",
        "dimensions": 384,
    })
    vec.invoke({
        "command": "add_texts",
        "collection": "docs",
        "texts": ["Hello world", "Goodbye world"],
        "embeddings": [[0.1, 0.2, ...], [0.3, 0.4, ...]],
    })
    results = vec.invoke({
        "command": "similarity_search",
        "collection": "docs",
        "query_embedding": [0.1, 0.2, ...],
        "k": 5,
    })
"""

from __future__ import annotations

import json
import logging
from typing import Any

from langchain_core.callbacks import CallbackManagerForToolRun
from langchain_core.documents import Document
from langchain_core.tools import BaseTool
from pydantic import BaseModel, ConfigDict, Field

from janus_core.transaction.shim_vec import SqliteVecShim
from janus_core.transaction.types import TransactionHandle

logger = logging.getLogger(__name__)


class JanusVectorStoreInput(BaseModel):
    """Input schema for the Janus VectorStore tool."""

    command: str = Field(
        description=(
            "The vector operation to perform. One of: "
            "'register_collection' — Define a new vector collection. "
            "'add_texts' — Add text documents with embeddings. "
            "'add_documents' — Add Document objects with embeddings. "
            "'similarity_search' — Find similar documents by query embedding. "
            "'delete' — Delete a document by ID. "
            "'get' — Retrieve a document by ID. "
            "'list_collections' — List all registered collections."
        )
    )
    collection: str | None = Field(
        default=None,
        description="Collection name (required for most operations).",
    )
    dimensions: int | None = Field(
        default=None,
        description="Vector dimensions for 'register_collection'.",
    )
    texts: list[str] | None = Field(
        default=None,
        description="List of text strings for 'add_texts'.",
    )
    embeddings: list[list[float]] | None = Field(
        default=None,
        description="List of embedding vectors (float arrays) for 'add_texts' or 'add_documents'.",
    )
    metadatas: list[dict[str, Any]] | None = Field(
        default=None,
        description="List of metadata dicts for 'add_texts' (optional, parallel to texts).",
    )
    ids: list[str] | None = Field(
        default=None,
        description="Document IDs for 'add_texts' (optional; generated if not provided).",
    )
    documents: list[dict[str, Any]] | None = Field(
        default=None,
        description=(
            "List of Document-like dicts for 'add_documents'. "
            "Each should have 'page_content', optionally 'metadata' and 'id'."
        ),
    )
    query_embedding: list[float] | None = Field(
        default=None,
        description="Query embedding vector for 'similarity_search'.",
    )
    k: int | None = Field(
        default=4,
        description="Number of results for 'similarity_search' (default: 4).",
    )
    filters: dict[str, Any] | None = Field(
        default=None,
        description="Metadata filters for 'similarity_search' (optional).",
    )
    doc_id: str | None = Field(
        default=None,
        description="Document ID for 'get' or 'delete'.",
    )


class JanusVectorStore(BaseTool):
    """Transactional vector store with MVCC-based isolation.

    Provides vector storage for long-term memory and RAG pipelines,
    fully integrated with Janus's transactional system. All operations
    use MVCC visibility predicates, ensuring consistent reads across
    concurrent transactions and rollback-safe writes.

    Features:
      - **MVCC isolation**: See only committed data + your own writes
      - **Savepoint support**: Create checkpoints, rollback speculative work
      - **Coordinated commit**: 2PC with filesystem and database changes
      - **Flexible backends**: Uses sqlite-vec if available, falls back to
        brute-force cosine similarity otherwise

    Example::

        vec = JanusVectorStore(shim=vec_shim, txn=txn)
        
        # Register a collection
        vec.invoke({"command": "register_collection", "collection": "memories"})
        
        # Add documents
        vec.invoke({
            "command": "add_texts",
            "collection": "memories",
            "texts": ["Important fact 1", "Important fact 2"],
            "embeddings": [[0.1, 0.2, 0.3], [0.4, 0.5, 0.6]],
        })
        
        # Search
        vec.invoke({
            "command": "similarity_search",
            "collection": "memories",
            "query_embedding": [0.1, 0.2, 0.3],
            "k": 5,
        })
    """

    name: str = "janus_vectorstore"
    description: str = (
        "Transactional vector store for long-term memory and RAG. "
        "Supports register_collection, add_texts, add_documents, "
        "similarity_search, get, delete, list_collections. "
        "All operations are isolated within the current transaction."
    )
    args_schema: type[BaseModel] = JanusVectorStoreInput

    shim: SqliteVecShim
    """The underlying SqliteVecShim."""

    txn: TransactionHandle
    """The current transaction handle (for visibility predicate)."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def _run(
        self,
        command: str,
        collection: str | None = None,
        dimensions: int | None = None,
        texts: list[str] | None = None,
        embeddings: list[list[float]] | None = None,
        metadatas: list[dict[str, Any]] | None = None,
        ids: list[str] | None = None,
        documents: list[dict[str, Any]] | None = None,
        query_embedding: list[float] | None = None,
        k: int | None = 4,
        filters: dict[str, Any] | None = None,
        doc_id: str | None = None,
        run_manager: CallbackManagerForToolRun | None = None,
    ) -> str:
        """Execute a vector store operation."""
        try:
            if command == "register_collection":
                return self._handle_register_collection(collection, dimensions)
            elif command == "add_texts":
                return self._handle_add_texts(collection, texts, embeddings, metadatas, ids)
            elif command == "add_documents":
                return self._handle_add_documents(collection, documents, embeddings)
            elif command == "similarity_search":
                return self._handle_similarity_search(collection, query_embedding, k, filters)
            elif command == "delete":
                return self._handle_delete(collection, doc_id)
            elif command == "get":
                return self._handle_get(collection, doc_id)
            elif command == "list_collections":
                return self._handle_list_collections()
            else:
                return f"Error: Unknown command '{command}'."
        except Exception as e:
            logger.exception("VectorStore error")
            return f"Error: {e}"

    # ── Handlers ─────────────────────────────────────────────────────

    def _handle_register_collection(
        self,
        collection: str | None,
        dimensions: int | None,
    ) -> str:
        if not collection:
            return "Error: 'collection' is required for register_collection."
        
        # Update dimensions if specified (shim stores global default)
        if dimensions is not None:
            self.shim._dimensions = dimensions
        
        self.shim.register_collection(collection)
        dims = self.shim._dimensions
        return f"Collection '{collection}' registered with {dims} dimensions."

    def _handle_add_texts(
        self,
        collection: str | None,
        texts: list[str] | None,
        embeddings: list[list[float]] | None,
        metadatas: list[dict[str, Any]] | None,
        ids: list[str] | None,
    ) -> str:
        if not collection:
            return "Error: 'collection' is required."
        if not texts:
            return "Error: 'texts' list is required."
        if not embeddings:
            return "Error: 'embeddings' list is required."
        if len(texts) != len(embeddings):
            return f"Error: texts ({len(texts)}) and embeddings ({len(embeddings)}) must have same length."
        
        # Generate IDs if not provided
        if ids is None:
            import uuid
            ids = [str(uuid.uuid4()) for _ in texts]
        elif len(ids) != len(texts):
            return f"Error: ids ({len(ids)}) must match texts ({len(texts)})."
        
        # Metadatas default to empty
        if metadatas is None:
            metadatas = [{} for _ in texts]
        elif len(metadatas) != len(texts):
            return f"Error: metadatas ({len(metadatas)}) must match texts ({len(texts)})."
        
        added_ids: list[str] = []
        for doc_id, text, emb, meta in zip(ids, texts, embeddings, metadatas):
            payload = {"text": text, **meta}
            self.shim.upsert(self.txn, collection, doc_id, emb, payload)
            added_ids.append(doc_id)
        
        return json.dumps({
            "status": "success",
            "added_count": len(added_ids),
            "ids": added_ids,
        })

    def _handle_add_documents(
        self,
        collection: str | None,
        documents: list[dict[str, Any]] | None,
        embeddings: list[list[float]] | None,
    ) -> str:
        if not collection:
            return "Error: 'collection' is required."
        if not documents:
            return "Error: 'documents' list is required."
        if not embeddings:
            return "Error: 'embeddings' list is required."
        if len(documents) != len(embeddings):
            return f"Error: documents ({len(documents)}) and embeddings ({len(embeddings)}) must have same length."
        
        import uuid
        added_ids: list[str] = []
        
        for doc, emb in zip(documents, embeddings):
            doc_id = doc.get("id") or str(uuid.uuid4())
            page_content = doc.get("page_content", "")
            metadata = doc.get("metadata", {})
            
            payload = {"text": page_content, **metadata}
            self.shim.upsert(self.txn, collection, doc_id, emb, payload)
            added_ids.append(doc_id)
        
        return json.dumps({
            "status": "success",
            "added_count": len(added_ids),
            "ids": added_ids,
        })

    def _handle_similarity_search(
        self,
        collection: str | None,
        query_embedding: list[float] | None,
        k: int | None,
        filters: dict[str, Any] | None,
    ) -> str:
        if not collection:
            return "Error: 'collection' is required."
        if not query_embedding:
            return "Error: 'query_embedding' is required."
        
        k = k or 4
        results = self.shim.search(
            self.txn, collection, query_embedding, limit=k, filters=filters
        )
        
        # Format results
        formatted = []
        for r in results:
            doc = {
                "id": r.get("id"),
                "score": r.get("score", r.get("distance")),
            }
            if "payload" in r:
                payload = r["payload"]
                doc["text"] = payload.get("text", "")
                doc["metadata"] = {k: v for k, v in payload.items() if k != "text"}
            formatted.append(doc)
        
        return json.dumps({
            "status": "success",
            "results": formatted,
            "count": len(formatted),
        })

    def _handle_delete(
        self,
        collection: str | None,
        doc_id: str | None,
    ) -> str:
        if not collection:
            return "Error: 'collection' is required."
        if not doc_id:
            return "Error: 'doc_id' is required."
        
        deleted = self.shim.delete(self.txn, collection, doc_id)
        if deleted:
            return f"Document '{doc_id}' deleted from '{collection}'."
        else:
            return f"Document '{doc_id}' not found in '{collection}'."

    def _handle_get(
        self,
        collection: str | None,
        doc_id: str | None,
    ) -> str:
        if not collection:
            return "Error: 'collection' is required."
        if not doc_id:
            return "Error: 'doc_id' is required."
        
        result = self.shim.get(self.txn, collection, doc_id)
        if result is None:
            return f"Document '{doc_id}' not found in '{collection}'."
        
        # Format output
        doc: dict[str, Any] = {"id": result.get("id")}
        if "payload" in result:
            payload = result["payload"]
            doc["text"] = payload.get("text", "")
            doc["metadata"] = {k: v for k, v in payload.items() if k != "text"}
        
        return json.dumps({"status": "success", "document": doc})

    def _handle_list_collections(self) -> str:
        collections = list(self.shim._collections)
        return json.dumps({
            "collections": collections,
            "count": len(collections),
            "vec_available": self.shim.vec_available,
        })


# ── LangChain VectorStore interface (optional) ───────────────────────

class JanusTransactionalVectorStore:
    """LangChain VectorStore-compatible interface wrapping JanusVectorStore.

    This provides the standard VectorStore methods (add_texts, similarity_search)
    while backing them with transactional MVCC storage.

    Example::

        from langchain_janus.vectorstore_tool import JanusTransactionalVectorStore
        
        # Create with explicit shim+txn (from JanusContext)
        vs = JanusTransactionalVectorStore(shim=ctx.vec_shim, txn=ctx.txn, collection="docs")
        
        # Use like a normal VectorStore
        vs.add_texts(["Hello", "World"], embeddings=[[0.1, 0.2], [0.3, 0.4]])
        vs.similarity_search_by_vector([0.1, 0.2], k=5)
    """

    def __init__(
        self,
        shim: SqliteVecShim,
        txn: TransactionHandle,
        collection: str = "default",
        dimensions: int = 3,
    ):
        self._shim = shim
        self._txn = txn
        self._collection = collection
        self._dimensions = dimensions
        
        # Auto-register collection
        self._shim._dimensions = dimensions
        self._shim.register_collection(collection)

    @property
    def collection(self) -> str:
        return self._collection

    def add_texts(
        self,
        texts: list[str],
        embeddings: list[list[float]],
        metadatas: list[dict[str, Any]] | None = None,
        ids: list[str] | None = None,
    ) -> list[str]:
        """Add texts with embeddings to the vector store.

        Args:
            texts: The texts to add.
            embeddings: Corresponding embeddings (must match texts length).
            metadatas: Optional metadata dicts.
            ids: Optional IDs (generated if not provided).

        Returns:
            List of document IDs that were added.
        """
        import uuid
        
        if len(texts) != len(embeddings):
            raise ValueError(f"texts ({len(texts)}) and embeddings ({len(embeddings)}) must match")
        
        if ids is None:
            ids = [str(uuid.uuid4()) for _ in texts]
        if metadatas is None:
            metadatas = [{} for _ in texts]
        
        for doc_id, text, emb, meta in zip(ids, texts, embeddings, metadatas):
            payload = {"text": text, **meta}
            self._shim.upsert(self._txn, self._collection, doc_id, emb, payload)
        
        return ids

    def add_documents(
        self,
        documents: list[Document],
        embeddings: list[list[float]],
    ) -> list[str]:
        """Add Document objects with embeddings.

        Args:
            documents: LangChain Document objects.
            embeddings: Corresponding embeddings.

        Returns:
            List of document IDs.
        """
        import uuid
        
        if len(documents) != len(embeddings):
            raise ValueError("documents and embeddings must have same length")
        
        ids: list[str] = []
        for doc, emb in zip(documents, embeddings):
            doc_id = doc.id or str(uuid.uuid4())
            payload = {"text": doc.page_content, **doc.metadata}
            self._shim.upsert(self._txn, self._collection, doc_id, emb, payload)
            ids.append(doc_id)
        
        return ids

    def similarity_search_by_vector(
        self,
        embedding: list[float],
        k: int = 4,
        filters: dict[str, Any] | None = None,
    ) -> list[Document]:
        """Search for similar documents by vector.

        Args:
            embedding: Query embedding vector.
            k: Number of results to return.
            filters: Optional metadata filters.

        Returns:
            List of Document objects.
        """
        results = self._shim.search(
            self._txn, self._collection, embedding, limit=k, filters=filters
        )
        
        docs: list[Document] = []
        for r in results:
            doc_id = r.get("id")
            payload = r.get("payload", {})
            text = payload.get("text", "")
            metadata = {k: v for k, v in payload.items() if k != "text"}
            
            # Add score to metadata
            score = r.get("score", r.get("distance"))
            if score is not None:
                metadata["score"] = score
            
            docs.append(Document(id=doc_id, page_content=text, metadata=metadata))
        
        return docs

    def delete(self, ids: list[str]) -> bool:
        """Delete documents by ID.

        Args:
            ids: List of document IDs to delete.

        Returns:
            True if any deletions occurred.
        """
        deleted_any = False
        for doc_id in ids:
            if self._shim.delete(self._txn, self._collection, doc_id):
                deleted_any = True
        return deleted_any

    def get_by_ids(self, ids: list[str]) -> list[Document]:
        """Get documents by their IDs.

        Args:
            ids: List of document IDs.

        Returns:
            List of Document objects (may be shorter if some IDs not found).
        """
        docs: list[Document] = []
        for doc_id in ids:
            result = self._shim.get(self._txn, self._collection, doc_id)
            if result:
                payload = result.get("payload", {})
                text = payload.get("text", "")
                metadata = {k: v for k, v in payload.items() if k != "text"}
                docs.append(Document(id=doc_id, page_content=text, metadata=metadata))
        return docs
