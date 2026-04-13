"""Enterprise Memory Graph — Neo4j-backed memory with semantic search,
temporal decay, conflict resolution, and consolidation.

Replaces flat memory loading with:
1. Graph-based retrieval (exact concept/table/column matches)
2. Vector semantic search (meaning-based, for fuzzy matches)
3. Temporal decay (newer memories score higher)
4. Conflict resolution (contradicting memories flagged)
5. Consolidation (related facts merged into single definitions)
"""

from __future__ import annotations

import logging
import math
import uuid
from datetime import datetime

from app.services.schema_graph import get_graph_driver

logger = logging.getLogger(__name__)

_EMBEDDING_DIM = 384
_MAX_MEMORIES_PER_QUERY = 10
_DECAY_RATE = 0.003


_embedder = None


async def _get_embedding(text: str) -> list[float]:
    """Get embedding vector using local sentence-transformers (free, no API key needed)."""
    global _embedder
    if _embedder is None:
        import asyncio

        from sentence_transformers import SentenceTransformer

        _embedder = SentenceTransformer("all-MiniLM-L6-v2")
        logger.info("Loaded local embedding model: all-MiniLM-L6-v2 (384 dim)")

    try:
        import asyncio

        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, _embedder.encode, text)
        return result.tolist()
    except Exception as exc:
        logger.warning("Embedding failed: %s", exc)
        return [0.0] * _EMBEDDING_DIM


async def save_memory_to_graph(
    *,
    memory_id: str,
    org_id: str,
    user_id: str | None,
    memory_type: str,
    content: str,
    source: str,
    confidence: float,
    source_conversation_id: str | None = None,
    related_tables: list[str] | None = None,
    related_columns: list[dict] | None = None,
) -> bool:
    """Save a memory node to Neo4j with embedding + relationships."""
    driver = get_graph_driver()
    if not driver:
        return False

    try:
        embedding = await _get_embedding(content)

        async with driver.session() as session:
            await session.run(
                """
                CREATE (m:Memory {
                    id: $id, org_id: $org_id, user_id: $user_id,
                    memory_type: $mtype, content: $content,
                    source: $source, confidence: $confidence,
                    embedding: $embedding,
                    access_count: 0, is_active: true, is_consolidated: false,
                    created_at: datetime(), last_accessed: datetime(),
                    source_conversation_id: $conv_id
                })
            """,
                id=memory_id,
                org_id=org_id,
                user_id=user_id or "",
                mtype=memory_type,
                content=content,
                source=source,
                confidence=confidence,
                embedding=embedding,
                conv_id=source_conversation_id or "",
            )

            # Link to related tables (from schema graph)
            if related_tables:
                for table_name in related_tables:
                    await session.run(
                        """
                        MATCH (m:Memory {id: $mem_id})
                        MATCH (t:Table {name: $table, org_id: $org_id})
                        MERGE (m)-[:ABOUT]->(t)
                    """,
                        mem_id=memory_id,
                        table=table_name,
                        org_id=org_id,
                    )

            # Link to related columns
            if related_columns:
                for col in related_columns:
                    await session.run(
                        """
                        MATCH (m:Memory {id: $mem_id})
                        MATCH (t:Table {name: $table, org_id: $org_id})-[:HAS_COLUMN]->(c:Column {name: $col})
                        MERGE (m)-[:ABOUT]->(c)
                    """,
                        mem_id=memory_id,
                        table=col.get("table", ""),
                        col=col.get("column", ""),
                        org_id=org_id,
                    )

            # Check for conflicts with existing memories
            await _detect_conflicts(session, memory_id, org_id, memory_type, content)

        logger.info("Memory saved to graph: [%s] %s", memory_type, content[:60])
        return True

    except Exception as exc:
        logger.warning("Failed to save memory to graph: %s", exc)
        return False


async def _detect_conflicts(
    session, new_memory_id: str, org_id: str, memory_type: str, content: str
):
    """Find existing memories that might conflict with the new one."""
    try:
        keywords = [w.lower() for w in content.split() if len(w) > 3][:5]
        if not keywords:
            return

        result = await session.run(
            """
            MATCH (existing:Memory {org_id: $org_id, memory_type: $mtype, is_active: true})
            WHERE existing.id <> $new_id
              AND ANY(kw IN $keywords WHERE toLower(existing.content) CONTAINS kw)
            RETURN existing.id AS id, existing.content AS content
            LIMIT 5
        """,
            org_id=org_id,
            mtype=memory_type,
            new_id=new_memory_id,
            keywords=keywords,
        )

        records = [r.data() async for r in result]
        for record in records:
            await session.run(
                """
                MATCH (new:Memory {id: $new_id})
                MATCH (old:Memory {id: $old_id})
                MERGE (new)-[:POTENTIALLY_CONFLICTS {detected_at: datetime()}]->(old)
            """,
                new_id=new_memory_id,
                old_id=record["id"],
            )
            logger.info(
                "Potential conflict: new '%s' vs existing '%s'",
                content[:40],
                record["content"][:40],
            )

    except Exception as exc:
        logger.debug("Conflict detection failed: %s", exc)


# ---------------------------------------------------------------------------
# Hybrid retrieval — graph + vector
# ---------------------------------------------------------------------------


async def retrieve_memories(
    question: str,
    org_id: str,
    user_id: str | None = None,
    max_results: int = _MAX_MEMORIES_PER_QUERY,
) -> list[dict]:
    """Hybrid memory retrieval: graph traversal + vector similarity.

    Returns scored, deduplicated, conflict-resolved memories.
    """
    driver = get_graph_driver()
    if not driver:
        return []

    try:
        # Path 1: Keyword-based graph retrieval
        graph_memories = await _retrieve_by_graph(driver, question, org_id, user_id)

        # Path 2: Vector semantic search
        vector_memories = await _retrieve_by_vector(driver, question, org_id, user_id)

        # Merge, deduplicate, score with temporal decay
        all_memories = _merge_and_score(graph_memories, vector_memories)

        # Resolve conflicts — if two memories contradict, prefer newer/higher-confidence
        resolved = await _resolve_conflicts(driver, all_memories)

        return resolved[:max_results]

    except Exception as exc:
        logger.warning("Memory retrieval failed: %s", exc)
        return []


async def _retrieve_by_graph(driver, question: str, org_id: str, user_id: str | None) -> list[dict]:
    """Graph-based retrieval — keyword matching + relationship traversal."""
    keywords = [w.lower() for w in question.split() if len(w) > 3][:10]
    if not keywords:
        return []

    async with driver.session() as session:
        result = await session.run(
            """
            MATCH (m:Memory {org_id: $org_id, is_active: true})
            WHERE (m.user_id = '' OR m.user_id = $user_id)
              AND ANY(kw IN $keywords WHERE toLower(m.content) CONTAINS kw)
            RETURN m.id AS id, m.content AS content, m.memory_type AS type,
                   m.confidence AS confidence, m.access_count AS access_count,
                   m.created_at AS created_at, m.last_accessed AS last_accessed,
                   m.user_id AS user_id, m.is_consolidated AS consolidated,
                   'graph' AS source
            ORDER BY m.confidence DESC
            LIMIT 15
        """,
            org_id=org_id,
            user_id=user_id or "",
            keywords=keywords,
        )

        return [r.data() async for r in result]


async def _retrieve_by_vector(
    driver, question: str, org_id: str, user_id: str | None
) -> list[dict]:
    """Vector semantic search using Neo4j vector index."""
    embedding = await _get_embedding(question)

    async with driver.session() as session:
        try:
            result = await session.run(
                """
                CALL db.index.vector.queryNodes('memory_embedding_index', 10, $embedding)
                YIELD node, score
                WHERE node.org_id = $org_id AND node.is_active = true
                  AND (node.user_id = '' OR node.user_id = $user_id)
                RETURN node.id AS id, node.content AS content, node.memory_type AS type,
                       node.confidence AS confidence, node.access_count AS access_count,
                       node.created_at AS created_at, node.last_accessed AS last_accessed,
                       node.user_id AS user_id, node.is_consolidated AS consolidated,
                       score AS vector_score, 'vector' AS source
                ORDER BY score DESC
                LIMIT 10
            """,
                embedding=embedding,
                org_id=org_id,
                user_id=user_id or "",
            )

            return [r.data() async for r in result]

        except Exception:
            # Vector index might not exist yet — fall back to brute force
            logger.debug("Vector index not available, skipping semantic search")
            return []


def _merge_and_score(graph_results: list[dict], vector_results: list[dict]) -> list[dict]:
    """Merge results from both retrieval paths, deduplicate, apply temporal decay."""
    seen_ids = set()
    merged = []

    for mem in graph_results + vector_results:
        mem_id = mem.get("id")
        if mem_id in seen_ids:
            continue
        seen_ids.add(mem_id)

        confidence = mem.get("confidence", 0.5)
        vector_score = mem.get("vector_score", 0.0)
        access_count = mem.get("access_count", 0)

        created = mem.get("created_at")
        days_old = 0
        if created:
            try:
                if hasattr(created, "to_native"):
                    created = created.to_native()
                if isinstance(created, datetime):
                    days_old = (datetime.utcnow() - created.replace(tzinfo=None)).days
            except Exception:
                days_old = 0

        recency_weight = math.exp(-_DECAY_RATE * days_old)
        frequency_boost = min(1.0 + (access_count * 0.05), 1.5)

        base_score = max(confidence, vector_score * 0.8)
        final_score = base_score * recency_weight * frequency_boost

        if mem.get("consolidated"):
            final_score *= 1.2

        mem["final_score"] = round(final_score, 4)
        mem["days_old"] = days_old
        merged.append(mem)

    merged.sort(key=lambda x: x.get("final_score", 0), reverse=True)
    return merged


async def _resolve_conflicts(driver, memories: list[dict]) -> list[dict]:
    """If two memories conflict, keep the one with higher score."""
    if len(memories) < 2:
        return memories

    try:
        mem_ids = [m["id"] for m in memories if m.get("id")]
        async with driver.session() as session:
            result = await session.run(
                """
                MATCH (a:Memory)-[:POTENTIALLY_CONFLICTS]-(b:Memory)
                WHERE a.id IN $ids AND b.id IN $ids
                RETURN a.id AS id_a, b.id AS id_b
            """,
                ids=mem_ids,
            )

            conflicts = [r.data() async for r in result]

        if not conflicts:
            return memories

        # Build conflict pairs
        to_remove = set()
        score_map = {m["id"]: m.get("final_score", 0) for m in memories}
        for conflict in conflicts:
            id_a, id_b = conflict["id_a"], conflict["id_b"]
            # Remove the lower-scoring memory
            if score_map.get(id_a, 0) >= score_map.get(id_b, 0):
                to_remove.add(id_b)
            else:
                to_remove.add(id_a)

        resolved = [m for m in memories if m["id"] not in to_remove]
        if to_remove:
            logger.info("Conflict resolution: removed %d lower-scoring memories", len(to_remove))
        return resolved

    except Exception as exc:
        logger.debug("Conflict resolution failed: %s", exc)
        return memories


# ---------------------------------------------------------------------------
# Memory formatting for prompt injection
# ---------------------------------------------------------------------------


def format_memories_for_prompt(memories: list[dict]) -> str:
    """Format retrieved memories for LLM prompt injection."""
    if not memories:
        return ""

    lines = [
        "",
        "AGENT MEMORY (use these facts silently — do NOT mention them to the user):",
        "=" * 60,
    ]

    org_memories = [m for m in memories if not m.get("user_id")]
    user_memories = [m for m in memories if m.get("user_id")]

    if org_memories:
        lines.append("\n[ORGANIZATION KNOWLEDGE]")
        for m in org_memories:
            tag = m.get("type", "fact").upper().replace("_", " ")
            score = m.get("final_score", 0)
            lines.append(f"  [{tag}] {m['content']}  (relevance: {score:.2f})")

    if user_memories:
        lines.append("\n[YOUR PREFERENCES]")
        for m in user_memories:
            tag = m.get("type", "fact").upper().replace("_", " ")
            lines.append(f"  [{tag}] {m['content']}")

    lines.append("")
    return "\n".join(lines)


async def update_memory_access(memory_ids: list[str]):
    """Update access_count and last_accessed for used memories."""
    driver = get_graph_driver()
    if not driver or not memory_ids:
        return

    try:
        async with driver.session() as session:
            await session.run(
                """
                MATCH (m:Memory)
                WHERE m.id IN $ids
                SET m.access_count = m.access_count + 1,
                    m.last_accessed = datetime()
            """,
                ids=memory_ids,
            )
    except Exception as exc:
        logger.debug("Memory access update failed: %s", exc)


# ---------------------------------------------------------------------------
# Vector index setup (run once)
# ---------------------------------------------------------------------------


async def ensure_vector_index():
    """Create the vector index in Neo4j if it doesn't exist."""
    driver = get_graph_driver()
    if not driver:
        return

    try:
        async with driver.session() as session:
            await session.run("""
                CREATE VECTOR INDEX memory_embedding_index IF NOT EXISTS
                FOR (m:Memory)
                ON (m.embedding)
                OPTIONS {indexConfig: {
                    `vector.dimensions`: 384,
                    `vector.similarity_function`: 'cosine'
                }}
            """)
            logger.info("Neo4j vector index ensured")
    except Exception as exc:
        logger.warning("Vector index creation failed (may already exist): %s", exc)


# ---------------------------------------------------------------------------
# Consolidation (merge related memories)
# ---------------------------------------------------------------------------


async def consolidate_memories(org_id: str, llm=None) -> int:
    """Merge related memories into consolidated facts. Run weekly."""
    driver = get_graph_driver()
    if not driver or not llm:
        return 0

    try:
        async with driver.session() as session:
            result = await session.run(
                """
                MATCH (m1:Memory {org_id: $org_id, is_active: true, is_consolidated: false})
                MATCH (m2:Memory {org_id: $org_id, is_active: true, is_consolidated: false})
                WHERE m1.id < m2.id AND m1.memory_type = m2.memory_type
                  AND ANY(kw IN split(toLower(m1.content), ' ')
                      WHERE size(kw) > 4 AND toLower(m2.content) CONTAINS kw)
                RETURN m1.id AS id1, m1.content AS content1,
                       m2.id AS id2, m2.content AS content2,
                       m1.memory_type AS type
                LIMIT 20
            """,
                org_id=org_id,
            )

            groups = [r.data() async for r in result]

        if not groups:
            return 0

        # Use LLM to merge related facts
        consolidated = 0
        from langchain_core.messages import HumanMessage, SystemMessage

        for group in groups:
            merge_prompt = (
                f"Merge these two related facts into ONE concise statement:\n"
                f"Fact 1: {group['content1']}\n"
                f"Fact 2: {group['content2']}\n"
                f"Return ONLY the merged fact, nothing else."
            )
            response = await llm.ainvoke(
                [
                    SystemMessage(
                        content="You merge related facts into single concise statements."
                    ),
                    HumanMessage(content=merge_prompt),
                ]
            )
            merged_content = response.content.strip()

            # Create consolidated memory
            new_id = str(uuid.uuid4())
            embedding = await _get_embedding(merged_content)

            async with driver.session() as session:
                await session.run(
                    """
                    CREATE (m:Memory {
                        id: $id, org_id: $org_id, user_id: '',
                        memory_type: $type, content: $content,
                        source: 'consolidated', confidence: 0.95,
                        embedding: $embedding,
                        access_count: 0, is_active: true, is_consolidated: true,
                        created_at: datetime(), last_accessed: datetime()
                    })
                """,
                    id=new_id,
                    org_id=org_id,
                    type=group["type"],
                    content=merged_content,
                    embedding=embedding,
                )

                # Link consolidation
                await session.run(
                    """
                    MATCH (new:Memory {id: $new_id})
                    MATCH (old1:Memory {id: $id1})
                    MATCH (old2:Memory {id: $id2})
                    CREATE (new)-[:CONSOLIDATES]->(old1)
                    CREATE (new)-[:CONSOLIDATES]->(old2)
                    SET old1.is_active = false, old2.is_active = false
                """,
                    new_id=new_id,
                    id1=group["id1"],
                    id2=group["id2"],
                )

            consolidated += 1

        logger.info("Consolidated %d memory groups for org %s", consolidated, org_id)
        return consolidated

    except Exception as exc:
        logger.warning("Memory consolidation failed: %s", exc)
        return 0
