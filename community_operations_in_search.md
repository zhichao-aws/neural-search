# Community Node Operations in GraphRAG Search

This document details all operations on **Community nodes** across the 4 GraphRAG search types.

---

## What are Communities?

**Communities** are hierarchical clusters of related entities discovered through graph clustering (Leiden algorithm). Each community has:
- **Structure**: Parent/child relationships forming a hierarchy
- **Members**: Entity IDs and relationship IDs
- **Summary**: LLM-generated community report describing the cluster
- **Metadata**: Level, rank, size, period

**Communities are graph structures** because they:
1. Group graph nodes (entities) based on connectivity
2. Form a hierarchical graph structure themselves
3. Link entities, relationships, and text units within the cluster

---

## Community Operations Summary

| Search Type | Community Index | Community Traversal | Community Usage |
|-------------|----------------|---------------------|-----------------|
| **Basic** | ❌ No | ❌ No | Not used |
| **Local** | ✅ Yes | ✅ Yes | Context enrichment via entity→community links |
| **Global** | ✅ Yes | ⚠️ Reads reports | Primary data source (community reports) |
| **DRIFT** | ✅ Yes | ✅ Yes | Primer (reports) + Follow-ups (traversal via Local) |

---

## 1. Basic Search - NO COMMUNITY OPERATIONS

**Community Usage**: None

Basic Search does not use communities at all. It operates purely on text chunks.

---

## 2. Local Search - COMMUNITY GRAPH TRAVERSAL ⭐

### Operation: Entity → Community Lookup

**Purpose**: Find all communities that contain the selected entities and rank them by relevance

**File**: `graphrag/query/structured_search/local_search/mixed_context.py:240-261`

**Core Code**:
```python
# Traverse from entities to their communities
community_matches = {}
for entity in selected_entities:
    if entity.community_ids:
        for community_id in entity.community_ids:
            community_matches[community_id] = (
                community_matches.get(community_id, 0) + 1
            )

# Sort communities by number of matched entities and rank
selected_communities = [
    self.community_reports[community_id]
    for community_id in community_matches
    if community_id in self.community_reports
]
selected_communities.sort(
    key=lambda x: (x.attributes["matches"], x.rank),
    reverse=True,
)
```

**Graph Operation Details**:
1. **Traversal**: `entity.community_ids` → list of community IDs
2. **Counting**: Track how many selected entities belong to each community
3. **Retrieval**: Fetch `CommunityReport` objects by community ID
4. **Ranking**: Sort by (# of matched entities, community rank)
5. **Context Building**: Add community reports to context up to token limit

**What Data is Retrieved**:
- Community report title
- Community report summary (or full_content)
- Community rank (importance)
- Community attributes (optional)

**Token Budget**: Controlled by `community_prop` parameter (default 25% of context)

---

## 3. Global Search - COMMUNITY REPORTS AS PRIMARY DATA ⭐

### Operation A: Dynamic Community Selection (Optional)

**Purpose**: Use LLM to select relevant communities based on query

**File**: `graphrag/query/structured_search/global_search/community_context.py` (dynamic selection logic)

**Core Code** (when enabled):
```python
# LLM rates each community for relevance to query
# Selects communities with rating above threshold
# Can include parent communities if children are selected
```

**Graph Operation**:
- Uses community hierarchy (parent/child relationships)
- Traverses community tree structure

**Note**: This is an optional feature, disabled by default

---

### Operation B: Community Report Batching

**Purpose**: Split community reports into batches for parallel processing

**File**: `graphrag/query/structured_search/global_search/search.py:153-173`

**Core Code**:
```python
# Build context from community reports
context_result = await self.context_builder.build_context(
    query=query,
    conversation_history=conversation_history,
    **self.context_builder_params,
)

# Map phase: parallel LLM calls on batches of community reports
map_responses = await asyncio.gather(*[
    self._map_response_single_batch(
        context_data=data,
        query=query,
        max_length=self.map_max_length,
        **self.map_llm_params,
    )
    for data in context_result.context_chunks
])
```

**Graph Operation**:
- Reads community reports (pre-computed graph summaries)
- Splits into batches
- Each batch contains multiple community reports

**What Data is Used**:
- Community report full_content or summary
- Community report title
- Community report rank

---

### Operation C: Community Report Aggregation

**Purpose**: Aggregate insights from multiple community reports in reduce phase

**File**: `graphrag/query/structured_search/global_search/search.py:296-370`

**Core Code**:
```python
# Collect all key points from map responses
key_points = []
for index, response in enumerate(map_responses):
    for element in response.response:
        key_points.append({
            "analyst": index,
            "answer": element["answer"],
            "score": element["score"],
        })

# Filter and rank by score
filtered_key_points = [
    point for point in key_points
    if point["score"] > 0
]
filtered_key_points = sorted(
    filtered_key_points,
    key=lambda x: x["score"],
    reverse=True,
)
```

**Graph Operation**:
- Synthesizes information across multiple community nodes
- Ranks insights by importance scores
- Aggregates dataset-level understanding from community-level summaries

---

## 4. DRIFT Search - HYBRID COMMUNITY OPERATIONS ⭐

DRIFT uses communities in **two distinct phases**:

### Phase 1: Primer - Community Report Vector Search

**Purpose**: Find relevant community reports to prime the search

**File**: `graphrag/query/structured_search/drift_search/drift_context.py:166-227`

**Core Code**:
```python
# Query expansion using HyDE
query_processor = PrimerQueryProcessor(...)
query_embedding, token_ct = await query_processor(query)

# Convert community reports to DataFrame with embeddings
report_df = self.convert_reports_to_df(self.reports)

# Vector similarity search on community report embeddings
query_norm = np.linalg.norm(query_embedding)
document_norms = np.linalg.norm(
    report_df["full_content_embedding"].to_list(), axis=1
)
dot_products = np.dot(
    np.vstack(report_df["full_content_embedding"].to_list()), query_embedding
)
report_df["similarity"] = dot_products / (document_norms * query_norm)

# Select top-K most similar community reports
top_k = report_df.nlargest(self.config.drift_k_followups, "similarity")
```

**Graph Operation**:
- Vector similarity search on community report embedding index
- Cosine similarity between query and community full_content embeddings
- Retrieves top-K most relevant community nodes

**What Data is Retrieved**:
- Community report short_id
- Community report community_id
- Community report full_content

---

### Phase 2: Primer - Community-Based Query Decomposition

**Purpose**: Use community reports to decompose query into follow-up questions

**File**: `graphrag/query/structured_search/drift_search/primer.py:122-151`

**Core Code**:
```python
def decompose_query(self, query: str, reports: pd.DataFrame):
    """Decompose the query into subqueries based on community reports."""
    community_reports = "\n\n".join(reports["full_content"].tolist())
    prompt = DRIFT_PRIMER_PROMPT.format(
        query=query,
        community_reports=community_reports
    )
    model_response = await self.chat_model.achat(prompt, json=True)
    # Returns: intermediate answers + follow-up questions
```

**Graph Operation**:
- Reads community report content
- Uses community-level summaries to understand query scope
- Generates follow-up questions based on community themes

---

### Phase 3: Follow-ups - Community Traversal (via Local Search)

**Purpose**: Each follow-up question uses Local Search, which includes community lookup

**File**: `graphrag/query/structured_search/drift_search/search.py:243-245`

**Core Code**:
```python
# Execute follow-up searches using LocalSearch
results = await self._search_step(
    global_query=query,
    search_engine=self.local_search,  # <-- Uses Local Search
    actions=actions
)
```

**Graph Operation**:
- Delegates to Local Search
- Performs entity→community traversal (same as Operation 2 in Local Search)
- Each iteration explores different entity neighborhoods and their communities

---

## Community Node Data Structure

### Community
```python
@dataclass
class Community(Named):
    id: str                              # UUID
    short_id: str | None                 # Human-readable ID
    title: str                           # Community name
    level: str                           # [GRAPH STRUCTURE] Hierarchy level
    parent: str                          # [GRAPH LINK] Parent community ID
    children: list[str]                  # [GRAPH LINK] Child community IDs
    entity_ids: list[str] | None         # [GRAPH LINK] Member entities
    relationship_ids: list[str] | None   # [GRAPH LINK] Member relationships
    text_unit_ids: list[str] | None      # [GRAPH LINK] Associated text chunks
    covariate_ids: dict | None           # [GRAPH LINK] Associated covariates
    attributes: dict | None              # Additional metadata
    size: int | None                     # Number of member entities
    period: str | None                   # ISO8601 timestamp for versioning
```

### CommunityReport
```python
@dataclass
class CommunityReport(Named):
    id: str                              # UUID
    short_id: str | None                 # Human-readable ID
    title: str                           # Report title
    community_id: str                    # [GRAPH LINK] Associated community
    summary: str                         # Short summary
    full_content: str                    # Full report text
    rank: float | None                   # Importance rank
    full_content_embedding: list[float]  # [VECTOR INDEX] For similarity search
    attributes: dict | None              # Additional metadata
    size: int | None                     # Community size
    period: str | None                   # Timestamp
```

---

## Community Graph Structure

```
Community Hierarchy (Graph):

    Level 0 (Root)
         |
    ┌────┴────┐
    │         │
Level 1 Communities
    │         │
    ├─────┬───┤
    │     │   │
Level 2 Communities
    │     │   │
  [Entities + Relationships]
```

**Key Graph Properties**:
1. **Hierarchical**: Communities form a tree structure via parent/child links
2. **Membership**: Each community contains sets of entity IDs and relationship IDs
3. **Clustering**: Created via Leiden algorithm on the entity-relationship graph
4. **Summarization**: Each community has an LLM-generated report

---

## Community Operations Comparison

### Vector Search on Communities:
| Search Type | Operation | Index Used |
|-------------|-----------|------------|
| Basic | ❌ | - |
| Local | ❌ | - |
| Global | ⚠️ Optional (dynamic selection) | Community embeddings |
| DRIFT | ✅ Yes (primer) | `community_report.full_content_embedding` |

### Community Traversal:
| Search Type | Operation | Traversal Pattern |
|-------------|-----------|-------------------|
| Basic | ❌ | - |
| Local | ✅ | Entity → Community (via `entity.community_ids`) |
| Global | ⚠️ | Reads reports directly (no traversal) |
| DRIFT | ✅ | Primer (reports) + Follow-ups (entity→community) |

### Community Report Usage:
| Search Type | Usage Pattern | Purpose |
|-------------|---------------|---------|
| Basic | ❌ Not used | - |
| Local | Context enrichment | Adds community-level context to entity answers |
| Global | Primary data source | Map-reduce over community summaries |
| DRIFT | Hybrid | Primer (guide exploration) + Follow-ups (context) |

---

## Conclusion

**Community operations are fundamental graph operations in GraphRAG:**

1. **Local Search**: Traverses entity→community links to enrich context with community-level summaries

2. **Global Search**: Operates primarily on community reports, which are graph-derived structures representing entity clusters

3. **DRIFT Search**: Uses communities in two ways:
   - Vector search on community embeddings to find relevant clusters
   - Entity→community traversal in follow-up searches

Communities bridge entity-level and dataset-level reasoning, enabling GraphRAG to provide context at multiple scales of the knowledge graph.
