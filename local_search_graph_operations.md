# GraphRAG Search Types and Graph Operations

This document explains all search types in GraphRAG and details the graph operations they perform.

---

## Table of Contents
- [All Search Types Overview](#all-search-types-overview)
- [Graph Operations by Search Type](#graph-operations-by-search-type)
- [Detailed Graph Operations (Local Search)](#detailed-graph-operations-local-search)
- [Implementation Reference](#implementation-reference)

---

## All Search Types Overview

GraphRAG supports **4 search types**:

| Search Type | Uses Graph | Description |
|-------------|------------|-------------|
| **Basic** | ❌ No | Standard vector RAG on text chunks |
| **Local** | ✅ Yes | Entity-centric graph traversal |
| **Global** | ⚠️ Indirect | Uses pre-computed community summaries derived from graph |
| **DRIFT** | ✅ Yes | Iterative exploration using Local Search |

---

## Graph Operations by Search Type

### 1. Basic Search - NO GRAPH OPERATIONS

**Workflow**:
```
Query → Embed query → Vector search on text chunks → Retrieve top-K chunks → LLM generates response
```

**Operations**:
- ✅ Vector similarity search on text embeddings
- ✅ LLM generation
- ❌ No entity mapping
- ❌ No graph traversal
- ❌ No relationship retrieval

**When to use**: Simple questions answerable from individual text chunks

---

### 2. Local Search - FULL GRAPH OPERATIONS ⭐

**Workflow**:
```
Query
  ↓
[1] Entity Mapping: Query → Entities (vector search on entity embeddings)
  ↓
[2] Graph Traversal (1-hop from selected entities):
  ├─→ entity.community_ids → Communities
  ├─→ relationship.source/target → Relationships (edges)
  ├─→ entity.text_unit_ids → Text Units (source text)
  └─→ covariate.subject_id → Covariates (metadata)
  ↓
Combine all context → LLM generates response
```

**Graph Operations**: See [detailed breakdown below](#detailed-graph-operations-local-search)

**When to use**: Questions about specific entities, relationships, or entity-centric analysis

---

### 3. Global Search - NO RUNTIME GRAPH OPERATIONS

**Workflow**:
```
Query
  ↓
[Map Phase]
  ├─→ Split community reports into batches
  ├─→ Parallel LLM calls on each batch
  └─→ Each LLM generates key points with importance scores
  ↓
[Reduce Phase]
  ├─→ Aggregate and rank key points by score
  └─→ LLM generates final comprehensive response
```

**Operations**:
- ✅ Uses community reports (pre-computed from graph clustering)
- ✅ Map-reduce pattern with parallel LLM calls
- ✅ Importance scoring and ranking
- ❌ No runtime entity mapping
- ❌ No runtime graph traversal

**Note**: Communities are created during indexing via Leiden graph clustering algorithm, but at query time Global Search only reads the pre-generated summaries.

**When to use**: Dataset-level questions, themes, high-level overviews

---

### 4. DRIFT Search - FULL GRAPH OPERATIONS (via Local Search) ⭐

**Workflow**:
```
Query
  ↓
[Primer Phase - NO GRAPH OPS]
  ├─→ Query expansion (HyDE: generate hypothetical answer)
  ├─→ Vector search on community report embeddings
  └─→ LLM query decomposition → Generate follow-up questions
  ↓
[Iterative Follow-up Phase - USES LOCAL SEARCH]
  For N iterations:
    ├─→ Select top-K incomplete follow-up questions
    ├─→ Run LocalSearch for each follow-up
    │     └─→ [All 5 graph operations from Local Search]
    ├─→ Each LocalSearch produces:
    │     ├─→ Intermediate answer
    │     ├─→ Confidence score
    │     └─→ New follow-up questions
    └─→ Build hierarchical Q&A tree
  ↓
[Reduce Phase - NO GRAPH OPS]
  └─→ Aggregate all intermediate answers → LLM generates final response
```

**Graph Operations**: Same as Local Search, but executed multiple times (N × 5 operations)

**When to use**: Complex research questions requiring depth, multi-faceted queries

---

## Detailed Graph Operations (Local Search)

Local Search performs **5 graph operations** in a 1-hop traversal pattern.

### Operation 1: Entity Mapping (Query → Entities)

**Purpose**: Map query to relevant entities using semantic similarity

**File**: `graphrag/query/context_builder/entity_extraction.py:58-62`

**Code**:
```python
search_results = text_embedding_vectorstore.similarity_search_by_text(
    text=query,
    text_embedder=lambda t: text_embedder.embed(t),
    k=k * oversample_scaler,
)
```

**Operation Type**: Vector similarity search on entity embedding index

---

### Operation 2: Community Lookup (Entities → Communities)

**Purpose**: Find communities containing the selected entities

**File**: `graphrag/query/structured_search/local_search/mixed_context.py:240-246`

**Code**:
```python
for entity in selected_entities:
    if entity.community_ids:
        for community_id in entity.community_ids:
            community_matches[community_id] = community_matches.get(community_id, 0) + 1
```

**Operation Type**: Graph traversal via `entity.community_ids`

---

### Operation 3: Relationship Retrieval (Entities → Relationships)

**Purpose**: Find all relationships (graph edges) connected to selected entities

**File**: `graphrag/query/input/retrieval/relationships.py:21-26`

**Code**:
```python
selected_entity_names = [entity.title for entity in selected_entities]
selected_relationships = [
    relationship for relationship in relationships
    if relationship.source in selected_entity_names
    and relationship.target in selected_entity_names
]
```

**Operation Type**: Edge filtering by source/target node matching

**Note**: Also retrieves out-network relationships (edges to entities not in selected set)

---

### Operation 4: Text Unit Retrieval (Entities → Text Units)

**Purpose**: Get source text chunks that mention the selected entities

**File**: `graphrag/query/structured_search/local_search/mixed_context.py:331-333`

**Code**:
```python
for text_id in entity.text_unit_ids or []:
    if text_id not in text_unit_ids_set and text_id in self.text_units:
        selected_unit = deepcopy(self.text_units[text_id])
```

**Operation Type**: Reference traversal via `entity.text_unit_ids`

---

### Operation 5: Covariate Retrieval (Entities → Covariates)

**Purpose**: Get metadata/claims/attributes associated with entities

**What are Covariates**: Entity-linked metadata such as:
- Claims about entities (assertions, facts)
- Temporal attributes (start_date, end_date)
- Status information
- Custom entity attributes

**File**: `graphrag/query/context_builder/local_context.py:123-125`

**Code**:
```python
for entity in selected_entities:
    selected_covariates.extend([
        cov for cov in covariates if cov.subject_id == entity.title
    ])
```

**Called from**: `graphrag/query/structured_search/local_search/mixed_context.py:435-446`
```python
# Build covariate context for each covariate type
for covariate in self.covariates:
    covariate_context, covariate_context_data = build_covariates_context(
        selected_entities=added_entities,
        covariates=self.covariates[covariate],
        ...
    )
```

**Operation Type**: Graph attribute lookup via `covariate.subject_id → entity.title`

**Why this is a Graph Operation**:
- Covariates are **entity attributes stored as separate nodes** in the knowledge graph
- Links covariates to entities via `covariate.subject_id` → `entity.title` relationship
- Enables retrieval of entity-specific metadata that complements structural graph information
- Part of the entity's graph neighborhood (1-hop from entity node)

**Used By**:
- ✅ **Local Search**: Actively retrieves covariates for selected entities
- ✅ **DRIFT Search**: Via Local Search in follow-up phase
- ❌ **Basic Search**: Not used
- ❌ **Global Search**: Not used (operates at community level)

---

## Graph Operations Summary Table

| # | Operation | Graph Access Pattern | Basic | Local | Global | DRIFT |
|---|-----------|---------------------|-------|-------|--------|-------|
| 1 | **Entity Mapping** | Vector search on entity embeddings | ❌ | ✅ | ❌ | ✅ |
| 2 | **Community Lookup** | `entity.community_ids` traversal | ❌ | ✅ | ⚠️ Static | ✅ |
| 3 | **Relationship Retrieval** | Edge filtering by source/target | ❌ | ✅ | ❌ | ✅ |
| 4 | **Text Unit Retrieval** | `entity.text_unit_ids` traversal | ⚠️ Direct vector search | ✅ Via entities | ❌ | ✅ |
| 5 | **Covariate Retrieval** | Attribute matching | ❌ | ✅ | ❌ | ✅ |
| | **Graph Depth** | | 0-hop | 1-hop | 0-hop | Multi-hop |

**Legend**:
- ✅ = Performs this graph operation
- ❌ = Does not perform this graph operation
- ⚠️ = Indirect or different approach

---

## Graph Structure Visualization

### Knowledge Graph Structure:
```
┌─────────────────────────────────────────────────────────────┐
│                     Knowledge Graph                          │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  Entities (Nodes)                                            │
│    ├─ id, title, description                                 │
│    ├─ description_embedding (for vector search)              │
│    ├─ community_ids → [LINK to Communities]                  │
│    └─ text_unit_ids → [LINK to TextUnits]                    │
│                                                              │
│  Relationships (Edges)                                        │
│    ├─ source → [EDGE to Entity]                              │
│    ├─ target → [EDGE to Entity]                              │
│    └─ weight, description                                     │
│                                                              │
│  Communities (Hierarchical Clusters)                          │
│    ├─ entity_ids → [LINK to Entities]                        │
│    ├─ relationship_ids → [LINK to Relationships]             │
│    └─ Community Reports (LLM-generated summaries)            │
│                                                              │
│  TextUnits (Source Text Chunks)                              │
│    ├─ text content                                           │
│    ├─ entity_ids → [LINK to Entities]                        │
│    └─ relationship_ids → [LINK to Relationships]             │
│                                                              │
│  Covariates (Entity Metadata/Claims)                         │
│    └─ subject_id → [LINK to Entity]                          │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

### Local Search Traversal Pattern:
```
      Query
        ↓
   [Vector Search]
        ↓
    Entities (top-K nodes)
        │
        ├──→ entity.community_ids ──→ Communities
        │
        ├──→ Filter relationships where:
        │    source/target = entity ──→ Relationships (edges)
        │
        ├──→ entity.text_unit_ids ──→ TextUnits
        │
        └──→ covariate.subject_id ──→ Covariates
                ↓
        All Context → LLM → Response
```

**Pattern**: 1-hop traversal (shallow but wide across multiple dimensions)

---

## Key Insights

### What Makes a "Graph Operation"?

**Graph operations** involve traversing or querying the knowledge graph structure:
1. ✅ **Entity mapping via embeddings** - Uses entity embedding index
2. ✅ **Traversing entity links** - Following `entity.community_ids`, `entity.text_unit_ids`
3. ✅ **Edge filtering** - Finding relationships by source/target
4. ✅ **Attribute matching** - Linking covariates to entities

**Not graph operations** (even if they involve data structures):
- ❌ Vector search on raw text chunks (Basic Search)
- ❌ Reading pre-computed community summaries (Global Search)
- ❌ LLM calls for generation or decomposition
- ❌ Map-reduce aggregation

---

### Which Search Types Use the Graph?

| Search Type | Graph Index Required | Runtime Graph Traversal | Use Case |
|-------------|---------------------|------------------------|----------|
| **Basic** | ❌ No | ❌ No | Simple lookups, no entity analysis needed |
| **Local** | ✅ Yes (full) | ✅ Yes (1-hop) | Entity-centric questions, relationships |
| **Global** | ⚠️ Partial (community reports) | ❌ No | Dataset-level themes, high-level overview |
| **DRIFT** | ✅ Yes (full) | ✅ Yes (multi-hop) | Complex research, deep exploration |

---

### DRIFT = Orchestration Layer over Local Search

DRIFT does **not** add new graph operations. It only adds:
- **Query preprocessing**: HyDE expansion, vector search on community reports, LLM decomposition
- **Orchestration**: Iterative loop, state management, follow-up generation
- **Graph operations**: Delegates to `LocalSearch.search()` for all graph traversal

**Formula**: `DRIFT graph operations = Local Search graph operations × N iterations`

---

## Covariate as a Graph Operation

**Why Covariate retrieval is a graph operation**:

Covariates represent **entity-linked attributes stored as separate nodes** in the knowledge graph:

```python
@dataclass
class Covariate(Identified):
    subject_id: str              # [GRAPH LINK] Links to entity
    subject_type: str            # Type (usually "entity")
    covariate_type: str          # Type (e.g., "claim")
    attributes: dict | None      # Metadata (description, dates, status, etc.)
```

**Graph relationship**: `Covariate.subject_id → Entity.title`

**Examples of Covariates**:
- **Claims**: Assertions about entities ("Company X acquired Company Y")
- **Temporal data**: start_date, end_date for events
- **Status**: active, inactive, verified, disputed
- **Custom attributes**: Any entity-specific metadata

**Graph traversal pattern**:
```
Query → Entities (selected)
           ↓
    For each entity:
           ↓
    Covariates where covariate.subject_id == entity.title
           ↓
    Retrieved covariate attributes
```

**Usage in searches**:
- **Local Search**: Enriches entity context with claims and metadata
- **DRIFT Search**: Inherited from Local Search
- **Global/Basic**: Not used (no entity-level detail needed)

**Token budget**: Controlled within local context allocation, added after entities and relationships

---

## Conclusion

**Graph operations in GraphRAG are primarily performed by Local Search**, which does a 1-hop traversal starting from query-mapped entities. DRIFT extends this by calling Local Search iteratively for multi-hop exploration. Basic and Global search do not perform runtime graph operations, making them faster but less capable of entity-relationship reasoning.

The core graph operations are:
1. Entity mapping via vector search
2. Community lookup via entity links
3. Relationship retrieval via edge filtering
4. Text unit retrieval via entity references
5. Covariate retrieval via subject matching

These operations enable GraphRAG to provide rich, structured context that leverages the full knowledge graph structure for answering complex questions.
