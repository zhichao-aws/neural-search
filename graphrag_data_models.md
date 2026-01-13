# GraphRAG Data Models Reference

This document provides a comprehensive overview of all data models used in GraphRAG, including their fields and descriptions.

## Table of Contents
- [Base Classes](#base-classes)
- [Core Data Entities](#core-data-entities)
  - [Document](#document)
  - [TextUnit](#textunit)
  - [Entity](#entity)
  - [Relationship](#relationship)
  - [Community](#community)
  - [CommunityReport](#communityreport)
  - [Covariate](#covariate)
- [Data Model Hierarchy](#data-model-hierarchy)

---

## Base Classes

### Identified

**Location**: `graphrag/data_model/identified.py:10`

Base class providing identification fields for all data models.

| Field | Type | Description |
|-------|------|-------------|
| `id` | `str` | The unique identifier of the item (typically UUID) |
| `short_id` | `str \| None` | Human-readable ID used to refer to this item in prompts or texts displayed to users, such as in a report text (optional) |

---

### Named

**Location**: `graphrag/data_model/named.py:12`

**Inherits from**: `Identified`

Extends `Identified` with a title/name field.

| Field | Type | Description |
|-------|------|-------------|
| `title` | `str` | The name/title of the item |
| *Inherited* | | All fields from `Identified` (id, short_id) |

---

## Core Data Entities

### Document

**Location**: `graphrag/data_model/document.py:13`

**Inherits from**: `Named`

A protocol for a document in the system.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `id` | `str` | - | Unique identifier (inherited from `Identified`) |
| `short_id` | `str \| None` | `None` | Human-readable ID (inherited from `Identified`) |
| `title` | `str` | - | Title of the document (inherited from `Named`) |
| `type` | `str` | `"text"` | Type of the document |
| `text` | `str` | `""` | The raw text content of the document |
| `text_unit_ids` | `list[str]` | `[]` | List of text unit IDs in the document |
| `attributes` | `dict[str, Any] \| None` | `None` | A dictionary of structured attributes such as author, etc. (optional) |

---

### TextUnit

**Location**: `graphrag/data_model/text_unit.py:13`

**Inherits from**: `Identified`

A protocol for a TextUnit item in a Document database. Text units are chunks of text extracted from documents.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `id` | `str` | - | Unique identifier (inherited from `Identified`) |
| `short_id` | `str \| None` | `None` | Human-readable ID (inherited from `Identified`) |
| `text` | `str` | - | The text content of the unit |
| `entity_ids` | `list[str] \| None` | `None` | List of entity IDs related to the text unit (optional) |
| `relationship_ids` | `list[str] \| None` | `None` | List of relationship IDs related to the text unit (optional) |
| `covariate_ids` | `dict[str, list[str]] \| None` | `None` | Dictionary of different types of covariates related to the text unit (optional) |
| `n_tokens` | `int \| None` | `None` | The number of tokens in the text (optional) |
| `document_ids` | `list[str] \| None` | `None` | List of document IDs in which the text unit appears (optional) |
| `attributes` | `dict[str, Any] \| None` | `None` | A dictionary of additional attributes associated with the text unit (optional) |

---

### Entity

**Location**: `graphrag/data_model/entity.py:13`

**Inherits from**: `Named`

A protocol for an entity in the system. Entities are named objects extracted from text (people, organizations, locations, events, etc.).

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `id` | `str` | - | Unique identifier (inherited from `Identified`) |
| `short_id` | `str \| None` | `None` | Human-readable ID (inherited from `Identified`) |
| `title` | `str` | - | Name/title of the entity (inherited from `Named`) |
| `type` | `str \| None` | `None` | Type of the entity (can be any string, e.g., "PERSON", "ORGANIZATION") (optional) |
| `description` | `str \| None` | `None` | Description of the entity (optional) |
| `description_embedding` | `list[float] \| None` | `None` | The semantic (i.e. text) embedding of the entity description (optional) |
| `name_embedding` | `list[float] \| None` | `None` | The semantic (i.e. text) embedding of the entity name (optional) |
| `community_ids` | `list[str] \| None` | `None` | The community IDs this entity belongs to (optional) |
| `text_unit_ids` | `list[str] \| None` | `None` | List of text unit IDs in which the entity appears (optional) |
| `rank` | `int \| None` | `1` | Rank of the entity, used for sorting (optional). Higher rank indicates more important entity. This can be based on centrality or other metrics |
| `attributes` | `dict[str, Any] \| None` | `None` | Additional attributes associated with the entity (optional), e.g. start time, end time, etc. To be included in the search prompt |

---

### Relationship

**Location**: `graphrag/data_model/relationship.py:13`

**Inherits from**: `Identified`

A relationship between two entities. This is a generic relationship that can represent any type of connection between entities.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `id` | `str` | - | Unique identifier (inherited from `Identified`) |
| `short_id` | `str \| None` | `None` | Human-readable ID (inherited from `Identified`) |
| `source` | `str` | - | The source entity name |
| `target` | `str` | - | The target entity name |
| `weight` | `float \| None` | `1.0` | The edge weight (strength of the relationship) |
| `description` | `str \| None` | `None` | A description of the relationship (optional) |
| `description_embedding` | `list[float] \| None` | `None` | The semantic embedding for the relationship description (optional) |
| `text_unit_ids` | `list[str] \| None` | `None` | List of text unit IDs in which the relationship appears (optional) |
| `rank` | `int \| None` | `1` | Rank of the relationship, used for sorting (optional). Higher rank indicates more important relationship. This can be based on centrality or other metrics |
| `attributes` | `dict[str, Any] \| None` | `None` | Additional attributes associated with the relationship (optional). To be included in the search prompt |

---

### Community

**Location**: `graphrag/data_model/community.py:13`

**Inherits from**: `Named`

A protocol for a community in the system. Communities are hierarchical clusters of related entities discovered through graph analysis (using Leiden algorithm).

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `id` | `str` | - | Unique identifier (inherited from `Identified`) |
| `short_id` | `str \| None` | `None` | Human-readable ID (inherited from `Identified`) |
| `title` | `str` | - | Title/name of the community (inherited from `Named`) |
| `level` | `str` | - | Community level in the hierarchy |
| `parent` | `str` | - | Community ID of the parent node of this community |
| `children` | `list[str]` | - | List of community IDs of the child nodes of this community |
| `entity_ids` | `list[str] \| None` | `None` | List of entity IDs related to the community (optional) |
| `relationship_ids` | `list[str] \| None` | `None` | List of relationship IDs related to the community (optional) |
| `text_unit_ids` | `list[str] \| None` | `None` | List of text unit IDs related to the community (optional) |
| `covariate_ids` | `dict[str, list[str]] \| None` | `None` | Dictionary of different types of covariates related to the community (optional), e.g. claims |
| `attributes` | `dict[str, Any] \| None` | `None` | A dictionary of additional attributes associated with the community (optional). To be included in the search prompt |
| `size` | `int \| None` | `None` | The size of the community (number of text units) |
| `period` | `str \| None` | `None` | ISO8601 timestamp for incremental updates and versioning |

---

### CommunityReport

**Location**: `graphrag/data_model/community_report.py:13`

**Inherits from**: `Named`

Defines an LLM-generated summary report of a community. These reports provide high-level insights about the community's themes and content.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `id` | `str` | - | Unique identifier (inherited from `Identified`) |
| `short_id` | `str \| None` | `None` | Human-readable ID (inherited from `Identified`) |
| `title` | `str` | - | Title of the report (inherited from `Named`) |
| `community_id` | `str` | - | The ID of the community this report is associated with |
| `summary` | `str` | `""` | Summary of the report |
| `full_content` | `str` | `""` | Full content of the report |
| `rank` | `float \| None` | `1.0` | Rank of the report, used for sorting (optional). Higher means more important |
| `full_content_embedding` | `list[float] \| None` | `None` | The semantic (i.e. text) embedding of the full report content (optional) |
| `attributes` | `dict[str, Any] \| None` | `None` | A dictionary of additional attributes associated with the report (optional) |
| `size` | `int \| None` | `None` | The size of the report (number of text units) |
| `period` | `str \| None` | `None` | The period of the report (ISO8601 timestamp for versioning) (optional) |

---

### Covariate

**Location**: `graphrag/data_model/covariate.py:13`

**Inherits from**: `Identified`

A protocol for a covariate in the system. Covariates are metadata associated with a subject, such as entity claims. Each subject (e.g., entity) may be associated with multiple types of covariates.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `id` | `str` | - | Unique identifier (inherited from `Identified`) |
| `short_id` | `str \| None` | `None` | Human-readable ID (inherited from `Identified`) |
| `subject_id` | `str` | - | The subject ID (e.g., entity ID) |
| `subject_type` | `str` | `"entity"` | The subject type |
| `covariate_type` | `str` | `"claim"` | The covariate type (e.g., "claim") |
| `text_unit_ids` | `list[str] \| None` | `None` | List of text unit IDs in which the covariate info appears (optional) |
| `attributes` | `dict[str, Any] \| None` | `None` | Additional attributes for the covariate (optional) |

---

## Data Model Hierarchy

```
Identified (base class)
├── id: str
└── short_id: str | None

Named (extends Identified)
├── title: str
└── [inherits: id, short_id]

Concrete Models:
├── Document (extends Named)
│   └── Fields: type, text, text_unit_ids, attributes
│
├── Entity (extends Named)
│   └── Fields: type, description, description_embedding, name_embedding,
│       community_ids, text_unit_ids, rank, attributes
│
├── Community (extends Named)
│   └── Fields: level, parent, children, entity_ids, relationship_ids,
│       text_unit_ids, covariate_ids, attributes, size, period
│
├── CommunityReport (extends Named)
│   └── Fields: community_id, summary, full_content, rank,
│       full_content_embedding, attributes, size, period
│
├── TextUnit (extends Identified)
│   └── Fields: text, entity_ids, relationship_ids, covariate_ids,
│       n_tokens, document_ids, attributes
│
├── Relationship (extends Identified)
│   └── Fields: source, target, weight, description, description_embedding,
│       text_unit_ids, rank, attributes
│
└── Covariate (extends Identified)
    └── Fields: subject_id, subject_type, covariate_type,
        text_unit_ids, attributes
```

---

## Parquet Output Columns

When saved to Parquet files, the data models use the following column orderings:

### Entities (`entities.parquet`)
`id`, `human_readable_id`, `title`, `type`, `description`, `text_unit_ids`, `frequency`, `degree`, `x`, `y`

### Relationships (`relationships.parquet`)
`id`, `human_readable_id`, `source`, `target`, `description`, `weight`, `combined_degree`, `text_unit_ids`

### Communities (`communities.parquet`)
`id`, `human_readable_id`, `community`, `level`, `parent`, `children`, `title`, `entity_ids`, `relationship_ids`, `text_unit_ids`, `period`, `size`

### Community Reports (`community_reports.parquet`)
`id`, `human_readable_id`, `community`, `level`, `parent`, `children`, `title`, `summary`, `full_content`, `rank`, `rating_explanation`, `findings`, `full_content_json`, `period`, `size`

### Text Units (`text_units.parquet`)
`id`, `human_readable_id`, `text`, `n_tokens`, `document_ids`, `entity_ids`, `relationship_ids`, `covariate_ids`

### Documents (`documents.parquet`)
`id`, `human_readable_id`, `title`, `text`, `text_unit_ids`, `creation_date`, `metadata`

### Covariates (`covariates.parquet`)
`id`, `human_readable_id`, `covariate_type`, `type`, `description`, `subject_id`, `object_id`, `status`, `start_date`, `end_date`, `source_text`, `text_unit_id`

---

## Additional Notes

- All data models are implemented as Python dataclasses
- All models include a `from_dict()` class method for deserialization
- The `attributes` field in most models allows for extensible metadata storage
- Embeddings are stored as `list[float]` representing vector representations
- IDs are typically UUIDs in string format
- `short_id` provides human-readable references for use in prompts and reports
- The schema definitions are centralized in `graphrag/data_model/schemas.py`
