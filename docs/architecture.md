# Data-to-LLM Architecture & RAG-Enabled Chatbot
## Section D — High-Level Solution Design

---

## Overview

This document describes the end-to-end architecture that connects the Electricity Maps ETL pipeline (Bronze → Silver → Gold) to an LLM-based conversational interface. The goal is to allow internal users — analysts, engineers, and managers to ask natural language questions about France's electricity production, flows, imports, and exports, and receive factual, grounded answers.

The system uses **Retrieval-Augmented Generation (RAG)** to combine two types of knowledge:
- **Structured data** from the Gold layer (Parquet files produced by the ETL pipeline)
- **Unstructured documentation** such as FAQs, domain glossaries, and Electricity Maps API documentation

---

## ETL Pipeline — What the RAG System Builds On

Before describing the RAG architecture, it is important to understand exactly what the upstream pipeline produces, since the chatbot queries these outputs directly.

### Silver Layer
Built using **PySpark**. Reads raw Bronze JSON files, flattens nested structures, enforces explicit schemas, validates records, and writes clean Parquet files.

| Table | Location | Key Fields |
|---|---|---|
| `electricity_flows` | `data/silver/electricity_flows/parquet/` | `zone`, `data_datetime`, `powerConsumptionTotal`, `powerProductionTotal`, `powerImportTotal`, `powerExportTotal`, `fossilFreePercentage`, `renewablePercentage`, per-source consumption and production breakdowns |
| `electricity_mix` | `data/silver/electricity_mix/parquet/` | `zone`, `data_datetime`, `carbonIntensity`, `isEstimated`, `estimationMethod` |

### Gold Layer
Built using **Pandas**. Reads Silver Parquet files and produces two analytics-ready data products:

**`energy_mix`** — Relative contribution of each energy source to total production:

| Column | Description |
|---|---|
| `zone` | Always `FR` |
| `data_datetime` | Hourly timestamp of the data point |
| `solar_pct` | Solar as fraction of total production |
| `wind_pct` | Wind as fraction of total production |
| `hydro_pct` | Hydro as fraction of total production |
| `nuclear_pct` | Nuclear as fraction of total production |
| `coal_pct` | Coal as fraction of total production |
| `gas_pct` | Gas as fraction of total production |

**`import_export`** — Net electricity flow in and out of France:

| Column | Description |
|---|---|
| `zone` | Always `FR` |
| `data_datetime` | Hourly timestamp of the data point |
| `powerImportTotal` | Total MWh imported into France |
| `powerExportTotal` | Total MWh exported from France |
| `net_import_export` | `powerImportTotal - powerExportTotal` (positive = net importer) |

These two Parquet files are the **primary structured data source** for the RAG chatbot.

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                        APPLICATION LAYER                            │
│                                                                     │
│   User (Analyst / Engineer / Manager)                               │
│         │                                                           │
│         ▼                                                           │
│   ┌─────────────┐                                                   │
│   │  Chat UI    │  (Internal web app / Slack bot)                   │
│   └──────┬──────┘                                                   │
│          │  Natural language question                               │
│          ▼                                                          │
│   ┌──────────────────┐                                              │
│   │   API Gateway    │  (FastAPI)                                   │
│   └──────┬───────────┘                                              │
│          │                                                          │
│          ▼                                                          │
│   ┌──────────────────────────────────────────┐                      │
│   │         RAG Orchestration Layer           │                     │
│   │       (LangChain / LlamaIndex)            │                     │
│   │                                           │                     │
│   │  1. Query Classification                  │                     │
│   │     └─ Data question or doc question?     │                     │
│   │                                           │                     │
│   │  2. Retrieval (one or both paths)         │                     │
│   │     ├─ Structured  → DuckDB query         │                     │
│   │     │   on Gold Parquet files             │                     │
│   │     └─ Unstructured → Vector search       │                     │
│   │         on embedded documentation         │                     │
│   │                                           │                     │
│   │  3. Context assembly                      │                     │
│   │     └─ Merge retrieved results            │                     │
│   │        into a single prompt context       │                     │
│   │                                           │                     │
│   │  4. LLM call with context + question      │                     │
│   └──────┬───────────────────────┬────────────┘                     │
│          │                       │                                  │
│          ▼                       ▼                                  │
│   ┌─────────────┐      ┌──────────────────┐                        │
│   │  Structured │      │  Unstructured    │                        │
│   │  Retriever  │      │  Retriever       │                        │
│   │  (DuckDB    │      │  (Vector DB —    │                        │
│   │  on Parquet)│      │  FAISS / Chroma) │                        │
│   └──────┬──────┘      └────────┬─────────┘                        │
│          │                      │                                   │
└──────────┼──────────────────────┼───────────────────────────────────┘
           │                      │
┌──────────┼──────────────────────┼───────────────────────────────────┐
│          │  INFRASTRUCTURE LAYER│                                   │
│          ▼                      ▼                                   │
│  ┌──────────────────────┐  ┌──────────────────────┐                │
│  │   Gold Layer         │  │   Document Store      │               │
│  │   (Parquet files)    │  │                       │               │
│  │                      │  │ • Electricity Maps    │               │
│  │  data/gold/          │  │   API documentation   │               │
│  │  ├─ energy_mix/      │  │ • Domain FAQs         │               │
│  │  │   data.parquet    │  │ • Energy glossary     │               │
│  │  └─ import_export/   │  │ • Zone metadata       │               │
│  │      data.parquet    │  │                       │               │
│  │                      │  │ (PDF / MD / TXT)      │               │
│  └──────────────────────┘  └──────────┬────────────┘               │
│                                        │                            │
│                                        ▼                            │
│                            ┌──────────────────────┐                │
│                            │  Embedding Pipeline   │               │
│                            │  (offline batch job)  │               │
│                            │                       │               │
│                            │ • Chunk documents     │               │
│                            │ • Embed with          │               │
│                            │   OpenAI / HuggingFace│               │
│                            │ • Store in Vector DB  │               │
│                            └──────────────────────┘                │
│                                                                     │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │                       LLM Service                            │  │
│  │         (Claude API / OpenAI GPT / Azure OpenAI)             │  │
│  │                                                              │  │
│  │  Receives: question + retrieved context                      │  │
│  │  Returns:  grounded natural language answer                  │  │
│  └──────────────────────────────────────────────────────────────┘  │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Application Layer

### 1. Chat Interface
The entry point for internal users. A lightweight internal web app or Slack bot where users ask questions in plain English, for example:

- *"What share of France's electricity came from solar yesterday?"*
- *"Was France a net importer or exporter of electricity last week?"*
- *"What does fossil-free percentage mean?"*

### 2. API Gateway
A FastAPI service that receives the question, routes it to the RAG orchestration layer, and returns the final answer. It handles authentication and request logging.

### 3. RAG Orchestration Layer
Built with **LangChain** or **LlamaIndex**, this layer runs four steps on every question:

**Step 1 — Query Classification**
The question is classified as a *data question* (requires querying Gold Parquet files), a *documentation question* (requires searching embedded docs), or both.

**Step 2 — Retrieval**
- Data questions → DuckDB reads the Gold Parquet files directly and returns a small result set as context. For example:
  ```sql
  SELECT zone, data_datetime, solar_pct, wind_pct, hydro_pct
  FROM energy_mix
  ORDER BY data_datetime DESC LIMIT 24
  ```
- Documentation questions → The question is embedded and semantically similar document chunks are retrieved from the vector database.

**Step 3 — Context Assembly**
Retrieved data rows and document chunks are combined into a structured context block that fits within the LLM's context window.

**Step 4 — LLM Call**
The question and context are sent to the LLM with a system prompt instructing it to answer only from the provided context and say "I don't know" if the answer is not there. This prevents hallucination.

---

## Infrastructure Layer

### Gold Layer — Structured Data Source

The two Parquet files produced by `gold.py` are queried at runtime using DuckDB:

| File | What it answers |
|---|---|
| `data/gold/energy_mix/data.parquet` | Questions about energy source percentages — solar, wind, nuclear, hydro, gas, coal contribution to total production |
| `data/gold/import_export/data.parquet` | Questions about whether France is importing or exporting, and the net MWh flow |

DuckDB is used because it queries Parquet files directly with SQL and requires zero cluster infrastructure — a single Python process handles it.

### Document Store — Unstructured Knowledge Source
Raw text files indexed offline into the vector database:
- Electricity Maps API documentation
- FAQs (what is carbon intensity, what is fossil-free %, what is sandbox mode)
- Zone metadata and country code mappings
- Energy transition glossary

### Embedding Pipeline
An offline batch job run once, and re-run when documents change:
1. Reads raw documents from the document store
2. Splits into overlapping chunks (~512 tokens, 50-token overlap)
3. Generates embeddings using OpenAI `text-embedding-ada-002` or a HuggingFace model
4. Stores embeddings in the vector database

### Vector Database
- **Development**: FAISS or ChromaDB (local, no infrastructure)
- **Production**: Pinecone or Weaviate (managed, scalable)

### LLM Service
Options depending on deployment context:
- **Anthropic Claude API** — strong reasoning, stays grounded in provided context
- **OpenAI GPT-4** — widely used, reliable
- **Azure OpenAI** — preferred for enterprise environments with data residency requirements

---

## How Structured and Unstructured Data Are Combined

A concrete example showing how both retrieval paths work together:

```
Question: "Is France becoming more reliant on renewables?"

Step 1 — Classified as: data question + documentation question

Step 2a — Structured retriever (DuckDB on energy_mix parquet):
  SELECT data_datetime, solar_pct, wind_pct, hydro_pct
  FROM energy_mix
  ORDER BY data_datetime DESC

  Result: table of renewable percentages over time

Step 2b — Unstructured retriever (vector search on docs):
  Retrieved chunk: "Renewables in Electricity Maps data include
  solar, wind, and hydro. Nuclear is fossil-free but is not
  counted as renewable..."

Step 3 — Context assembled:
  [DATA]: Recent solar_pct, wind_pct, hydro_pct values
  [DOCS]: Definition of renewables vs fossil-free

Step 4 — LLM generates answer:
  "Based on the data, France's combined renewable share
   (solar + wind + hydro) over the captured period shows
   solar and wind as the strongest contributors. Note that
   nuclear, while fossil-free, is not included in the
   renewable percentage figure."
```

This dual approach ensures answers are grounded in real pipeline data and contextually explained using domain knowledge — neither alone is sufficient.

---

## Design Decisions

**Why DuckDB for Gold layer queries?**
The Gold layer produces plain Parquet files via Pandas. DuckDB queries these directly with SQL — no Spark cluster needed at chatbot query time. It is fast for the small result sets a chatbot needs and adds zero operational overhead.

**Why RAG instead of fine-tuning?**
Fine-tuning embeds knowledge statically into model weights. Since the Gold layer updates every time the pipeline runs, RAG is the right choice — it retrieves fresh data at query time without retraining the model.

**Why Pandas for Gold instead of PySpark?**
The Gold transformations (percentage calculations, net flow arithmetic) are straightforward column-level operations on a small dataset. Spinning up a Spark session for this would be unnecessary overhead. Pandas is the right tool at this scale.

**Why a dual retriever?**
Numbers, trends, and aggregates are best answered by precise SQL against the Parquet files. Definitions, context, and explanations are best answered by semantic search over documentation. Serving both from one retrieval mechanism would degrade quality for both.

---

## Technology Stack Summary

| Component | Technology |
|---|---|
| Chat UI | React internal app / Slack bot |
| API Gateway | FastAPI (Python) |
| RAG Orchestration | LangChain or LlamaIndex |
| Structured Retriever | DuckDB querying Gold Parquet files |
| Unstructured Retriever | FAISS (dev) / Pinecone (prod) |
| Embedding Model | OpenAI text-embedding-ada-002 or HuggingFace |
| LLM | Anthropic Claude / OpenAI GPT-4 / Azure OpenAI |
| Silver Processing | PySpark with explicit schema enforcement |
| Gold Processing | Pandas (`energy_mix`, `import_export` Parquet files) |
| Data Storage | Local Parquet files (extensible to S3) |
| Vector Database | ChromaDB (dev) / Pinecone (prod) |