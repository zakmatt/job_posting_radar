# Job Posting Radar

Pipeline for ingesting, normalizing, embedding, and searching tech job postings from Polish markets (NoFluffJobs, JustJoin.it).

Built with **Kedro** for reproducibility and **Qdrant** for vector search.

## Quickstart

### 1. Environment Setup
Create a virtual environment and install dependencies using `uv`:
```bash
uv sync
source .venv/bin/activate
```

### 2. Infrastructure
Start the monitoring and database stack (Qdrant, Prometheus, Grafana, Pushgateway):
```bash
docker compose up -d
```

### 3. Run the Pipeline
Run the full pipeline (Ingest → Normalize → Embed → Upsert):
```bash
kedro run
```

Or run specific stages:
```bash
kedro run --pipeline ingest      # Fetch jobs from NoFluff & JustJoin
kedro run --pipeline normalize   # Standardize to common schema
kedro run --pipeline embed       # Generate vector embeddings
kedro run --pipeline upsert      # Upload to Qdrant
```

## Search & Discovery

### Semantic Search
Find jobs matching a natural language query:
```bash
# Basic search
python scripts/search_jobs.py --q "senior ml engineer"

# With filters
python scripts/search_jobs.py --q "python backend developer" --limit 10 --mode remote
python scripts/search_jobs.py --q "data scientist" --city Warszawa --mode hybrid
python scripts/search_jobs.py --q "devops kubernetes" --source nofluff --limit 5
```

**Options:**
- `--q` - Search query (required)
- `--limit` - Max results (default: 20)
- `--mode` - Filter by work mode: `remote`, `hybrid`, `onsite`
- `--city` - Filter by city name
- `--source` - Filter by source: `nofluff`, `justjoin`

### Similar Jobs
Find jobs similar to an existing posting:
```bash
# Find similar jobs from the same source
python scripts/similar_jobs.py --source nofluff --source-id 3DWAXHWK

# Cross-source similarity (find JustJoin jobs similar to a NoFluff posting)
python scripts/similar_jobs.py --source nofluff --source-id 3DWAXHWK --cross-source

# Exclude same company
python scripts/similar_jobs.py --source justjoin --source-id senior-python-dev --exclude-same-company
```

**Options:**
- `--source` - Source of reference job: `nofluff`, `justjoin` (required)
- `--source-id` - Source-specific job ID (required)
- `--limit` - Max results (default: 20)
- `--cross-source` - Include jobs from both sources
- `--exclude-same-company` - Exclude jobs from the same company

### Example Output
```
================================================================================
Search results for: "senior ml engineer"
Found 20 matching jobs
================================================================================

 1. [0.847] Senior Machine Learning Engineer
    Company:  TechCorp
    Location: Warszawa, Krakow (remote)
    Salary:   25,000-35,000 PLN/month (b2b)
    Source:   nofluff
    URL:      https://nofluffjobs.com/job/senior-ml-engineer-techcorp

 2. [0.823] ML Platform Engineer
    Company:  DataStartup
    Location: Remote (remote)
    Salary:   22,000-30,000 PLN/month (b2b)
    Source:   justjoin
    URL:      https://justjoin.it/offers/ml-platform-engineer
```

## Monitoring & Visualization

| Service | URL | Credentials |
|---------|-----|-------------|
| Grafana | http://localhost:3000 | admin / admin |
| Prometheus | http://localhost:9090 | - |
| Qdrant Dashboard | http://localhost:6333/dashboard | - |
| Kedro Viz | `kedro viz` | - |

## Project Structure

```
job-posting-radar/
├── conf/
│   └── base/
│       ├── catalog.yml      # Dataset definitions
│       └── parameters.yml   # Pipeline parameters
├── scripts/
│   ├── search_jobs.py       # Semantic search CLI
│   └── similar_jobs.py      # Similar jobs CLI
├── src/job_posting_radar/
│   ├── clients/             # HTTP clients
│   │   ├── nofluff.py       # NoFluffJobs API client
│   │   └── justjoin.py      # JustJoin API client
│   ├── pipelines/
│   │   ├── ingestion/       # Fetch raw job data
│   │   ├── normalization/   # Standardize schema
│   │   │   ├── models.py    # Pydantic models
│   │   │   ├── normalizers.py
│   │   │   └── utils.py
│   │   ├── embedding/       # Generate vectors
│   │   │   └── embeddings.py
│   │   └── upsert/          # Upload to Qdrant
│   │       └── store.py
│   ├── config.py            # App settings (env vars)
│   └── metrics.py           # Prometheus metrics
├── data/
│   ├── 01_raw/              # Raw API responses
│   ├── 02_normalized/       # Standardized postings
│   ├── 03_embedded/         # With embeddings
│   └── 04_vector_records/   # Ready for Qdrant
└── docker/                  # Docker configs
```

## Configuration

### Pipeline Parameters (`conf/base/parameters.yml`)
```yaml
ingest:
  pages: 5           # Pages to fetch per source
  limit: 100         # Max postings per source
  since_days: 7      # Only recent postings

embed:
  batch_size: 50     # Embedding batch size

upsert:
  batch_size: 50     # Qdrant upsert batch size
```

### Environment Variables
Override defaults in `.env` or export directly:
```bash
# Qdrant
QDRANT_HOST=localhost
QDRANT_PORT=6333
QDRANT_COLLECTION_NAME=job_posts

# Embedding model
EMBEDDING_MODEL_NAME=sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2
EMBEDDING_DIMENSION=384

# Metrics
PUSHGATEWAY_HOST=localhost
PUSHGATEWAY_PORT=9091
```

## Data Pipeline

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Ingest    │────▶│  Normalize  │────▶│    Embed    │────▶│   Upsert    │
│             │     │             │     │             │     │             │
│ NoFluff API │     │ Pydantic    │     │ Sentence    │     │   Qdrant    │
│ JustJoin API│     │ Models      │     │ Transformers│     │   Vector DB │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
     │                    │                   │                    │
     ▼                    ▼                   ▼                    ▼
  01_raw/            02_normalized/      03_embedded/        Vector Store
```

## Troubleshooting

**Qdrant connection refused:**
```bash
docker compose up -d qdrant
```

**No jobs found in search:**
```bash
# Check collection status
curl http://localhost:6333/collections/job_posts

# Run the full pipeline
kedro run
```

**Embedding model download slow:**
The first run downloads the model (~500MB). Subsequent runs use cached model.
