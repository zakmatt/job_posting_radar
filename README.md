# Job Posting Radar

Pipeline for ingesting, normalizing, embedding, and searching tech job postings from Polish markets (NoFluffJobs, JustJoin.it).

Built with **Kedro** for reproducibility and **Qdrant** for vector search.

## Quickstart

### 1. Environment Setup
```bash
uv sync
source .venv/bin/activate
```

### 2. Infrastructure
```bash
docker compose up -d
```

### 3. Run the Pipeline
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

## API Server

### Start the API
```bash
uvicorn app.main:app --reload --port 8000
```

### Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Health check & Qdrant status |
| GET | `/search` | Semantic job search |
| GET | `/similar` | Find similar jobs |
| GET | `/docs` | Swagger UI (auto-generated) |

### Example Requests
```bash
# Health check
curl http://localhost:8000/health

# Search for jobs
curl "http://localhost:8000/search?q=senior+python+developer&limit=10&mode=remote"

# Search with city filter
curl "http://localhost:8000/search?q=data+scientist&city=Warszawa&mode=hybrid"

# Find similar jobs
curl "http://localhost:8000/similar?source=nofluff&source_id=ABC123"

# Similar jobs across sources
curl "http://localhost:8000/similar?source=nofluff&source_id=ABC123&cross_source=true"
```

### Response Example
```json
{
  "query": "senior python developer",
  "total": 15,
  "duplicates_collapsed": 3,
  "results": [
    {
      "score": 0.891,
      "title": "Senior Python Developer",
      "company": "TechCorp",
      "locations": [{"city": "Warszawa", "country": "Poland"}],
      "work_mode": "remote",
      "salaries": [{"from_amount": 25000, "to_amount": 35000, "currency": "PLN", "period": "month"}],
      "source": "nofluff",
      "source_id": "ABC123",
      "job_url": "https://nofluffjobs.com/job/senior-python-dev",
      "also_on": ["justjoin"]
    }
  ]
}
```

## CLI Scripts

### Semantic Search
```bash
python scripts/search_jobs.py --q "senior ml engineer"
python scripts/search_jobs.py --q "python backend" --limit 10 --mode remote
python scripts/search_jobs.py --q "data scientist" --city Warszawa
```

### Similar Jobs
```bash
python scripts/similar_jobs.py --source nofluff --source-id 3DWAXHWK
python scripts/similar_jobs.py --source nofluff --source-id 3DWAXHWK --cross-source
```

### CLI Options
| Option | Description |
|--------|-------------|
| `--q` | Search query (required for search) |
| `--limit` | Max results (default: 20) |
| `--mode` | Work mode: `remote`, `hybrid`, `onsite` |
| `--city` | City name filter |
| `--source` | Source: `nofluff`, `justjoin` |
| `--no-collapse` | Show all duplicates |
| `--cross-source` | Include both sources (similar only) |
| `--exclude-same-company` | Exclude same company (similar only) |

## Monitoring

| Service | URL | Credentials |
|---------|-----|-------------|
| API Docs | http://localhost:8000/docs | - |
| Grafana | http://localhost:3000 | admin / admin |
| Prometheus | http://localhost:9090 | - |
| Qdrant | http://localhost:6333/dashboard | - |
| Kedro Viz | `kedro viz` | - |

## Project Structure

```
job-posting-radar/
├── app/                         # FastAPI application
│   ├── main.py                  # API endpoints
│   ├── models.py                # Response models
│   └── services.py              # Search service
├── conf/base/
│   ├── catalog.yml              # Dataset definitions
│   └── parameters.yml           # Pipeline parameters
├── scripts/
│   ├── search_jobs.py           # Search CLI
│   └── similar_jobs.py          # Similar jobs CLI
├── src/job_posting_radar/
│   ├── clients/                 # HTTP clients
│   ├── pipelines/
│   │   ├── ingestion/           # Fetch raw data
│   │   ├── normalization/       # Standardize schema
│   │   ├── embedding/           # Generate vectors
│   │   └── upsert/              # Upload to Qdrant
│   ├── config.py                # Settings
│   └── metrics.py               # Prometheus metrics
├── data/                        # Pipeline artifacts
└── docker/                      # Docker configs
```

## Configuration

### Pipeline Parameters (`conf/base/parameters.yml`)
```yaml
ingest:
  pages: 5
  limit: 100
  since_days: 7

embed:
  batch_size: 50

upsert:
  batch_size: 50
```

### Environment Variables
```bash
QDRANT_HOST=localhost
QDRANT_PORT=6333
QDRANT_COLLECTION_NAME=job_posts
EMBEDDING_MODEL_NAME=sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2
EMBEDDING_DIMENSION=384
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
```

## Troubleshooting

**Qdrant connection refused:**
```bash
docker compose up -d qdrant
```

**No jobs found:**
```bash
curl http://localhost:6333/collections/job_posts
kedro run
```

**Embedding model slow:** First run downloads ~500MB model. Cached afterward.
