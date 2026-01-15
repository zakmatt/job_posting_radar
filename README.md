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
Run the full pipeline (Ingest → Normalize → Embed):
```bash
kedro run
```

Or run specific stages:
```bash
kedro run --pipeline ingest
kedro run --pipeline normalize
kedro run --pipeline embed
```

## Monitoring & Visualization

- **Grafana**: [http://localhost:3000](http://localhost:3000) (Login: `admin`/`admin`)
- **Prometheus**: [http://localhost:9090](http://localhost:9090)
- **Qdrant Dashboard**: [http://localhost:6333/dashboard](http://localhost:6333/dashboard)
- **Kedro Viz**: Run `kedro viz` to see the pipeline DAG.

## Project Structure

- `conf/`: Configuration files (catalog, parameters).
- `src/job_posting_radar/`: Source code.
    - `clients/`: HTTP clients for NoFluff and JustJoin.
    - `normalization/`: Logic for cleaning and standardizing data.
    - `vector/`: Embedding generation and Qdrant integration.
    - `pipelines/`: Kedro pipeline and node definitions.
- `data/`: Local storage for raw and processed job postings.

## Configuration

- Pipeline parameters live in `conf/base/parameters.yml`.
- Application-wide settings (URLs, timeouts) are in `src/job_posting_radar/config.py` and can be overridden via `.env`.
