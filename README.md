#### Install uv tool

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

#### Install dependencies

```bash
uv sync
```

#### Run Prefect server and PostgreSQL.
```bash
docker compose up -d

# Webview: http://127.0.0.1:4200
```

#### Set environment variables

```bash
uv run prefect config set PREFECT_API_URL=http://localhost:4200/api

# Others
uv run prefect config view
```

#### Run a workflow locally

```bash
uv run ./src/flows/01_hello_world.py
```


#### Example: Download orders data and generate a report
```bash
uv run ./src/flows/02_generate_orders_report.py

# New terminal tab
uv run ./src/flows/02_download_orders.py

# Simplier version
uv run ./src/flows/03_orders_report.py
```


#### Start worker:
```
uv run prefect worker start --pool "pool-1" --work-queue "default"
```

#### Deploy workflow:
```
uv run prefect deploy --name hello-world-deployment
```