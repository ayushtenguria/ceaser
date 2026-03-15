# Ceaser Backend

AI-powered data analysis platform backend built with FastAPI, LangGraph, and SQLAlchemy.

## Prerequisites

- Python 3.11+
- PostgreSQL (running locally on port 5432)
- A Clerk account (for auth)
- A Google Gemini API key and/or Anthropic API key

## Setup

### 1. Create the database

```bash
createdb ceaser
```

### 2. Install dependencies

```bash
cd ceaser/backend
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

For optional connectors:

```bash
pip install -e ".[bigquery]"
pip install -e ".[snowflake]"
```

### 3. Configure environment

```bash
cp .env.example .env
```

Edit `.env` and fill in your keys. To generate a Fernet encryption key:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

### 4. Run database migrations

```bash
alembic revision --autogenerate -m "initial"
alembic upgrade head
```

Or let the app create tables automatically on first startup (dev convenience).

### 5. Start the server

```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

The API docs are available at [http://localhost:8000/docs](http://localhost:8000/docs).

## Project Structure

```
app/
  main.py             # FastAPI app, lifespan, middleware
  core/
    config.py         # Pydantic Settings (env vars)
    security.py       # Clerk JWT verification
    deps.py           # FastAPI dependencies (DB session, auth, LLM)
  db/
    session.py        # Async engine, session factory, Base
    models.py         # SQLAlchemy ORM models
    migrations/       # Alembic migrations
  api/
    auth.py           # User sync and /me endpoint
    chat.py           # Chat + conversation CRUD
    connections.py    # External DB connection management
    files.py          # File upload / list / delete
    schemas.py        # Pydantic request/response models
  agents/
    state.py          # LangGraph AgentState TypedDict
    router.py         # Query classification node
    sql_agent.py      # SQL generation node
    python_agent.py   # Python code generation node
    executor.py       # SQL / Python execution node
    graph.py          # StateGraph definition and run_agent()
  connectors/
    base.py           # Abstract BaseConnector
    postgres.py       # PostgreSQL (asyncpg)
    mysql.py          # MySQL (pymysql)
    sqlite_conn.py    # SQLite (aiosqlite)
    factory.py        # get_connector() factory
  sandbox/
    executor.py       # Subprocess-based Python code execution
  services/
    encryption.py     # Fernet encrypt / decrypt
    schema.py         # DB schema introspection
    file_parser.py    # CSV / Excel parsing
```

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Health check |
| POST | `/api/v1/auth/sync` | Sync Clerk user to local DB |
| GET | `/api/v1/auth/me` | Current user info |
| POST | `/api/v1/chat` | Send message, receive SSE stream |
| GET | `/api/v1/conversations` | List conversations |
| GET | `/api/v1/conversations/{id}` | Get conversation |
| GET | `/api/v1/conversations/{id}/messages` | List messages |
| DELETE | `/api/v1/conversations/{id}` | Delete conversation |
| POST | `/api/v1/connections/` | Create DB connection |
| GET | `/api/v1/connections/` | List connections |
| POST | `/api/v1/connections/{id}/test` | Test connection |
| POST | `/api/v1/connections/{id}/schema` | Refresh schema cache |
| DELETE | `/api/v1/connections/{id}` | Delete connection |
| POST | `/api/v1/files/upload` | Upload CSV/Excel |
| GET | `/api/v1/files/` | List uploaded files |
| DELETE | `/api/v1/files/{id}` | Delete file |
