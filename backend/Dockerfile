FROM python:3.12-slim

WORKDIR /app

# Install system deps for psycopg2 and openpyxl
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev && rm -rf /var/lib/apt/lists/*

# Install Python deps
COPY pyproject.toml .
RUN pip install --no-cache-dir -e . && \
    pip install --no-cache-dir plotly chardet pyarrow tabulate

# Copy app code
COPY . .

# Create required directories
RUN mkdir -p uploads/parquet logs

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
