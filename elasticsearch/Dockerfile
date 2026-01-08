# Base image
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir \
    fastapi==0.100.0 \
    uvicorn[standard]==0.23.0 \
    elasticsearch==8.13.2 \
    python-dotenv \
    requests

# Copy application files
COPY elasticsearch_queries.py .
COPY query_api.py .

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD python -c "import requests; requests.get('http://localhost:8000/health')"

# Run API server
CMD ["uvicorn", "query_api:app", "--host", "0.0.0.0", "--port", "8000"]
