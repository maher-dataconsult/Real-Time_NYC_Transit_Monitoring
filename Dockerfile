# NYC Transit Data Pipeline - Prefect Orchestrated
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install Python dependencies with retries and longer timeout
RUN pip install --upgrade pip --timeout 120 --retries 5 && \
    pip install --timeout 120 --retries 5 \
    prefect \
    dbt-snowflake \
    duckdb \
    dlt[snowflake] \
    snowflake-connector-python[pandas] \
    pandas \
    requests \
    pyarrow

# Copy project files
COPY . /app/

# Create directory for batch files
RUN mkdir -p /app/batch_files

# Expose Prefect UI port
EXPOSE 4200

# Set up entrypoint
RUN chmod +x /app/entrypoint.sh
ENTRYPOINT ["/app/entrypoint.sh"]
