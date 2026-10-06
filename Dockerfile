FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir "confluent-kafka==2.*" "jsonschema==4.*" "google-cloud-storage==3.*" "google-cloud-bigquery==3.*"
COPY src/ src/
COPY schemas/ schemas/
COPY sql/ sql/
COPY backtest/results.json backtest/results.json
RUN useradd --create-home producer
USER producer
ENV PYTHONPATH=/app/src
CMD ["python", "-m", "producer.run"]
