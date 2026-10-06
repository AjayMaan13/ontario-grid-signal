# The official Airflow image plus the libraries our tasks import. Our own code is not copied in:
# git-sync puts the repo on the pods and PYTHONPATH points at its src/ folder.
FROM apache/airflow:3.2.2
RUN pip install --no-cache-dir "confluent-kafka==2.*" "jsonschema==4.*" "google-cloud-storage==3.*" "google-cloud-bigquery==3.*"
