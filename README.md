# Health Stream ML: Event-Driven Anomaly Detection

This project demonstrates an event-driven architecture for continuous health monitoring and anomaly detection using streaming telemetry data. It evaluates massive influxes of physiological data on the fly rather than using batch processing.

## Architecture

*   **Ingestion:** Apache Kafka for high-throughput streaming data simulation. A producer script generates dummy telemetry data (heart rate, blood pressure) mimicking wearable devices.
*   **Modeling:** A streaming ML service built with `scikit-learn`'s `IsolationForest`. It maintains a sliding window of data to compute dynamic risk scores.
*   **Monitoring:** Prometheus scrapes the model service for dynamic risk scores, anomaly triggers, and "data drift" (changes in baseline distribution). Grafana visualizes these metrics.
*   **API:** A FastAPI service providing access to current anomaly status and risk scores.
*   **Deployment:** Fully containerized with Docker, deployable via `docker-compose` or Kubernetes manifests.

## Running Locally with Docker Compose

1.  Make sure you have Docker and Docker Compose installed.
2.  Run the stack:
    ```bash
    docker-compose up --build
    ```
3.  Access the services:
    *   **FastAPI Service:** http://localhost:8080/metrics/current (View real-time anomaly scores for `P-001`)
    *   **Prometheus:** http://localhost:9090
    *   **Grafana:** http://localhost:3000 (Login: admin / admin)

## Setting up Grafana

1.  Log in to Grafana at `http://localhost:3000`.
2.  Add a Data Source: Connections > Add new connection > Prometheus. Set URL to `http://prometheus:9090`. Save & Test.
3.  Create a Dashboard and add panels to visualize the following metrics:
    *   `health_anomaly_score`: Dynamic risk score.
    *   `health_is_anomaly`: Binary flag (1 if an anomaly is detected).
    *   `health_data_drift_hr`: Tracks drift in the baseline heart rate mean.

## Kubernetes Deployment

The project includes a unified Kubernetes manifest file for cluster deployment.

1.  To deploy to an existing Kubernetes cluster:
    ```bash
    # Ensure you are in the health-stream-ml directory
    kubectl apply -f k8s/all-in-one.yaml
    ```
    *(Note: You will need to build and push the Docker images for `producer`, `model-service`, and `api-service` to a container registry and update the `image` fields in `k8s/all-in-one.yaml` before applying.)*
