Below is the complete set of file contents for your GitHub repository Myrepo, based on the folder structure above. Copy each block into the corresponding file.

---

📁 .gitignore (root)

```gitignore
# Python
__pycache__/
*.py[cod]
*.so
.Python
env/
venv/
.env

# Go
*.exe
*.exe~
*.dll
*.so
*.dylib
*.test
*.out

# Terraform
*.tfstate
*.tfstate.*
.terraform/

# IDE
.vscode/
.idea/

# OS
.DS_Store
Thumbs.db

# Secrets / sensitive
*.pem
*.key
secrets/
```

---

📁 README.md (root)

```markdown
# Myrepo – Compliance Platform Prompts & Code

This repository contains **master prompts** for building a multi‑entity compliance platform (capturing API requests/responses, batch jobs, UI events) with PHI/PII handling, 5M+ events/day per entity, and deployment on OpenShift, AWS ECS, or on‑prem.

## Folder Structure

- `prompts/compliance-platform/` – All prompts for the compliance platform.
- `templates/` – Reusable prompt templates.

## How to Use

1. Clone this repo to your office machine.
2. Open `prompts/compliance-platform/unified-prompt.md` in Cursor.
3. Paste into Cursor Composer → generate the entire platform.
4. Or use individual component prompts in `components/`.

## License

Internal use only.
```

---

📁 prompts/compliance-platform/README.md

```markdown
# Compliance Platform Prompts

Use these prompts with **Cursor** (or any AI coding assistant) to generate a production‑ready compliance platform.

## Quick Start

- **Unified prompt** – `unified-prompt.md` generates everything at once.
- **Component prompts** – `components/` folder contains smaller, focused prompts.

## Best Practices

- Review generated code before deploying.
- Customise environment variables and resource names.
- Run security scans (Trivy, etc.) on all Docker images.
```

---

📁 prompts/compliance-platform/unified-prompt.md

```markdown
# Unified Prompt: Multi‑Entity Compliance Platform for API, Batch, and UI Applications

We need a complete compliance platform that captures API requests/responses, batch job events, and UI application events. The platform must handle PHI/PII, support 5M events/day per entity (drug, clinical, comm restrictions), and run on OpenShift, AWS ECS, or on‑prem VMs.

## Core Architecture Decisions

1. **Capture patterns**:
   - HTTP APIs: Use Envoy sidecar (intercept traffic, async publish to Kafka). No code changes.
   - Batch/UI apps: Use a local HTTP agent (Go binary) listening on localhost:9090. Any language calls `POST /v1/record`. Agent buffers to disk and publishes to Kafka asynchronously.
   - AWS Lambda: Provide native library (Python/Node/Java) or send to Kinesis Firehose.

2. **Unified event bus**: Single Kafka topic `compliance.events` with 150 partitions, key = `{entity_type}_{consumer_name}`, retention 7 days, idempotent producer, acks=all, compression=lz4.

3. **Compliance consumer** (Python):
   - Polls batches, hashes patient_id (HMAC-SHA256 with per‑consumer pepper from Secrets Manager).
   - Uploads raw payload to S3 (SSE‑KMS, Object Lock, WORM, 7‑year lifecycle).
   - Inserts metadata into Aurora PostgreSQL (partitioned by date, indexed on entity_type+event_id, consumer_name+timestamp, patient_hash, searchable_attributes JSONB).
   - Writes hot cache to MongoDB (7‑day TTL).
   - Commits offsets after success; DLQ for failures.

4. **Retrieval API** (FastAPI):
   - JWT authentication (extract consumer_name, role).
   - Query by event_id, patient_id (hashed), entity_type, date range.
   - Returns presigned S3 URLs (15‑min expiry).
   - <200ms latency.

5. **Archival & analytics**: Kinesis Firehose (or Kafka Connect) writes Parquet to S3 archive. Glue + Athena for ad‑hoc queries.

6. **Deployment**:
   - Sidecar: Kubernetes sidecar container (OpenShift) or ECS task sidecar or systemd (on‑prem).
   - Local agent: Deploy as DaemonSet (K8s), sidecar (ECS), or systemd service (on‑prem).
   - Consumer & retrieval API: Deploy on EC2/ECS (Docker Compose or ECS service).
   - CI/CD: Jenkins pipeline builds images, scans with Trivy, pushes to Quay, deploys.

## What to Generate (in order)

1. **Local HTTP Agent** (Go or Rust) – binary that:
   - Listens on `:9090`, endpoint `POST /v1/record`.
   - Accepts JSON: `{entity_type, event_id, consumer_name, payload, patient_id, timestamp}`.
   - Async Kafka producer (Sarama or rdkafka) with idempotence, batching, compression.
   - Disk buffer (SQLite or file) if Kafka down.
   - Health `/health` and metrics `/metrics`.
   - Dockerfile, systemd unit, and Kubernetes DaemonSet YAML.

2. **Envoy sidecar configuration** (with external gRPC filter or Lua) to capture HTTP request/response and publish to same Kafka topic (same schema as agent). Provide Kubernetes Deployment snippet and ECS task definition.

3. **Compliance consumer** (Python) – full code with async Kafka, S3 upload, Aurora batch insert, MongoDB cache, deduplication, DLQ, CloudWatch metrics.

4. **Retrieval API** (FastAPI) – JWT validation, DB queries, presigned S3 URLs, pagination.

5. **Aurora DDL** – partitioned table, indexes, RLS, pg_partman setup.

6. **MongoDB indexes** – TTL index, compound indexes.

7. **Kafka topic creation script** (CLI or Terraform).

8. **Kinesis Firehose + Glue + Athena** – Terraform module.

9. **Jenkinsfile** (declarative) for building, scanning, deploying all components.

10. **Deployment manifests** for OpenShift (DeploymentConfig with sidecar), ECS (task definition), on‑prem (docker-compose + systemd).

All code must be production‑ready, with error handling, retries, logging, and observability. Provide clear README with environment variables and setup steps.
```

---

📁 prompts/compliance-platform/components/01-aurora-schema.sql

```sql
-- Aurora PostgreSQL DDL for compliance_metadata table
-- Run this on your Aurora cluster

CREATE TABLE IF NOT EXISTS compliance_metadata (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_type VARCHAR(50) NOT NULL,
    event_id VARCHAR(255) NOT NULL,
    consumer_name VARCHAR(100) NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    patient_hash BYTEA,
    s3_payload_path TEXT NOT NULL,
    searchable_attributes JSONB,
    created_at TIMESTAMPTZ DEFAULT now()
) PARTITION BY RANGE (event_timestamp);

-- Indexes
CREATE INDEX idx_entity_event ON compliance_metadata (entity_type, event_id);
CREATE INDEX idx_consumer_time ON compliance_metadata (consumer_name, event_timestamp);
CREATE INDEX idx_patient_hash ON compliance_metadata (patient_hash) WHERE patient_hash IS NOT NULL;
CREATE INDEX idx_searchable_gin ON compliance_metadata USING GIN (searchable_attributes);

-- Row Level Security
ALTER TABLE compliance_metadata ENABLE ROW LEVEL SECURITY;
CREATE POLICY consumer_isolation ON compliance_metadata
    USING (consumer_name = current_setting('app.current_consumer'));

-- Example: Create daily partition for next 30 days (use pg_partman for automation)
DO $$
DECLARE
    d date;
BEGIN
    FOR d IN SELECT generate_series(current_date, current_date + interval '30 days', '1 day'::interval) LOOP
        EXECUTE format('CREATE TABLE IF NOT EXISTS compliance_metadata_%s PARTITION OF compliance_metadata FOR VALUES FROM (%L) TO (%L)',
            to_char(d, 'YYYY_MM_DD'), d, d + 1);
    END LOOP;
END $$;
```

---

📁 prompts/compliance-platform/components/02-kafka-topic-config.sh

```bash
#!/bin/bash
# Kafka topic creation script for MSK / Confluent / open-source Kafka

TOPIC_NAME="compliance.events"
PARTITIONS=150
REPLICATION_FACTOR=3
RETENTION_MS=604800000  # 7 days

kafka-topics.sh --bootstrap-server $BOOTSTRAP_SERVERS \
  --create \
  --topic $TOPIC_NAME \
  --partitions $PARTITIONS \
  --replication-factor $REPLICATION_FACTOR \
  --config retention.ms=$RETENTION_MS \
  --config cleanup.policy=delete \
  --config min.insync.replicas=2 \
  --config unclean.leader.election.enable=false

echo "Topic $TOPIC_NAME created with $PARTITIONS partitions."
```

---

📁 prompts/compliance-platform/components/03-sidecar-python-proxy.py

```python
# This is a PROMPT, not the final code. Paste into Cursor to generate the proxy.

"""
Generate a Python FastAPI proxy that:

- Listens on port 8080.
- Forwards every request to a backend API (environment variable BACKEND_URL).
- Captures the full request (path, headers, body) and the full response.
- Extracts from headers: X-Consumer-Name, X-Entity-Type, X-Event-ID, X-Patient-ID.
- Asynchronously publishes to Kafka topic `compliance.events` with key = f"{entity_type}_{consumer_name}".
- Uses aiokafka with idempotence=True, acks=all, compression=lz4, linger.ms=20.
- Does NOT wait for Kafka acknowledgement (fire-and-forget).
- Returns the backend response to the client immediately.
- Provides health endpoint GET /health.
- Handles Kafka unavailability by logging and optionally buffering to local disk.
"""
```

---

📁 prompts/compliance-platform/components/04-compliance-consumer.py

```python
# Prompt for the compliance consumer – paste into Cursor.

"""
Generate a Python script (aiokafka, asyncpg, boto3, pymongo) that:

- Consumes from Kafka topic `compliance.events`, consumer group `compliance-storage`.
- Manual offset commit (enable.auto.commit=False).
- Batch size: 500 messages.
- For each message, extract: entity_type, event_id, consumer_name, patient_id (if present), timestamp, payload (full JSON).
- Compute patient_hash = HMAC-SHA256(patient_id + pepper). Retrieve pepper from AWS Secrets Manager, cached per consumer.
- Upload payload to S3: s3://compliance-payloads/{entity_type}/{consumer_name}/{date}/{event_id}.json
  - Use SSE-KMS (KMS key from env), Object Lock in GOVERNANCE mode for 7 years.
- Insert metadata into Aurora PostgreSQL using asyncpg.copy_records_to_table.
- After successful S3+DB, commit offsets.
- Implement deduplication: store processed event_ids in a Redis set or an Aurora table (expire after 7 days).
- On failure after retries, send message to DLQ topic `compliance.events.dlq`.
- Emit CloudWatch metrics: consumer_lag, records_processed, errors.
- Use asyncio, structured logging, graceful shutdown.
"""
```

---

📁 prompts/compliance-platform/components/05-retrieval-api.py

```python
# Prompt for retrieval API – FastAPI.

"""
Generate a FastAPI application with:

- JWT authentication (public key from env). Extract `consumer_name` and `role` from token.
- Role 'compliance_officer' can query any consumer; others restricted to their own.
- Endpoint: GET /compliance/events
  Query params: event_id (optional), patient_id (optional), entity_type (optional), consumer_name (optional), date_from, date_to, limit (default 100), cursor.
- If patient_id provided, compute hash using same pepper (retrieve from Secrets Manager).
- Query MongoDB hot cache first (collection `recent_events`, TTL 7 days). If not found, query Aurora PostgreSQL.
- Use asyncpg connection pool for Aurora.
- For each result, generate a presigned S3 GET URL (expiry 15 minutes) using boto3.
- Return JSON: { "next_cursor": "...", "results": [ { "metadata": {...}, "payload_url": "..." } ] }
- Include OpenAPI docs at /docs.
- Health check endpoint.
"""
```

---

📁 prompts/compliance-platform/components/06-jenkins-pipeline.groovy

```groovy
// Declarative Jenkins pipeline for compliance platform services

pipeline {
    agent any
    environment {
        QUAY_REGISTRY = 'quay.io/your-org'
        AWS_DEFAULT_REGION = 'us-east-1'
    }
    parameters {
        choice(name: 'SERVICE', choices: ['sidecar', 'consumer', 'retrieval-api'], description: 'Service to build')
        string(name: 'ENVIRONMENT', defaultValue: 'dev', description: 'Target environment')
    }
    stages {
        stage('Checkout') {
            steps { checkout scm }
        }
        stage('Unit Tests') {
            steps { sh 'pytest tests/' }
        }
        stage('Build Docker Image') {
            steps {
                script {
                    def imageTag = "${env.BUILD_NUMBER}-${env.GIT_COMMIT.take(7)}"
                    sh "docker build -t ${QUAY_REGISTRY}/${SERVICE}:${imageTag} -f Dockerfile.${SERVICE} ."
                    sh "docker tag ${QUAY_REGISTRY}/${SERVICE}:${imageTag} ${QUAY_REGISTRY}/${SERVICE}:latest"
                }
            }
        }
        stage('Image Scan (Trivy)') {
            steps {
                sh "trivy image --severity HIGH,CRITICAL --exit-code 1 ${QUAY_REGISTRY}/${SERVICE}:${env.BUILD_NUMBER}-${env.GIT_COMMIT.take(7)}"
            }
        }
        stage('Push to Quay') {
            steps {
                withCredentials([string(credentialsId: 'quay-robot-token', variable: 'QUAY_TOKEN')]) {
                    sh "docker login -u robot\$your-org+compliance -p ${QUAY_TOKEN} ${QUAY_REGISTRY}"
                    sh "docker push ${QUAY_REGISTRY}/${SERVICE}:${env.BUILD_NUMBER}-${env.GIT_COMMIT.take(7)}"
                    sh "docker push ${QUAY_REGISTRY}/${SERVICE}:latest"
                }
            }
        }
        stage('Deploy to EC2') {
            when { environment name: 'ENVIRONMENT', value: 'prod' }
            steps {
                sshagent(['ec2-ssh-key']) {
                    sh """
                        ssh ec2-user@${EC2_HOST} "
                            docker pull ${QUAY_REGISTRY}/${SERVICE}:latest &&
                            docker-compose -f /opt/compliance/docker-compose.yml up -d --no-deps ${SERVICE}
                        "
                    """
                }
            }
        }
    }
    post {
        failure {
            slackSend color: 'danger', message: "Pipeline failed for ${SERVICE}"
        }
    }
}
```

---

📁 prompts/compliance-platform/components/07-terraform-main.tf

```hcl
# Terraform configuration for AWS resources (partial example – expand as needed)

terraform {
  required_version = ">= 1.5"
  backend "s3" {
    bucket = "myrepo-tfstate"
    key    = "compliance/terraform.tfstate"
    region = "us-east-1"
  }
}

provider "aws" {
  region = var.aws_region
}

# S3 bucket for payloads (with Object Lock)
resource "aws_s3_bucket" "compliance_payloads" {
  bucket = "compliance-payloads-${var.account_id}"
  force_destroy = false
}

resource "aws_s3_bucket_object_lock_configuration" "payloads_lock" {
  bucket = aws_s3_bucket.compliance_payloads.id
  rule {
    default_retention {
      mode = "GOVERNANCE"
      days = 2555   # 7 years
    }
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "payloads_encrypt" {
  bucket = aws_s3_bucket.compliance_payloads.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.compliance_key.id
    }
  }
}

# Aurora PostgreSQL cluster (Serverless v2)
resource "aws_rds_cluster" "compliance_aurora" {
  cluster_identifier      = "compliance-metadata"
  engine                  = "aurora-postgresql"
  engine_version          = "15.3"
  database_name           = "compliance"
  master_username         = var.db_username
  master_password         = var.db_password
  serverlessv2_scaling_configuration {
    min_capacity = 0.5
    max_capacity = 8
  }
}

# MSK cluster (simplified)
resource "aws_msk_cluster" "compliance_msk" {
  cluster_name           = "compliance-msk"
  kafka_version          = "3.5.0"
  number_of_broker_nodes = 3

  broker_node_group_info {
    instance_type = "kafka.m5.large"
    client_subnets = var.private_subnet_ids
    security_groups = [aws_security_group.msk.id]
  }
}

# Outputs
output "s3_payload_bucket" {
  value = aws_s3_bucket.compliance_payloads.id
}
output "aurora_endpoint" {
  value = aws_rds_cluster.compliance_aurora.endpoint
}
output "msk_bootstrap_brokers" {
  value = aws_msk_cluster.compliance_msk.bootstrap_brokers
}
```

---

📁 prompts/compliance-platform/components/08-local-agent.go

```go
// Prompt for the Go local HTTP agent – paste into Cursor.

/*
Generate a Go program that:

1. Listens on HTTP port 9090.
2. Endpoint: POST /v1/record
   Accepts JSON body: 
   {
     "entity_type": "DRUG",
     "event_id": "req-123",
     "consumer_name": "Acme",
     "patient_id": "p-456",
     "timestamp": "2025-01-01T00:00:00Z",
     "payload": { ... }
   }
3. Immediately returns HTTP 202 Accepted.
4. Uses a Kafka producer (Sarama) with:
   - Idempotence enabled
   - acks=all
   - compression: lz4
   - Batching: linger=20ms, batch_size=16KB
5. If Kafka is unreachable, buffers events to a local SQLite database (or file) and retries on reconnect.
6. Exposes health endpoint GET /health.
7. Exposes metrics endpoint GET /metrics (prometheus format).
8. Can be packaged as Docker image and also run as systemd service.
9. Configuration via environment variables (KAFKA_BROKERS, TOPIC, etc.).
*/
```

---

📁 prompts/compliance-platform/components/09-envoy-sidecar.yaml

```yaml
# Envoy configuration for sidecar (static config with external gRPC filter)

static_resources:
  listeners:
  - name: listener_0
    address:
      socket_address:
        address: 0.0.0.0
        port_value: 8080
    filter_chains:
    - filters:
      - name: envoy.filters.network.http_connection_manager
        typed_config:
          "@type": type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager
          stat_prefix: ingress_http
          codec_type: AUTO
          route_config:
            name: local_route
            virtual_hosts:
            - name: backend
              domains: ["*"]
              routes:
              - match: { prefix: "/" }
                route: { cluster: api_cluster }
          http_filters:
          - name: envoy.filters.http.grpc_http1_reverse_bridge
            typed_config:
              "@type": type.googleapis.com/envoy.extensions.filters.http.grpc_http1_reverse_bridge.v3.FilterConfig
              filter_name: compliance_filter
              upstream_config:
                name: compliance_filter_upstream
                cluster_name: compliance_grpc_cluster
          - name: envoy.filters.http.router
            typed_config:
              "@type": type.googleapis.com/envoy.extensions.filters.http.router.v3.Router

  clusters:
  - name: api_cluster
    connect_timeout: 5s
    type: STRICT_DNS
    load_assignment:
      cluster_name: api_cluster
      endpoints:
      - lb_endpoints:
        - endpoint:
            address:
              socket_address:
                address: api_container
                port_value: 9090

  - name: compliance_grpc_cluster
    connect_timeout: 5s
    type: STRICT_DNS
    load_assignment:
      cluster_name: compliance_grpc_cluster
      endpoints:
      - lb_endpoints:
        - endpoint:
            address:
              socket_address:
                address: localhost
                port_value: 50051
```

---

📁 prompts/compliance-platform/components/10-mongodb-indexes.js

```javascript
// MongoDB indexes for compliance hot cache

// Switch to the compliance database
use compliance_cache;

// TTL index on event_timestamp (7 days)
db.recent_events.createIndex(
  { "event_timestamp": 1 },
  { expireAfterSeconds: 604800 }   // 7 days
);

// Compound index for fast queries
db.recent_events.createIndex(
  { "consumer_name": 1, "patient_hash": 1, "event_timestamp": -1 }
);

// Index for event_id lookups
db.recent_events.createIndex(
  { "event_id": 1 }
);

// Index for entity_type + event_id (for transaction audits)
db.recent_events.createIndex(
  { "entity_type": 1, "event_id": 1 }
);
```

---

📁 templates/prompt-template.md

```markdown
# Prompt Template

**Title**: [Component Name]

**Objective**: [What this component should do]

**Inputs**: [Environment variables, config files, expected schemas]

**Outputs**: [What the generated code should produce]

**Requirements**:
- [Requirement 1]
- [Requirement 2]
- [Requirement 3]

**Example Prompt** (copy this into Cursor):

[Write the actual prompt text here]

**Additional Notes**:
- Any specific libraries or constraints.
```

---

