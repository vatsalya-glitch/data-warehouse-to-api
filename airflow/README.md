# Airflow Orchestration for Data Warehouse to API Pipeline

Production-ready Airflow DAG implementation for the three-phase batch pipeline pattern.

## Architecture

```
├── dags/
│   └── warehouse_to_api_dag.py      # Main DAG (Phase 1 → 2 → 3)
├── config.py                         # Configuration loader
├── warehouse_ops.py                  # BigQuery operations
├── serving_db_ops.py                 # PostgreSQL operations
└── requirements.txt                  # Python dependencies
```

## DAG Structure

The DAG implements three sequential phases, each with hard gates:

### Phase 1: Preflight
- Validate schemas before any data writes
- Dry-run queries to catch schema mismatches early
- Check column coverage across domains
- **Fail before Phase 2 if checks don't pass**

### Phase 2: Build
- Build each domain table (TRUNCATE + INSERT)
- Deduplicate with window functions
- Data quality gate: check driving domain non-empty and unique keys
- Assemble final denormalized lookup table
- **Fail before Phase 3 if quality checks don't pass**

### Phase 3: Serve
- Export lookup table to object storage (sharded)
- Import shards in parallel into staging table
- Build indexes on staging table
- Atomic rename-swap (staging → live)
- Reconcile row counts between warehouse and serving DB
- Keep previous table for rollback safety

## Setup

### 1. Install Airflow

```bash
pip install -r requirements.txt
```

### 2. Configure Airflow

Set environment variables:
```bash
export AIRFLOW_HOME=/opt/airflow
export AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql://airflow:password@localhost/airflow
```

Initialize database:
```bash
airflow db init
airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com
```

### 3. Configure Connections

Add BigQuery connection in Airflow UI or via CLI:
```bash
airflow connections add 'google_cloud_default' \
  --conn-type 'google_cloud_platform' \
  --conn-extra '{"extra__google_cloud_platform__key_path": "/path/to/service-account-key.json"}'
```

Add PostgreSQL connection:
```bash
airflow connections add 'postgres_serving_db' \
  --conn-type 'postgres' \
  --conn-host 'localhost' \
  --conn-port 5432 \
  --conn-schema 'lookups' \
  --conn-login 'postgres_user' \
  --conn-password 'password'
```

### 4. Place Configuration

Copy your `config.yaml` to Airflow's config directory:
```bash
cp config/config.example.yaml /opt/airflow/config/config.yaml
# Edit with your actual values
```

### 5. Place SQL Queries

Create SQL query files that the DAG expects:
```
/opt/airflow/sql/
├── domains/
│   ├── customer_core.sql
│   ├── customer_engagement.sql
│   └── customer_activity.sql
├── assembly.sql               # Final lookup assembly
└── cleanup.sql               # Optional: cleanup queries
```

Example `domains/customer_core.sql`:
```sql
SELECT 
  customer_id,
  customer_name,
  email,
  phone,
  address,
  signup_date,
  CURRENT_DATE() as _run_date
FROM `project.dataset.raw_customers`
WHERE customer_id IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY customer_id
  ORDER BY updated_at DESC
) = 1
```

Example `assembly.sql`:
```sql
SELECT
  c.customer_id,
  c.customer_name,
  c.email,
  c.phone,
  c.address,
  c.signup_date,
  e.tier,
  e.total_spent,
  a.last_order_date,
  CURRENT_TIMESTAMP() as _loaded_at
FROM `project.dataset.domain_customer_core` c
LEFT JOIN `project.dataset.domain_customer_engagement` e
  ON c.customer_id = e.customer_id
LEFT JOIN `project.dataset.domain_customer_activity` a
  ON c.customer_id = a.customer_id
WHERE c.customer_id IS NOT NULL
```

## Running the DAG

### Start Airflow

```bash
# Terminal 1: Scheduler
airflow scheduler

# Terminal 2: Webserver
airflow webserver --port 8080
```

Access UI at http://localhost:8080

### Trigger DAG

Via UI: Click "Trigger DAG" button

Via CLI:
```bash
airflow dags trigger warehouse_to_api_pipeline
```

### Monitor Execution

Watch task logs in the Airflow UI, or tail logs:
```bash
tail -f /opt/airflow/logs/warehouse_to_api_pipeline/*/*/
```

## Configuration

Edit `config/config.yaml` to customize:

```yaml
warehouse:
  type: bigquery
  project_id: "my-project"
  dataset_id: "analytics"

serving_db:
  type: postgres
  host: "postgres.example.com"
  port: 5432
  database: "lookups"

lookup_table:
  name: "customers_lookup"
  primary_key: "customer_id"
  expected_columns:
    - customer_id
    - customer_name
    - email
    - ...

domains:
  customer_core:
    source_table: "raw_customers"
    intermediate_name: "domain_customer_core"
    ...

data_quality:
  min_rows: 100
  unique_primary_key: true
  row_count_change_tolerance: 0.15

scheduling:
  run_time: "02:00"
  frequency: "daily"
```

## Error Handling & Alerts

### Retry Policy
- Default: 1 retry with 5-minute delay
- Configurable via `default_args`

### Alerting
Set email alerts in `config.yaml`:
```yaml
monitoring:
  alert_email: "data-eng@example.com"
  alert_on_failure: true
  alert_if_duration_exceeds_minutes: 30
```

### Debugging

Check task logs in Airflow UI:
1. Click DAG name
2. Click task
3. View logs

Common issues:
- **"Connection not found"**: Verify Airflow connections are configured
- **"SQL file not found"**: Check `/opt/airflow/sql/` paths
- **"Data quality check failed"**: Review warehouse data and thresholds
- **"Import failed"**: Check serving DB credentials and capacity

## Advanced: Custom Operators

For more control, you can create custom operators:

```python
from airflow.models import BaseOperator

class BigQueryDryRunOperator(BaseOperator):
    """Custom operator for BigQuery dry-run validation."""
    
    def __init__(self, sql_file, **kwargs):
        super().__init__(**kwargs)
        self.sql_file = sql_file
    
    def execute(self, context):
        # Implement dry-run logic
        pass
```

## Testing

Unit test the Python modules:

```bash
# Test config loading
python -c "from airflow.config import PipelineConfig; c = PipelineConfig('config.yaml'); print(c.warehouse)"

# Test warehouse ops
python -c "from airflow.warehouse_ops import BigQueryWarehouseOps; ..."
```

## Deployment

### Docker

Use Airflow's official Docker Compose:
```bash
curl -LfO 'https://airflow.apache.org/docker-compose.yaml'
docker-compose up -d
```

### Kubernetes

Deploy using Airflow's Helm chart:
```bash
helm install airflow apache-airflow/airflow
```

### Cloud Composer (Google Cloud)

Create a Composer environment:
```bash
gcloud composer environments create warehouse-to-api \
  --location us-central1 \
  --python-version 3 \
  --machine-type n1-standard-4
```

Upload DAG and dependencies:
```bash
gcloud composer environments storage dags import \
  --environment warehouse-to-api \
  --location us-central1 \
  --source airflow/dags/warehouse_to_api_dag.py
```

## Monitoring & Operations

### Key Metrics to Track

- **Pipeline duration**: Alert if > `alert_if_duration_exceeds_minutes`
- **Row count change**: Should be within `row_count_change_tolerance`
- **Task failures**: Retry logic and email alerts
- **Data freshness**: Last successful run timestamp

### Manual Intervention

**To rollback to previous version:**
```sql
-- In Postgres
BEGIN;
ALTER TABLE customers_lookup RENAME TO customers_lookup_broken;
ALTER TABLE customers_lookup_old RENAME TO customers_lookup;
COMMIT;
```

**To skip a domain temporarily:**
Edit `config.yaml` and disable domain:
```yaml
domains:
  customer_core:
    enabled: true
  customer_engagement:
    enabled: false  # Skip this domain
```

## References

- [Airflow Documentation](https://airflow.apache.org/docs/)
- [BigQuery Airflow Provider](https://airflow.apache.org/docs/apache-airflow-providers-google/)
- [PostgreSQL Airflow Provider](https://airflow.apache.org/docs/apache-airflow-providers-postgres/)
