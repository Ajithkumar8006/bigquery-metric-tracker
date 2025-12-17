from google.cloud import bigquery
from google.cloud import monitoring_v3
from google.api import metric_pb2
from datetime import datetime, timezone
import yaml

def load_queries(file_path="queries.yaml"):
    """Load metrics queries from YAML."""
    with open(file_path, "r") as file:
        data = yaml.safe_load(file)
    return data["metrics"]

def create_metric_descriptor(project_id, metric_type):
    """Create custom metric descriptor in Cloud Monitoring if not exists."""
    client = monitoring_v3.MetricServiceClient()
    project_name = f"projects/{project_id}"

    descriptor = metric_pb2.MetricDescriptor(
        type=metric_type,
        metric_kind=metric_pb2.MetricDescriptor.MetricKind.GAUGE,
        value_type=metric_pb2.MetricDescriptor.ValueType.DOUBLE,
        description="BigQuery derived error metric",
        display_name=metric_type.split("/")[-1],
    )

    try:
        client.create_metric_descriptor(
            name=project_name,
            metric_descriptor=descriptor
        )
        print(f"Metric descriptor created: {metric_type}")
    except Exception:
        # Metric already exists
        pass

def run_query_and_publish_metric(query_name, metric_type, query, project_id):
    """Run BigQuery query and publish the result as custom metric."""
    bq_client = bigquery.Client(project=project_id)
    monitoring_client = monitoring_v3.MetricServiceClient()

    try:
        # Ensure metric descriptor exists
        create_metric_descriptor(project_id, metric_type)

        # Execute the query
        rows = list(bq_client.query(query).result())
        value = float(rows[0]["value"]) if rows else 0.0

        now = datetime.now(timezone.utc)

        # Prepare TimeSeries for Cloud Monitoring
        series = monitoring_v3.TimeSeries()
        series.metric.type = metric_type
        series.resource.type = "global"

        point = monitoring_v3.Point(
            interval=monitoring_v3.TimeInterval(end_time=now),
            value=monitoring_v3.TypedValue(double_value=value)
        )
        series.points = [point]

        # Publish metric
        monitoring_client.create_time_series(
            name=f"projects/{project_id}",
            time_series=[series]
        )

        return {
            "metric": query_name,
            "metric_type": metric_type,
            "published": True,
            "value": value,
            "timestamp": now.isoformat()
        }

    except Exception as e:
        return {
            "metric": query_name,
            "metric_type": metric_type,
            "published": False,
            "value": 0,
            "error": str(e),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
