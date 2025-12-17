from google.cloud import bigquery
from google.cloud import monitoring_v3
from datetime import datetime, timezone
import yaml

def load_queries(file_path="queries.yaml"):
    """Load queries from the queries.yaml file."""
    with open(file_path, "r") as file:
        data = yaml.safe_load(file)
    return data['metrics']  # Return the metrics list

def run_query_and_publish_metric(query_name, metric_type, query, project_name):
    """Run a BigQuery query and publish the result as a custom metric to Google Cloud Monitoring."""
    bq_client = bigquery.Client(project=project_name)
    monitoring_client = monitoring_v3.MetricServiceClient()

    try:
        # Execute the BigQuery query
        query_results = list(bq_client.query(query).result())
        
        if query_results:
            # Get the first field name dynamically and its value
            field_name = list(query_results[0].keys())[0]
            value = query_results[0][field_name]
        else:
            value = 0

        # Create the TimeSeries data for Cloud Monitoring
        now = datetime.now(timezone.utc)
        series = monitoring_v3.TimeSeries()
        series.metric.type = metric_type
        series.resource.type = "global"

        point = monitoring_v3.Point(
            interval=monitoring_v3.TimeInterval(end_time=now),
            value=monitoring_v3.TypedValue(int64_value=value)
        )
        series.points = [point]

        # Publish the metric
        monitoring_client.create_time_series(name=f"projects/{project_name}", time_series=[series])

        # Return the result
        return {
            "metric": query_name,
            "metric_type": metric_type,
            "published": True,
            "timestamp": now.isoformat(),
            "value": value
        }

    except Exception as e:
        # Handle errors and return a result indicating failure
        now = datetime.now(timezone.utc)
        return {
            "metric": query_name,
            "metric_type": metric_type,
            "published": False,
            "timestamp": now.isoformat(),
            "value": 0,
            "error": str(e)
        }
