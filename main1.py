from flask import Flask, jsonify
import metric_runner

app = Flask(__name__)

@app.route("/")
def run_queries_and_publish_metrics():
    project_id = "ajith-8006"  # Your GCP project ID

    # Load queries from YAML
    queries = metric_runner.load_queries("queries.yaml")
    results = []

    for query_data in queries:
        result = metric_runner.run_query_and_publish_metric(
            query_name=query_data["name"],
            metric_type=query_data["metric_type"],
            query=query_data["query"],
            project_id=project_id
        )
        results.append(result)

    return jsonify(results)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)
