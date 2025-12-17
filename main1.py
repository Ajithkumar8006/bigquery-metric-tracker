from flask import Flask, jsonify
import metric_runner1

app = Flask(__name__)

@app.route("/")
def run_queries_and_publish_metrics():
    project_id = "ajith-8006"

    queries = metric_runner1.load_queries("queries.yaml")
    results = []

    for query_data in queries:
        result = metric_runner1.run_query_and_publish_metric(
            query_name=query_data["name"],
            metric_type=query_data["metric_type"],
            query=query_data["query"],
            project_id=project_id   # ✅ FIXED
        )
        results.append(result)

    return jsonify(results)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
