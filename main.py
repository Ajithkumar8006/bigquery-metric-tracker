from flask import Flask, jsonify
import metric_runner

app = Flask(__name__)

@app.route("/")
def run_queries_and_publish_metrics():
    # Google Cloud Project Name
    project_name = "ajith-8006"

    # Load the list of queries and metrics from queries.yaml
    queries = metric_runner.load_queries("queries.yaml")
    
    results = []
    
    # Loop through the metrics and execute each query
    for query_data in queries:
        query_name = query_data['name']
        metric_type = query_data['metric_type']
        query = query_data['query']

        # Run the query and publish the metric
        result = metric_runner.run_query_and_publish_metric(query_name, metric_type, query, project_name)
        results.append(result)
    
    # Return the results as JSON
    return jsonify(results)

if __name__ == "__main__":
    app.run(debug=True)
