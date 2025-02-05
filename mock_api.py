from flask import Flask, request, jsonify

app = Flask(__name__)

#This is what we will be implementing in Polaris

# Mock /functions POST endpoint
@app.route("/functions", methods=["POST"])
def create_function():
    data = request.json
    return jsonify({"success": True, "function": data})

# Mock /functions GET endpoint
@app.route("/functions", methods=["GET"])
def list_functions():
    return jsonify([{"name": "func1"}, {"name": "func2"}])

if __name__ == "__main__":
    app.run(host="localhost", port=5000)  # Run on localhost
