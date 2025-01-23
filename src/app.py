from flask import Flask, request, jsonify, make_response
from flask_cors import CORS
from chatbotcontroller import ChatbotController

app = Flask(__name__)

# Enable CORS with specific origin
CORS(app, resources={r"/*": {"origins": "http://52.91.218.191:7410"}}, supports_credentials=True)

controller = ChatbotController()

@app.route('/query/user_prompt', methods=['POST', 'OPTIONS'])
def user_prompt():
    # Handle preflight OPTIONS request
    if request.method == 'OPTIONS':
        response = make_response('', 200)
        response.headers['Access-Control-Allow-Origin'] = 'http://52.91.218.191:7410'
        response.headers['Access-Control-Allow-Methods'] = 'POST, GET, OPTIONS'
        response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization'
        return response

    # Handle POST request
    return controller.process_user_input()

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=7400)
