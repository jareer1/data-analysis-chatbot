from flask import Flask
from chatbotcontroller import ChatbotController
from flask_cors import CORS  # Import the CORS module

# Initialize the Flask app
app = Flask(__name__)
CORS(app)

# Instantiate the controller
controller = ChatbotController()

# Add routes to the app
app.add_url_rule('/query/user_prompt', 'user_prompt', controller.process_user_input, methods=['POST'])

if __name__ == "__main__":
    # Run the Flask app
    app.run(debug=True, host='0.0.0.0', port=5000)