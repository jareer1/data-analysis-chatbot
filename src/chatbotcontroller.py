import re
import string
import base64
from flask import request, jsonify
import traceback
import io
from agent import create_agent_for_sql, create_agent_for_python
import matplotlib.pyplot as plt
from Services.backendServices import BackendServices
from helper import display_python_code_plots


class ChatbotController:
    def __init__(self):
        """
        Initialize the SQL, Python, and Orchestrator agents when the class is instantiated,
        and create a dictionary to manage chat histories.
        """
        self.python_agent = create_agent_for_python(agent_llm_name="gpt-4o")
        self.chat_histories = {}

    def process_user_input(self):
        """
        Handles user input by coordinating between SQL and Python agents,
        generating plots if necessary, and sending results to the frontend.
        """
        try:
            data = request.json
            # session_id = data.get('sessionId', None)
            user_input = data.get('input', '')
            connectionString = data.get('connectionString', '')
            print('connection string is ',connectionString)
            # if not session_id:
            #     return jsonify({"error": "sessionId is required."}), 400
            if not user_input:
                return jsonify({"error": "Input is required."}), 400

            # Initialize chat history for the session if not already present
            # if session_id not in self.chat_histories:
            #     self.chat_histories[session_id] = []
            #
            # # Step 1: Append user input to chat history
            # self.chat_histories[session_id].append({"role": "user", "content": user_input})

            # Step 2: Check if visualization is required
            keywords = ["plot", "graph", "chart", "diagram", "bar", "visualization"]
            if any(token in user_input.lower() for token in keywords):
                # SQL agent processing with chat history
                sql_payload = {"input": user_input,
                               # "history": self.chat_histories[session_id]
                               }
                sql_agent = create_agent_for_sql(tool_llm_name="gpt-4o", agent_llm_name="gpt-4o",
                                                 connectionString=connectionString
                                                 )
                print("Connection String:", connectionString)

                print(sql_payload)

                sql_result = sql_agent.invoke(sql_payload)

                sql_output = sql_result.get("output", "")
                if not sql_output:
                    return jsonify({"error": "SQL agent did not return a result."}), 500

                # Python agent processing for visualization with chat history
                prompt = {
                    "input": f"Write a code in Python to plot the following data:\n\n{sql_output}",
                    "history": []
                        # self.chat_histories[session_id],
                }
                python_result = self.python_agent.invoke(prompt)
                python_output = python_result.get("output", "")

                # Extract and execute Python code to generate the plot
                plot_image_base64 = display_python_code_plots(python_output)
                if plot_image_base64:
                    response_data = {
                        "text": "",
                        "plot_image": plot_image_base64,  # Base64 string of the image
                    }
                    return jsonify(response_data), 200

                # If no valid plot generated
                return jsonify({"error": "Failed to generate visualization."}), 500

            else:
                # SQL agent processing with chat history
                sql_payload = {"input": user_input,
                               # "history": self.chat_histories[session_id]
                               }
                sql_agent = create_agent_for_sql(tool_llm_name="gpt-4o", agent_llm_name="gpt-4o",
                                                 connectionString=connectionString
                                                 )
                print("Connection String:", connectionString)

                sql_result = sql_agent.invoke(sql_payload)

                # Ensure response_data is a dictionary, even if the SQL result is a string
                response_data = sql_result.get("output", "")

                if not response_data:
                    return jsonify({"error": "SQL agent did not return a result."}), 500

                # If response_data is a string, convert it into a dictionary
                if isinstance(response_data, str):
                    response_data = {"text": response_data}

                # Append SQL agent response to chat history
                # self.update_chat_history(session_id, {"role": "sql_agent", "content": response_data["text"]})

                # Format response
                return jsonify(response_data), 200

        except Exception as e:
            return jsonify({
                "error": str(e),
                "traceback": traceback.format_exc()
            }), 500

    def get_chat_history(self, session_id):
        """
        Retrieve chat history for a specific session.

        Args:
            session_id (str): The session ID.

        Returns:
            list: The chat history for the given session.
        """
        return self.chat_histories.get(session_id, [])

    def update_chat_history(self, session_id, message):
        """
        Update chat history for a specific session.

        Args:
            session_id (str): The session ID.
            message (dict): The message to append to the chat history.
        """
        if session_id not in self.chat_histories:
            self.chat_histories[session_id] = []
        self.chat_histories[session_id].append(message)

    def connect_to_sql(self):
        try:
            data = request.json
            db_type = data.get("db_type")
            username = data.get("username")
            password = data.get("password")
            host = data.get("host")
            port = data.get("port")
            database_name = data.get("database_name")

            if not all([db_type, username, password, host, port, database_name]):
                return jsonify({"error": "All connection details  are required."}), 400
            backendServices=BackendServices()
            connection_string = backendServices.getSqlData(
                db_type=db_type,
                username=username,
                password=password,
                host=host,
                port=port,
                database_name=database_name
            )
            print("Connection String:", connection_string)


            # Return the query result
            return "successfully connected",200

        except Exception as e:
            return jsonify({
                "error": str(e),
                "traceback": traceback.format_exc()
            }), 500
