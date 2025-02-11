import re
import string
import base64
import traceback
import io
from flask import request, jsonify
import matplotlib.pyplot as plt

# Import your Python agent creator and display helper
from chatbotService import ChatbotAgentService
from helper import display_python_code_plots

# Import SQLDatabase from langchain_community.utilities
from langchain_community.utilities import SQLDatabase
# Import AIMessage for constructing workflow input messages
from langchain_core.messages import AIMessage


class ChatbotController:
    def __init__(self):
        """
        Initialize the Python agent and a dictionary for managing chat histories.
        The SQL query agent will be built using a LangGraph workflow on demand.
        """
        self.chat_histories = {}

    def process_user_input(self):
        """
        Handle user input by using a LangGraph workflow for SQL querying,
        then (if needed) invoking the Python agent to generate visualizations.
        """
        try:
            data = request.json
            user_input = data.get("input", "")
            connectionString = "mysql://root:jareer@localhost:3306/autobench"

            print("Received user input:", user_input)  # Debugging line

            if not user_input:
                return jsonify({"error": "Input is required."}), 400

            chatbotAgentService = ChatbotAgentService()
            db = SQLDatabase.from_uri(connectionString)
            listTablesTool, getSchemaTool, _ = chatbotAgentService.createSqlToolkitTools(db)
            dbQueryTool = chatbotAgentService.createDbQueryTool(db)
            queryCheck = chatbotAgentService.createQueryCheckFunction(dbQueryTool)
            workflow = chatbotAgentService.createWorkflow(
                listTablesTool,
                getSchemaTool,
                dbQueryTool,
                queryCheck,
                ChatbotAgentService.createToolNodeWithFallback,
            )
            pythonAgent = chatbotAgentService.createAgentForPython()

            keywords = ["plot", "graph", "chart", "diagram", "bar", "visualization"]
            if any(token in user_input.lower() for token in keywords):
                reformatQuery=chatbotAgentService.reformatPromptForSql(user_input)
                print('reformatted query is ',reformatQuery)
                initial_message = AIMessage(content=reformatQuery, tool_calls=[])
                state = {"messages": [initial_message]}

                print("State before workflow invocation:", state)

                workflow_result = workflow.invoke(state, config={"recursion_limit": 50})
                print('workflow result is ',workflow_result)
                last_message_tuple = workflow_result['messages'][-1]
                print('last AI message is ', last_message_tuple)
                sql_output = last_message_tuple.tool_calls[0]['args']['final_answer']
                print(sql_output)  # Debugging line

                if not sql_output:
                    return jsonify({"error": "SQL workflow did not return a result."}), 500

                prompt = {
                    "input": (
                        f"Write Python code to plot the following data:\n\n{sql_output}\n\n"
                        f"Note: This query was generated from the user input: {user_input}"
                    ),
                    "history": []
                }
                python_result = pythonAgent.invoke(prompt)
                python_output = python_result.get("output", "")

                print("Python Generated Code:", python_output)  # Debugging line

                plot_image_base64 = display_python_code_plots(python_output)
                if plot_image_base64:
                    response=chatbotAgentService.formatResponse(plot_image_base64)
                    print('respose is ',response)
                    return jsonify({"text": "", "plot_image": response}), 200
                else:
                    return jsonify({"error": "Failed to generate visualization."}), 200

            else:
                initial_message = AIMessage(
                    content=user_input,
                    tool_calls=[]
                )
                state = {"messages": [initial_message]}

                print("State before workflow invocation:", state)  # Debugging line

                workflow_result = workflow.invoke(state, config={"recursion_limit": 50})
                print('workflow result ',workflow_result)
                last_message_tuple = workflow_result['messages'][-1]
                print('last AI message is ',last_message_tuple)
                sql_output = last_message_tuple.tool_calls[0]['args']['final_answer']
                print(sql_output)
                response = chatbotAgentService.formatResponse(sql_output)
                print('respose is ', response)

                return jsonify({"text": response}), 200

        except Exception as e:
            print("Exception occurred:", str(e))  # Debugging line
            print(traceback.format_exc())  # Debugging line
            return jsonify({
                "error": str(e),
                "traceback": traceback.format_exc()
            }), 500

    def get_chat_history(self, session_id):
        """
        Retrieve the chat history for a given session.
        """
        return self.chat_histories.get(session_id, [])

    def update_chat_history(self, session_id, message):
        """
        Append a message to the chat history for a given session.
        """
        if session_id not in self.chat_histories:
            self.chat_histories[session_id] = []
        self.chat_histories[session_id].append(message)