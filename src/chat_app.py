import os
import warnings
import sys
import streamlit as st
import unidecode

from helper import display_python_code_plots, display_text_with_images
from agent import create_agent_for_python, create_agent_for_sql

warnings.filterwarnings("ignore")
current_dir = os.path.dirname(os.path.abspath(__file__)) # Get the directory of the current file

# Define the path relative to the current file
# For example, if the directory to add is the parent directory of the current file
parent_dir = os.path.join(current_dir, "..")

# Add the parent directory to sys.path
sys.path.insert(0, parent_dir)
os.environ['OPENAI_API_KEY'] = "enter-your-key"

st.set_page_config(page_title="ProjectPro Query Based Analytics")

if 'agent_memory' not in st.session_state:
    st.session_state['agent_memory_sql'] = create_agent_for_sql()
    st.session_state['agent_memory_python'] = create_agent_for_python()


def generate_response(code_type, input_text):
    """
    Generate a response based on the provided input text and code type.

    This function takes input text and a code type (e.g., "python" or "sql") and generates a response
    using corresponding agents for the given code type.

    Args:
        code_type (str): The type of code to be generated ("python" or "sql").
        input_text (str): The input text to be processed.

    Returns:
        str: The generated response based on the input text and code type. If no response is generated,
        it returns "NO_RESPONSE".
    """
    prompt = unidecode.unidecode(input_text)
    if code_type == "python":
        try:
            response = st.session_state.sql_agent.invoke({"input": prompt})['output']
            print("Response->", response)
        except:
            return "NO_RESPONSE"
        keywords = ["please provide", "don't know", "more context", "provide more", "vague request"]
        if any(token in response.lower() for token in keywords):
            return "NO_RESPONSE"
        prompt = {"input": "Write a code in python to plot the following data\n\n" + response}
        return st.session_state.python_agent.invoke(prompt)
    else:
        return st.session_state.sql_agent.run(prompt)


def reset_conversation():
    st.session_state.messages = []
    st.session_state.sql_agent = create_agent_for_sql()
    st.session_state.python_agent = create_agent_for_python()


# Initialize chat history
if "messages" not in st.session_state:
    st.session_state.messages = []

if "agent" not in st.session_state:
    st.session_state.sql_agent = st.session_state['agent_memory_sql']
    st.session_state.python_agent = st.session_state['agent_memory_python']


st.title("ProjectPro Query Based Analytics")
col1, col2 = st.columns([3, 1])
with col2:
    st.button("Reset Chat", on_click=reset_conversation)

# Display chat messages from history on app rerun
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        if message["role"] in ("assistant", "error"):
            display_text_with_images(message["content"])
        elif message["role"] == "plot":
            exec(message["content"])
        else:
            st.markdown(message["content"])


# Accept user input
if prompt := st.chat_input("Please ask your question:"):
    # Display user message in chat message container
    with st.chat_message("user"):
        st.markdown(prompt)
    # Add user message to chat history
    st.session_state.messages.append({"role": "user", "content": prompt})

    keywords = ["plot", "graph", "chart", "diagram"]
    if any(token in prompt.lower() for token in keywords):
        prev_context = ""
        for msg in reversed(st.session_state.messages):
            if msg["role"] == "assistant":
                prev_context = msg["content"] + "\n\n" + prev_context
                break
        if len(prev_context) > 0:
            prompt = prompt + "\n\nGiven previous agent responses:\n" + prev_context + "\n"
        response = generate_response("python", prompt)
        if response == "NO_RESPONSE":
            response = "Please try again with a re-phrased query and more context"
            with st.chat_message("error"):
                display_text_with_images(response)
            st.session_state.messages.append(
                {"role": "error", "content": response})
        else:
            code = display_python_code_plots(response['output'])
            try:
                code = "import pandas as pd\n" + code.replace("fig.show()", "")
                code += "st.plotly_chart(fig, theme='streamlit', use_container_width=True)"
                exec(code)
                st.session_state.messages.append({"role": "plot", "content": code})
            except:
                response = "Please try again with a re-phrased query and more context"
                with st.chat_message("error"):
                    display_text_with_images(response)
                st.session_state.messages.append(
                    {"role": "error", "content": response})
    else:
        if len(st.session_state.messages) > 1:
            context_length = 0
            prev_context = ""
            for msg in reversed(st.session_state.messages):
                if context_length > 1:
                    break
                if msg["role"] == "assistant":
                    prev_context = msg["content"] + "\n\n" + prev_context
                    context_length += 1
            response = generate_response("sql", prompt + "\n\nGiven previous agent responses:\n" + prev_context + "\n")
        else:
            response = generate_response("sql", prompt)
        # Display assistant response in chat message container
        with st.chat_message("assistant"):
            display_text_with_images(response)
        # Add assistant response to chat history
        st.session_state.messages.append({"role": "assistant", "content": response})
