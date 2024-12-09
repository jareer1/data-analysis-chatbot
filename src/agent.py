from langchain import hub
from langchain.agents import AgentExecutor, create_openai_functions_agent, create_structured_chat_agent, create_tool_calling_agent
from langchain.hub import pull
from langchain_community.agent_toolkits.sql.base import create_sql_agent
from langchain.agents.agent_types import AgentType
from langchain.memory import ConversationBufferMemory
from langchain_community.agent_toolkits import SQLDatabaseToolkit
from langchain_community.chat_message_histories import SQLChatMessageHistory
from langchain_community.chat_models import AzureChatOpenAI
from langchain_community.utilities import SQLDatabase
from langchain_experimental.tools import PythonREPLTool
import os
import config
from langchain_openai import ChatOpenAI
from langchain_openai import AzureChatOpenAI

os.environ["OPENAI_API_KEY"]=config.OPENAI_API_KEY

CUSTOM_PREFIX = """You are an expert SQL analyst working with a PostgreSQL database.
Your task is to write clear, efficient, and accurate SQL queries.
Always consider:
1. Performance optimization
2. Proper JOIN conditions
3. WHERE clause efficiency
4. Appropriate aggregation functions
5. Clear column aliasing
"""

CUSTOM_SUFFIX = """Begin!
Context from previous conversation:
{chat_history}
Question: {input}

For any query:I will inspect the database structure to understand the tables and columns.
Follow this format for ALL queries:
1. Thought: Identify what needs to be done.
2. Action: sql_db_query
3. Action Input: Write the raw SQL query here without any formatting.
4. Observation: <result from query>
5. Thought: I now know the final answer.
6. Final Answer: If query is related to any visualization, in ans just return raw sql query results else give ans on basis of sql query.

Remember to:
- For visualization requests, return all data for visualizing it without explanation.
- Write SQL queries directly without any markdown formatting or ```sql tags.
- Complete ALL steps in the sequence including Final Answer.
- Use CTEs for complex queries.
- Format results clearly.

IMPORTANT:
- Never include ```sql, ```, or any other markdown formatting in your Action Input.
- Always end with "Thought: I now know the final answer" followed by "Final Answer:"

Scratchpad: {agent_scratchpad}
"""

langchain_chat_kwargs = {
    "temperature": 0,
    "max_tokens": 4000,
    "verbose": True,
}
chat_openai_model_kwargs = {
    "top_p": 1.0,
    "frequency_penalty": 0.0,
    "presence_penalty": -1,
}


db = SQLDatabase.from_uri("mysql://root:jareer@localhost:3306/ecommerce")


def get_chat_openai(model_name):
    """
    Returns an instance of the ChatOpenAI class initialized with the specified model name.

    Args:
        model_name (str): The name of the model to use.

    Returns:
        ChatOpenAI: An instance of the ChatOpenAI class.

    """

    llm = AzureChatOpenAI(
        azure_deployment=config.AZURE_OPENAI_CHAT_DEPLOYMENT_NAME,  # or your deployment
        api_version=config.AZURE_OPENAI_API_VERSION,
        api_key=config.AZURE_OPENAI_API_KEY,
        azure_endpoint=config.AZURE_OPENAI_ENDPOINT,# or your api version
        temperature=0,
        max_tokens=3000,
    )
    return llm


def get_sql_toolkit(tool_llm_name: str):
    """
    Instantiates a SQLDatabaseToolkit object with the specified language model.

    This function creates a SQLDatabaseToolkit object configured with a language model
    obtained by the provided model name. The SQLDatabaseToolkit facilitates SQL query
    generation and interaction with a database.

    Args:
        tool_llm_name (str): The name or identifier of the language model to be used.

    Returns:
        SQLDatabaseToolkit: An instance of SQLDatabaseToolkit initialized with the provided language model.
    """
    llm_tool = get_chat_openai(model_name=tool_llm_name)
    toolkit = SQLDatabaseToolkit(db=db, llm=llm_tool)
    return toolkit


def get_agent_llm(agent_llm_name: str):
    """
    Retrieve a language model agent for conversational tasks.

    Args:
        agent_llm_name (str): The name or identifier of the language model for the agent.

    Returns:
        ChatOpenAI: A language model agent configured for conversational tasks.
    """
    llm_agent = get_chat_openai(model_name=agent_llm_name)
    return llm_agent

def create_agent_for_sql(tool_llm_name: str = "gpt-4o", agent_llm_name: str = "gpt-4o"):
    """
    Create an agent for SQL-related tasks.

    Args:
        tool_llm_name (str): The name or identifier of the language model for SQL toolkit.
        agent_llm_name (str): The name or identifier of the language model for the agent.

    Returns:
        Agent: An agent configured for SQL-related tasks.

    """
    # agent_tools = sql_agent_tools()
    llm_agent = get_agent_llm(agent_llm_name)
    toolkit = get_sql_toolkit(tool_llm_name)
    message_history = SQLChatMessageHistory(
        session_id="my-session",
        connection="mysql://root:jareer@localhost:3306/ecommerce",        # use this if password need f"mysql://root:{password}
        table_name="message_store",
        session_id_field_name="session_id"
    )
    memory = ConversationBufferMemory(memory_key="chat_history", input_key='input', chat_memory=message_history, return_messages=False)

    agent = create_sql_agent(
        llm=llm_agent,
        toolkit=toolkit,
        agent_type=AgentType.ZERO_SHOT_REACT_DESCRIPTION,
        input_variables=["input", "agent_scratchpad", "chat_history"],
        suffix=CUSTOM_SUFFIX,
        prefix=CUSTOM_PREFIX,
        memory=memory,
        agent_executor_kwargs={"memory": memory},
        # handle_parsing_errors=True,
        # extra_tools=agent_tools,
        verbose=True,
    )
    return agent


def create_agent_for_python(agent_llm_name: str = "gpt-4-0125-preview"):
    """
    Create an agent for Python-related tasks with Chain of Thought reasoning.

    Args:
        agent_llm_name (str): The name or identifier of the language model for the agent.

    Returns:
        AgentExecutor: An agent executor configured for Python-related tasks with structured reasoning.
    """
    instructions = """
    You are an agent designed to answer Python-related questions and create Python code.
    - Always reason step-by-step before writing code. Think about what the user wants, and explain how you will solve the problem.
    - You have access to a Python REPL for executing Python code. Always debug and rerun if you encounter errors.
    - Output your thought process followed by the Python code in this format:

        Reasoning:
        <your step-by-step reasoning>

        Code:
        ```python
        <your Python code>
        ```
    - Use Plotly exclusively for visualizations and follow the requested format strictly.
    - If you can't generate the code, respond with "I don't know."
    """

    # Use a tool for Python execution
    tools = [PythonREPLTool()]

    # Fetch a base prompt and adjust for our specific use case
    base_prompt = pull("langchain-ai/openai-functions-template")
    prompt = base_prompt.partial(instructions=instructions)
    agent = create_openai_functions_agent(ChatOpenAI(model=agent_llm_name, temperature=0), tools, prompt)
    agent_executor = AgentExecutor(agent=agent, tools=tools, verbose=True)
    return agent_executor

