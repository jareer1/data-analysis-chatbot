from langchain import hub
from langchain.agents import AgentExecutor, create_openai_functions_agent
from langchain.agents import create_sql_agent
from langchain.agents.agent_types import AgentType
from langchain.memory import ConversationBufferMemory
from langchain_community.agent_toolkits import SQLDatabaseToolkit
from langchain_community.chat_message_histories import SQLChatMessageHistory
from langchain_community.utilities import SQLDatabase
from langchain_experimental.tools import PythonREPLTool
from langchain_openai import ChatOpenAI

CUSTOM_SUFFIX = """Begin!

Relevant pieces of previous conversation:
{chat_history}
(Note: Only reference this information if it is relevant to the current query.)

Question: {input}
Thought Process: It is imperative that I do not fabricate information not present in the NYC taxi dataset or engage in hallucination; maintaining accuracy and trustworthiness is crucial.
- For SQL queries involving string or `TEXT` comparisons, I must use the `LOWER()` function for case-insensitive comparisons, and the `LIKE` operator for fuzzy matching.
- Ensure all queries strictly correspond to the structure of the NYC taxi dataset (e.g., the `rides` table) and any relevant tables available (e.g., fare details, payment types).
- If calculating percentages (e.g., return rates, trip frequencies), the logic should be based on the correct formula (e.g., `total_value / overall_total * 100`), and ensure that joins between tables are correct when necessary (e.g., `rides` and any associated vendor/payment type tables).
- If the result of the SQL query is empty, the response should clearly state **"No results found"** without inventing an answer.
- My final response must STRICTLY reflect the output of the executed SQL query and adhere to the dataset.

{agent_scratchpad}
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

sql_db = 'postgresql://tsdbadmin:tbo0mp4fvj2aukky@pg9i4yanln.f38anlyk4s.tsdb.cloud.timescale.com:33188/tsdb'
db=SQLDatabase.from_uri(sql_db)

def get_chat_openai(model_name):
    llm = ChatOpenAI(
        model_name=model_name,
        model_kwargs=chat_openai_model_kwargs,
        **langchain_chat_kwargs
    )
    return llm


def get_sql_toolkit(tool_llm_name: str):

    llm_tool = get_chat_openai(model_name=tool_llm_name)
    toolkit = SQLDatabaseToolkit(db=db, llm=llm_tool)
    return toolkit


def get_agent_llm(agent_llm_name: str):
    llm_agent = get_chat_openai(model_name=agent_llm_name)
    return llm_agent


def create_agent_for_sql(tool_llm_name: str = "gpt-4-0125-preview", agent_llm_name: str = "gpt-4-0125-preview"):

    # agent_tools = sql_agent_tools()
    llm_agent = get_agent_llm(agent_llm_name)
    toolkit = get_sql_toolkit(tool_llm_name)
    message_history = SQLChatMessageHistory(
        session_id="my-session",
        connection_string="postgresql://tsdbadmin:tbo0mp4fvj2aukky@pg9i4yanln.f38anlyk4s.tsdb.cloud.timescale.com:33188/tsdb",
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
        memory=memory,
        agent_executor_kwargs={"memory": memory},
        # handle_parsing_errors=True,
        # extra_tools=agent_tools,
        verbose=True,
    )
    return agent


def create_agent_for_python(agent_llm_name: str = "gpt-4-0125-preview"):

    instructions = """You are an agent designed to write a python code to answer questions.
            You have access to a python REPL, which you can use to execute python code.
            If you get an error, debug your code and try again.
            You might know the answer without running any code, but you should still run the code to get the answer.
            If it does not seem like you can write code to answer the question, just return "I don't know" as the answer.
            Always output the python code only.
            Generate the code <code> for plotting the previous data in plotly, in the format requested. 
            The solution should be given using plotly and only plotly. Do not use matplotlib.  
            You must only generate plots based on data retrieved from SQL queries, not on assumptions.
            DO NOT USE ANY DUMMY DATA FOR ANY TYPES OF GRAPHS.
            Return the code <code> in the following
            format ```python <code>```
            """
    tools = [PythonREPLTool()]
    base_prompt = hub.pull("langchain-ai/openai-functions-template")
    prompt = base_prompt.partial(instructions=instructions)
    agent = create_openai_functions_agent(ChatOpenAI(model=agent_llm_name, temperature=0), tools, prompt)
    agent_executor = AgentExecutor(agent=agent, tools=tools, verbose=True)
    return agent_executor
