import os
import re
from typing import Any, Annotated, Literal
from typing_extensions import TypedDict

from langchain.agents import AgentExecutor, create_openai_functions_agent
from langchain_core.messages import AIMessage, ToolMessage
from pydantic import BaseModel, Field
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnableLambda, RunnableWithFallbacks
from langchain_openai import ChatOpenAI, AzureChatOpenAI as OpenAIAzureChatOpenAI
from langchain.hub import pull
from langchain.agents.agent_types import AgentType
from langchain.memory import ConversationBufferMemory
from langchain_community.agent_toolkits.sql.base import create_sql_agent
from langchain_community.agent_toolkits import SQLDatabaseToolkit
from langchain_community.chat_message_histories import SQLChatMessageHistory
from langchain_community.chat_models import AzureChatOpenAI
from langchain_community.utilities import SQLDatabase
from langchain_experimental.tools import PythonREPLTool
from langgraph.graph import END, StateGraph, START
from langgraph.graph.message import AnyMessage, add_messages
from langgraph.prebuilt import ToolNode
from langchain_core.tools import tool
import config


# -------------------------------------------------------------------------------
# A minimal SubmitFinalAnswer model
# -------------------------------------------------------------------------------
class SubmitFinalAnswer(BaseModel):
    """Submit the final answer to the user based on the query results."""
    final_answer: str = Field(..., description="The final answer to the user")


# -------------------------------------------------------------------------------
# State definition for the workflow
# -------------------------------------------------------------------------------
class State(TypedDict):
    messages: Annotated[list[AnyMessage], add_messages]


# -------------------------------------------------------------------------------
# ChatbotAgentService using Azure OpenAI
# -------------------------------------------------------------------------------
class ChatbotAgentService:
    def __init__(self):
        self.azureDeployment = config.AzureDeploymentName
        self.azureApiVersion = config.AzureOpenAiVersion
        self.azureApiKey = config.AzureOpenAiKey
        self.azureEndpoint = config.AzureOpenAiEndpoint

    def getChatOpenAI(self, modelName: str = None):
        """
        Returns an Azure Chat OpenAI instance. If modelName is provided, it is passed as the deployment name.
        """
        if modelName:
            return OpenAIAzureChatOpenAI(
                azure_deployment=self.azureDeployment,
                api_version=self.azureApiVersion,
                api_key=self.azureApiKey,
                azure_endpoint=self.azureEndpoint,
                temperature=0,
                max_tokens=3000,
                deployment_name=modelName,
            )
        else:
            return OpenAIAzureChatOpenAI(
                azure_deployment=self.azureDeployment,
                api_version=self.azureApiVersion,
                api_key=self.azureApiKey,
                azure_endpoint=self.azureEndpoint,
                temperature=0,
                max_tokens=3000,
            )

    def getAgentLLM(self, agentLLMName: str):
        return self.getChatOpenAI(modelName=agentLLMName)



    def createAgentForPython(self, agentLLMName: str = "gpt-4-0125-preview") -> AgentExecutor:
        instructions = (
            "You are an agent designed to answer Python-related questions and create Python code.\n"
            "- Always reason step-by-step before writing code. Think about what the user wants, and explain how you will solve the problem.\n"
            "- You have access to a Python REPL for executing Python code. Always debug and rerun if you encounter errors.\n"
            "- Ensure that all graphs are visually engaging, aesthetically pleasing, and designed with clarity and attention to detail. Use appropriate color schemes, clean layouts, and readable labels to enhance their appeal and effectiveness.\n"
            "- Always use a colour combination which is aesthetically pleasing.\n"
            "- Output your thought process followed by the Python code in this format:\n\n"
            "    Reasoning:\n"
            "    <your step-by-step reasoning>\n\n"
            "    Code:\n"
            "    ```python\n"
            "    <your Python code>\n"
            "    ```\n\n"
            "- Use Plotly exclusively for visualizations and follow the requested format strictly.\n"
            "- If you can't generate the code, respond with \"I don't know\".\n"
            "Remember: If you are not provided with data, never generate your own data; just respond with \"I don't know\"."
        )

        tools = [PythonREPLTool()]
        basePrompt = pull("langchain-ai/openai-functions-template")
        prompt = basePrompt.partial(instructions=instructions)

        agent = create_openai_functions_agent(
            self.getChatOpenAI(modelName=agentLLMName),
            tools,
            prompt
        )
        agentExecutor = AgentExecutor(agent=agent, tools=tools, verbose=True)
        return agentExecutor

    @staticmethod
    def createToolNodeWithFallback(tools: list) -> RunnableWithFallbacks[Any, dict]:
        """
        Create a ToolNode with fallback error handling.
        """
        return ToolNode(tools).with_fallbacks(
            [RunnableLambda(ChatbotAgentService.handleToolError)], exception_key="error"
        )

    @staticmethod
    def handleToolError(state) -> dict:
        error = state.get("error")
        toolCalls = state["messages"][-1].tool_calls
        return {
            "messages": [
                ToolMessage(
                    content=f"Error: {repr(error)}\n please fix your mistakes.",
                    tool_call_id=tc["id"],
                )
                for tc in toolCalls
            ]
        }

    def createSqlToolkitTools(self, db):
        """
        Create SQL toolkit tools for the given database instance.
        """
        databaseToolkit = SQLDatabaseToolkit(db=db, llm=self.getChatOpenAI(modelName="gpt-4"))
        allTools = databaseToolkit.get_tools()
        listTablesTool = next(tool for tool in allTools if tool.name == "sql_db_list_tables")
        getSchemaTool = next(tool for tool in allTools if tool.name == "sql_db_schema")
        return listTablesTool, getSchemaTool, allTools

    def createDbQueryTool(self, db):
        """
        Create a decorated database query tool function for the given database instance.
        """

        @tool
        def dbQueryTool(query: str) -> str:
            """
            Execute a SQL query using the provided database instance.

            Args:
                query (str): The SQL query to execute.

            Returns:
                str: The result of the query execution, or an error message if the query fails.
            """
            result = db.run_no_throw(query)
            if not result:
                return "Error: Query failed. Please rewrite your query and try again."
            return result

        return dbQueryTool

    def createQueryCheckFunction(self, dbQueryTool):
        """
        Create a query validation chain that checks for common SQL mistakes.
        """
        queryGenSystem = (
            "You are a SQL expert with a strong attention to detail.\n\n"
            "- Do not use any LIMIT statements in SQL.\n"
            "FOR THE FINAL ANS JUST RETURN ALL THE VALUES THAT U GOT FROM DATABASE\n"
            "Given an input question, output a syntactically correct MYSQL query to run\n "
            "then look at the results of the query and return the answer.\n\n"
            "When generating the query:\n\n"            
            "You can order the results by a relevant column if needed for better readability.\n"
            "NEVER make stuff up if you don't have enough information to answer the query... just say you don't have enough information.\n\n"
            "DO NOT make any DML statements (INSERT, UPDATE, DELETE, DROP etc.) to the database."
        )

        queryCheckPrompt = ChatPromptTemplate.from_messages(
            [("system", queryGenSystem), ("placeholder", "{messages}")]
        )
        queryCheck = queryCheckPrompt | self.getChatOpenAI(modelName="gpt-4o").bind_tools(
            [dbQueryTool], tool_choice="required"
        )
        return queryCheck

    # ---------------------------------------------------------------------------
    # Graph node functions (as static methods)
    # ---------------------------------------------------------------------------
    @staticmethod
    def firstToolCall(state: State) -> dict[str, list[AIMessage]]:
        """
        The first node creates an AIMessage that initiates a tool call to list SQL tables.
        """
        return {
            "messages": [
                AIMessage(
                    content="",
                    tool_calls=[{"name": "sql_db_list_tables", "args": {}, "id": "tool_abcd123"}],
                )
            ]
        }

    @staticmethod
    def modelCheckQuery(state: State, queryCheck) -> dict[str, list[AIMessage]]:
        """
        Use this node to double-check the query before execution.
        """
        return {"messages": [queryCheck.invoke({"messages": [state["messages"][-1]]})]}

    @staticmethod
    def queryGenNode(state: State, queryGen) -> dict[str, list[AIMessage]]:
        """
        Generate a query based on the question and schema. Return error messages if the wrong tool is called.
        """
        message = queryGen.invoke(state)
        toolMessages = []
        if message.tool_calls:
            for tc in message.tool_calls:
                if tc["name"] != "SubmitFinalAnswer":
                    toolMessages.append(
                        ToolMessage(
                            content=(
                                f"Error: The wrong tool was called: {tc['name']}. "
                                "Please fix your mistakes. Remember to only call "
                                "SubmitFinalAnswer to submit the final answer. "
                                "Generated queries should be outputted WITHOUT a tool call."
                            ),
                            tool_call_id=tc["id"],
                        )
                    )
        return {"messages": [message] + toolMessages}

    @staticmethod
    def shouldContinue(state: State) -> Literal[END, "correctQuery", "queryGen"]:
        messages = state["messages"]
        lastMessage = messages[-1]
        print("DEBUG: shouldContinue method called")
        print(f"DEBUG: Last message content: {lastMessage.content}")
        print(f"DEBUG: Last message tool calls: {getattr(lastMessage, 'tool_calls', None)}")

        # If the LLM has produced a final answer (marker found), terminate.
        if "Final Answer:" in lastMessage.content:
            print("DEBUG: Final Answer detected, ending workflow")
            return END

        # If a tool call was produced, we assume the query has been executed.
        if getattr(lastMessage, "tool_calls", None):
            print("DEBUG: Tool calls detected, ending workflow")
            return END

        # If the same query has been generated twice in a row, then break the loop.
        if len(messages) > 1 and messages[-1].content == messages[-2].content:
            print("DEBUG: Repeated message detected, ending workflow")
            return END

        # If an error message was produced, trigger regeneration.
        if lastMessage.content.startswith("Error:"):
            print("DEBUG: Error message detected, regenerating query")
            return "queryGen"

        print("DEBUG: Proceeding to query checking")
        return "correctQuery"

    # ---------------------------------------------------------------------------
    # Build and compile the workflow using langgraph.
    # ---------------------------------------------------------------------------
    def createWorkflow(
            self,
            listTablesTool,
            getSchemaTool,
            dbQueryTool,
            queryCheck,
            createToolNodeWithFallback,
    ):
        """
        Build and compile the workflow state graph.
        """
        workflow = StateGraph(State)

        # Add nodes.
        workflow.add_node("firstToolCall", ChatbotAgentService.firstToolCall)
        workflow.add_node("listTablesTool", createToolNodeWithFallback([listTablesTool]))
        workflow.add_node("getSchemaTool", createToolNodeWithFallback([getSchemaTool]))

        # Node for model to choose tables.
        modelGetSchema = self.getChatOpenAI(modelName="gpt-4o").bind_tools([getSchemaTool])
        workflow.add_node(
            "modelGetSchema", lambda state: {"messages": [modelGetSchema.invoke(state["messages"])]}
        )

        # Build the query generation chain.
        queryGenSystem = (
            "You are a SQL expert with a strong attention to detail.\n\n"
            "Given an input question, output a syntactically correct mySQL query to run, "
            "then look at the results of the query and return the answer.\n\n"
            "When generating the query:\n\n"
            "You can order the results by a relevant column to return the most interesting examples "
            "in the database.\n"
            "Never query for all the columns from a specific table, only ask for the relevant columns given the question.\n\n"
             "NEVER make stuff up if you don't have enough information to answer the query... just say you don't have enough information.\n\n"
            "If you have enough information to answer the input question, simply invoke the appropriate tool "
            "to submit the final answer to the user.\n\n"
            "DO NOT make any DML statements (INSERT, UPDATE, DELETE, DROP etc.) to the database."
        )
        queryGenPrompt = ChatPromptTemplate.from_messages(
            [("system", queryGenSystem), ("placeholder", "{messages}")]
        )
        queryGen = queryGenPrompt | self.getChatOpenAI(modelName="gpt-4o").bind_tools(
            [SubmitFinalAnswer]
        )

        # Local node functions capturing queryGen and queryCheck.
        def localQueryGenNode(state: State) -> dict[str, list[AIMessage]]:
            return ChatbotAgentService.queryGenNode(state, queryGen)

        def localModelCheckQuery(state: State) -> dict[str, list[AIMessage]]:
            return ChatbotAgentService.modelCheckQuery(state, queryCheck)

        workflow.add_node("queryGen", localQueryGenNode)
        workflow.add_node("correctQuery", localModelCheckQuery)
        workflow.add_node("executeQuery", ChatbotAgentService.createToolNodeWithFallback([dbQueryTool]))

        # Define edges.
        workflow.add_edge(START, "firstToolCall")
        workflow.add_edge("firstToolCall", "listTablesTool")
        workflow.add_edge("listTablesTool", "modelGetSchema")
        workflow.add_edge("modelGetSchema", "getSchemaTool")
        workflow.add_edge("getSchemaTool", "queryGen")
        workflow.add_conditional_edges("queryGen", ChatbotAgentService.shouldContinue)
        workflow.add_edge("correctQuery", "executeQuery")
        workflow.add_edge("executeQuery", "queryGen")

        app = workflow.compile()
        return app

    def reformatPromptForSql(self, prompt: str) -> str:
        systemPrompt = (
            "You are an expert at transforming natural language queries into precise SQL-friendly descriptions. "
            "Your core objectives are to:\n"
            "1. Deeply understand the user's underlying data request\n"
            "2. Extract key data and potential visualization requirements\n"
            "3. Rephrase the query to facilitate straightforward SQL query generation\n\n"
            "Reformatting Guidelines:\n"
            "- Distill the query to its essential data retrieval intent\n"
            "- Use action-oriented, specific language\n"
            "- Highlight desired data dimensions (time periods, aggregations)\n"
            "- Eliminate ambiguity\n"
            "- Ensure the reformatted prompt clearly suggests a SQL query approach\n\n"
            "Examples:\n"
            "'draw me a graph for monthly sales' -> 'Retrieve monthly total sales with time periods and sales amounts'\n"
            "'show product performance' -> 'Get product sales with total revenue and ranking'\n"
            "'customer trends' -> 'Fetch customer acquisition data by month with growth metrics'\n"
        )

        # Use Azure OpenAI for reformatting
        llm = self.getChatOpenAI(modelName="gpt-4o")

        # Create a chat prompt template
        promptTemplate = ChatPromptTemplate.from_messages([
            ("system", systemPrompt),
            ("human", "Reformat this query for SQL generation: {inputQuery}")
        ])

        reformattingChain = promptTemplate | llm

        try:
            reformattedResponse = reformattingChain.invoke({"inputQuery": prompt})
            return reformattedResponse.content
        except Exception as e:
            print(f"Prompt reformatting error: {e}")
            return prompt


    def clean_code_fence(self,response: str) -> str:
        """
        Removes markdown-style code fences (e.g., ```html ... ```) from the response.
        """
        return re.sub(r"```[a-zA-Z]*\n?", "", response).strip()

    def formatResponse(self, response: str) -> str:
        """
        Uses an LLM to convert a text response into HTML markup based on its content.
        """
        # Clean response of markdown-style code fences

        formattingInstructions = (
            "You are a formatting assistant for a web frontend. Given the response text below, "
            "determine the best way to display it. "
            "If the response contains structured data (for example, key-value pairs), output it as an HTML table using <table>, <tr>, and <td> tags. Also map the columns for the table properly. "
            "If the response contains a base64 encoded image or indicates that it is image data, embed it as an HTML <img> tag with the appropriate src attribute. "
            "If the response is short or does not contain multiple values, simply wrap it in a paragraph <p> tag. "
            "Return only the HTML markup without any additional commentary. "
        )

        prompt = f"{formattingInstructions}\n\nResponse:\n{response}"

        llm = self.getChatOpenAI(modelName="gpt-4o")

        try:
            formatted_response = llm.invoke(prompt).content
            formatted_response = self.clean_code_fence(formatted_response)

            print('formatted response is ', formatted_response)

            if re.search(r'<(table|img|p|tr|td)>', formatted_response):
                return formatted_response

            # Case 1: Check if the response contains key-value pairs (structured data) and generate a table manually
            if isinstance(formatted_response, dict):
                html_table = "<table border='1'>"
                for key, value in formatted_response.items():
                    html_table += f"<tr><td>{key}</td><td>{value}</td></tr>"
                html_table += "</table>"
                return html_table

            # Case 2: Check if the response contains a Base64-encoded image
            base64_pattern = re.compile(r"^data:image/.+;base64,")
            if isinstance(formatted_response, str) and base64_pattern.match(formatted_response.strip()):
                return f"<img src='{formatted_response}' alt='image' />"

            # Case 3: Return response wrapped in <p> tag if it's plain text
            return f"<p>{formatted_response}</p>"

        except Exception as e:
            print(f"Error in formatting response to markup: {e}")
            return f"<p>{response}</p>"


    def format_response_for_images(response):

        base64_pattern = re.compile(r'(data:image\/(png|jpg|jpeg|gif|webp);base64,[A-Za-z0-9+/=]+)')

        formatted_response = base64_pattern.sub(r'<img src="\1" alt="Embedded Image">', response)

        return formatted_response

    # Example Usage
    response_text = "Here is an image: data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAA..."
    formatted_text = format_response_for_images(response_text)
    print(formatted_text)

