import os
import re
from typing import Any, Annotated, Literal

import pandas as pd
from langchain_community.tools import ListSQLDatabaseTool, InfoSQLDatabaseTool
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
from langchain_community.agent_toolkits.sql.toolkit import (
    QuerySQLDataBaseTool,
    InfoSQLDatabaseTool,
    ListSQLDatabaseTool,
    QuerySQLCheckerTool,
)

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

    def createToolNodeWithFallback(tools: list) -> RunnableWithFallbacks[Any, dict]:
        """
        Create a ToolNode with fallback error handling.
        """
        return ToolNode(tools).with_fallbacks(
            [RunnableLambda(ChatbotAgentService.handleToolError)],
            exception_key="error"
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

    # -----------------------------------------------------------------------
    # 2) Build SQL Tools
    # -----------------------------------------------------------------------
    def createSqlToolkitTools(self, db: SQLDatabase):
        """
        Create SQL toolkit tools for the given database instance,
        then rename them so they're called 'sql_db_list_tables' / 'sql_db_schema', etc.
        """
        databaseToolkit = SQLDatabaseToolkit(db=db, llm=self.getChatOpenAI(modelName="gpt-4"))
        allTools = databaseToolkit.get_tools()
        listTablesTool = next(tool for tool in allTools if tool.name == "sql_db_list_tables")
        getSchemaTool = next(tool for tool in allTools if tool.name == "sql_db_schema")
        return listTablesTool, getSchemaTool, allTools

    def createDbQueryTool(self, db: SQLDatabase):
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
        # Adjust modelName if needed
        queryCheck = queryCheckPrompt | self.getChatOpenAI(modelName="gpt-4o").bind_tools(
            [dbQueryTool], tool_choice="required"
        )
        return queryCheck

    # -----------------------------------------------------------------------
    # 3) Node logic
    # -----------------------------------------------------------------------
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
        Generate a query based on the question and schema.
        Return error messages if the wrong tool is called.
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
        """
        Decide whether to end or move to the next node, based on the last message content.
        """
        messages = state["messages"]
        lastMessage = messages[-1]
        print("DEBUG: Last message is:", lastMessage)

        # If the LLM has produced a final answer, terminate.
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

        # If an error message was produced, go back and regenerate the query.
        if lastMessage.content.startswith("Error:"):
            print("DEBUG: Error message detected, regenerating query")
            return "queryGen"

        print("DEBUG: Proceeding to query checking")
        return "correctQuery"

    # -----------------------------------------------------------------------
    # 4) Build the workflow
    # -----------------------------------------------------------------------
    def createWorkflow(
            self,
            listTablesTool,
            getSchemaTool,
            dbQueryTool,
            queryCheck,
            createToolNodeWithFallback,
    ):
        """
        Build and compile the workflow state graph using langgraph.
        """
        workflow = StateGraph(State)

        # 1) Node: triggers the listing of tables
        workflow.add_node("firstToolCall", ChatbotAgentService.firstToolCall)
        workflow.add_node("listTablesTool", createToolNodeWithFallback([listTablesTool]))

        # 2) Node: get schema
        workflow.add_node("getSchemaTool", createToolNodeWithFallback([getSchemaTool]))
        modelGetSchema = self.getChatOpenAI(modelName="gpt-4o").bind_tools([getSchemaTool])
        workflow.add_node(
            "modelGetSchema",
            lambda state: {"messages": [modelGetSchema.invoke(state["messages"])]}
        )

        # 3) Build the query generation chain
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
        queryGenPrompt = ChatPromptTemplate.from_messages(
            [("system", queryGenSystem), ("placeholder", "{messages}")]
        )
        queryGen = queryGenPrompt | self.getChatOpenAI(modelName="gpt-4o").bind_tools(
            [SubmitFinalAnswer]
        )

        def localQueryGenNode(state: State) -> dict[str, list[AIMessage]]:
            return ChatbotAgentService.queryGenNode(state, queryGen)

        def localModelCheckQuery(state: State) -> dict[str, list[AIMessage]]:
            return ChatbotAgentService.modelCheckQuery(state, queryCheck)

        workflow.add_node("queryGen", localQueryGenNode)
        workflow.add_node("correctQuery", localModelCheckQuery)
        workflow.add_node("executeQuery", ChatbotAgentService.createToolNodeWithFallback([dbQueryTool]))

        # Edges
        workflow.add_edge(START, "firstToolCall")
        workflow.add_edge("firstToolCall", "listTablesTool")
        workflow.add_edge("listTablesTool", "modelGetSchema")
        workflow.add_edge("modelGetSchema", "getSchemaTool")
        workflow.add_edge("getSchemaTool", "queryGen")

        # If "shouldContinue" says "correctQuery", go there; if "queryGen", go back; or end
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

    def convertResponseToDataFrame(self,response,userInput):
        """
        Uses an LLM to convert a text response into HTML markup based on its content.
        """
        # Clean response of markdown-style code fences

        formattingInstructions = (
            f"""
            Convert the following Response into Python code that creates a pandas DataFrame.
            The output should ONLY contain valid Python code that creates a DataFrame variable named 'df'.
            No explanations, no markdown formatting or ```python tags, just the raw executable Python code.

            Response:
            {response}

            Example output format:
            import pandas as pd
            data = [
                ['Category1', 100, 25.5],
                ['Category2', 150, 30.2],
                # more rows...
            ]
            df = pd.DataFrame(data, columns=['Category', 'Value', 'Percentage'])

            NOTE: Do NOT include ```python or ``` markers around the code. Return ONLY the raw Python code.
            """
        )

        prompt = f"{formattingInstructions}\n\nResponse:\n{response}"

        llm = self.getChatOpenAI(modelName="gpt-4o")

        cleanedResponse = llm.invoke(prompt).content
        print('cleaned response is ',cleanedResponse)
        try:
            localVar = {}
            exec(cleanedResponse, globals(), localVar)
            if 'df' in localVar and isinstance(localVar['df'], pd.DataFrame):
                df = localVar['df']
                plot_image_base64 = self.visualize_dataframe(df, userInput)

                if plot_image_base64:
                    return True, plot_image_base64
                else:
                    return False,"Could not create visualization from the data"
            else:
                return False,"Failed to create DataFrame from SQL output"

        except Exception as e:
           return False,f"Error processing data: {str(e)}"

    def visualize_dataframe(self, df, user_input):
        import matplotlib.pyplot as plt
        import seaborn as sns
        import io
        import base64
        import pandas as pd
        import numpy as np
        from matplotlib.ticker import MaxNLocator

        # Set a more modern style
        plt.style.use('seaborn-v0_8-whitegrid')

        viz_types = {
            "plot": "line",
            "graph": "line",
            "chart": "bar",
            "diagram": "scatter",
            "bar": "bar",
            "heatmap": "heatmap",
            "pie": "pie",
            "correlation": "heatmap",
            "distribution": "hist",
            "histogram": "hist",
            "visualization": "auto"
        }

        # Determine requested visualization type
        requested_viz = "auto"
        for viz_keyword in viz_types:
            if viz_keyword in user_input.lower():
                requested_viz = viz_types[viz_keyword]
                break

        # Get column types
        numeric_columns = df.select_dtypes(include=['number']).columns.tolist()
        categorical_columns = df.select_dtypes(include=['object']).columns.tolist()
        date_columns = [col for col in df.columns if pd.api.types.is_datetime64_any_dtype(df[col])]

        # Define color palette
        colors = ['#3498db', '#2ecc71', '#e74c3c', '#f39c12', '#9b59b6', '#1abc9c', '#34495e', '#d35400']

        # Handle empty dataframes or insufficient columns
        if df.empty:
            return None

        # Check if there's any column whose name contains "id"
        id_cols = [col for col in categorical_columns if 'id' in col.lower()]
        id_col = id_cols[0] if id_cols else None

        # Configure figure with high DPI and better aspect ratio
        plt.figure(figsize=(16, 9), dpi=100)

        # Add a light gradient background for visual appeal
        ax = plt.gca()
        gradient = np.linspace(0, 1, 100).reshape(-1, 1)
        gradient = np.repeat(gradient, 100, axis=1)
        ax.imshow(
            gradient, aspect='auto', extent=[0, 1, 0, 1],
            transform=ax.transAxes, alpha=0.1, cmap='Blues_r', zorder=-1
        )

        # ---------------------------------------------------------------------
        # 1) LINE PLOT
        # ---------------------------------------------------------------------
        if requested_viz == "line":
            # We need at least one numeric column for the line’s y-values
            if not numeric_columns:
                return None

            # If we have an "ID" column (object dtype), use that as x-labels;
            # otherwise, fallback to the DataFrame index.
            if id_col and df[id_col].dtype == 'object':
                x_labels = df[id_col]
            else:
                x_labels = df.index.astype(str)

            # Sort the DataFrame by the first numeric column, descending
            df = df.sort_values(by=numeric_columns[0], ascending=False)

            # Calculate marker size based on data size
            marker_size = max(60 // len(df), 6)

            # Create enhanced line plot
            plt.plot(
                range(len(df)), df[numeric_columns[0]],
                marker='o', markersize=marker_size, linestyle='-', linewidth=2.5,
                color=colors[0], alpha=0.9
            )

            # Gradient fill under the line
            plt.fill_between(range(len(df)), df[numeric_columns[0]], alpha=0.2, color=colors[0])

            # X-ticks setup
            if len(df) > 20:
                step = max(len(df) // 10, 1)
                plt.xticks(range(0, len(df), step))
                shown_indices = list(range(0, len(df), step))
                plt.gca().set_xticklabels([x_labels.iloc[i] for i in shown_indices],
                                          rotation=45, ha='right', fontsize=9)
            else:
                plt.xticks(range(len(df)))
                plt.gca().set_xticklabels(x_labels, rotation=45, ha='right', fontsize=9)

            # Annotate data values
            y_max = df[numeric_columns[0]].max()
            y_min = df[numeric_columns[0]].min()
            y_range = y_max - y_min

            for i, v in enumerate(df[numeric_columns[0]]):
                # Skip some labels if too many data points
                if len(df) > 20 and i % (len(df) // 10) != 0:
                    continue

                y_pos = v + y_range * 0.03
                plt.text(
                    i, y_pos, f"{v:.1f}",
                    ha='center', va='bottom',
                    fontsize=9, fontweight='bold', color='#555555'
                )

            plt.title(f"{numeric_columns[0]} by {id_col if id_col else 'Index'}",
                      fontsize=14, pad=20, fontweight='bold')
            plt.ylabel(numeric_columns[0], fontsize=12, labelpad=15)
            plt.xlabel(id_col if id_col else "Index", fontsize=12, labelpad=15)
            plt.grid(axis='y', linestyle='--', alpha=0.4, color='#cccccc')

            # Subtle horizontal lines
            y_ticks = plt.yticks()[0]
            for y in y_ticks:
                plt.axhline(y=y, color='#dddddd', linestyle='-', alpha=0.3, zorder=-1)

        # ---------------------------------------------------------------------
        # 2) BAR CHART
        # ---------------------------------------------------------------------
        elif requested_viz == "bar" or (requested_viz == "auto" and len(numeric_columns) > 0):
            if not numeric_columns:
                return None

            # If there's a categorical column, use that as x-labels; otherwise, use the index
            if categorical_columns:
                x_labels = df[categorical_columns[0]]
            else:
                x_labels = df.index.astype(str)

            df = df.sort_values(by=numeric_columns[0], ascending=False)

            bars = plt.bar(
                range(len(df)), df[numeric_columns[0]],
                width=0.7, color=colors[0], alpha=0.85, edgecolor='white', linewidth=1.5
            )

            # Add gradient effect to bars
            for i, bar in enumerate(bars):
                bar_color = colors[i % len(colors)]
                bar.set_color(bar_color)
                bar.set_alpha(0.85)
                bar.set_edgecolor('white')
                bar.set_linewidth(1.5)

            plt.xticks(range(len(df)))
            plt.gca().set_xticklabels(x_labels, rotation=45, ha='right', fontsize=9)

            # Value labels
            max_val = df[numeric_columns[0]].max()
            for i, v in enumerate(df[numeric_columns[0]]):
                plt.text(
                    i, v + max_val * 0.02, f"{v:.1f}",
                    ha='center', fontsize=10, fontweight='bold', color='#555555'
                )

            plt.title(f"{numeric_columns[0]} by {categorical_columns[0] if categorical_columns else 'Index'}",
                      fontsize=14, pad=20, fontweight='bold')
            plt.ylabel(numeric_columns[0], fontsize=12, labelpad=15)
            plt.xlabel(categorical_columns[0] if categorical_columns else "Index", fontsize=12, labelpad=15)
            plt.grid(axis='y', linestyle='--', alpha=0.3, color='#dddddd')

        # ---------------------------------------------------------------------
        # 3) SCATTER PLOT
        # ---------------------------------------------------------------------
        elif requested_viz == "scatter" or (requested_viz == "auto" and len(numeric_columns) >= 2):
            # We need at least 2 numeric columns to do a scatter
            if len(numeric_columns) < 2:
                return None

            # Enhanced scatter plot with size based on third metric if available
            if len(numeric_columns) >= 3:
                min_size, max_size = 50, 300
                third_col = numeric_columns[2]
                norm_sizes = (
                        (df[third_col] - df[third_col].min()) /
                        (df[third_col].max() - df[third_col].min())
                )
                scatter_size = min_size + norm_sizes * (max_size - min_size)
            else:
                scatter_size = 100

            plt.scatter(
                df[numeric_columns[0]], df[numeric_columns[1]],
                s=scatter_size, c=colors[:len(df)], alpha=0.7,
                edgecolors='white', linewidth=1.5
            )

            # Add trend line if enough points
            if len(df) > 2:
                z = np.polyfit(df[numeric_columns[0]], df[numeric_columns[1]], 1)
                p = np.poly1d(z)
                x_range = np.linspace(df[numeric_columns[0]].min(), df[numeric_columns[0]].max(), 100)
                plt.plot(x_range, p(x_range), "--", color="#555555", alpha=0.8, linewidth=2)

                # Correlation
                corr = df[numeric_columns[:2]].corr().iloc[0, 1]
                plt.annotate(
                    f"Correlation: {corr:.2f}",
                    xy=(0.05, 0.95), xycoords='axes fraction',
                    fontsize=10, fontweight='bold',
                    bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="gray", alpha=0.8)
                )

            plt.title(f"Relationship between {numeric_columns[0]} and {numeric_columns[1]}",
                      fontsize=14, pad=20, fontweight='bold')
            plt.xlabel(numeric_columns[0], fontsize=12, labelpad=15)
            plt.ylabel(numeric_columns[1], fontsize=12, labelpad=15)
            plt.grid(True, linestyle='--', alpha=0.3, color='#dddddd')

        # ---------------------------------------------------------------------
        # 4) HEATMAP
        # ---------------------------------------------------------------------
        elif requested_viz == "heatmap" and len(numeric_columns) >= 2:
            corr_matrix = df[numeric_columns].corr()
            sns.heatmap(
                corr_matrix, annot=True, cmap='coolwarm',
                linewidths=0.5, linecolor='white', fmt='.2f',
                cbar_kws={"shrink": 0.8}
            )
            plt.title("Correlation Heatmap of Numeric Variables",
                      fontsize=14, pad=20, fontweight='bold')

        # ---------------------------------------------------------------------
        # 5) PIE CHART
        # ---------------------------------------------------------------------
        elif requested_viz == "pie" and len(categorical_columns) > 0:
            counts = df[categorical_columns[0]].value_counts()

            # Limit segments if too many
            if len(counts) > 8:
                other_count = counts[8:].sum()
                counts = counts[:7]
                counts['Other'] = other_count

            wedges, texts, autotexts = plt.pie(
                counts,
                labels=counts.index,
                colors=colors[:len(counts)],
                autopct='%1.1f%%',
                startangle=90,
                wedgeprops={'edgecolor': 'white', 'linewidth': 2, 'antialiased': True},
                textprops={'fontsize': 10, 'fontweight': 'bold'},
                shadow=True,
                explode=[0.05] * len(counts)
            )

            for autotext in autotexts:
                autotext.set_color('white')
                autotext.set_fontweight('bold')

            plt.title(f"Distribution of {categorical_columns[0]}",
                      fontsize=14, pad=20, fontweight='bold')

        # ---------------------------------------------------------------------
        # 6) HISTOGRAM
        # ---------------------------------------------------------------------
        elif requested_viz == "hist" and len(numeric_columns) > 0:
            # Create enhanced histogram
            n, bins, patches = plt.hist(
                df[numeric_columns[0]],
                bins=min(20, len(df[numeric_columns[0]].unique())),
                color=colors[0],
                alpha=0.8,
                edgecolor='white',
                linewidth=1.5
            )

            # Add KDE curve
            sns.kdeplot(df[numeric_columns[0]], color='#e74c3c', linewidth=2.5)

            # Add mean and median lines
            mean_val = df[numeric_columns[0]].mean()
            median_val = df[numeric_columns[0]].median()

            plt.axvline(
                mean_val, color='#2ecc71', linestyle='--', linewidth=2.5,
                label=f'Mean: {mean_val:.2f}'
            )
            plt.axvline(
                median_val, color='#3498db', linestyle='-.', linewidth=2.5,
                label=f'Median: {median_val:.2f}'
            )

            plt.legend(fontsize=10)
            plt.title(f"Distribution of {numeric_columns[0]}",
                      fontsize=14, pad=20, fontweight='bold')
            plt.xlabel(numeric_columns[0], fontsize=12, labelpad=15)
            plt.ylabel("Frequency", fontsize=12, labelpad=15)
            plt.grid(axis='y', linestyle='--', alpha=0.3)

        # ---------------------------------------------------------------------
        # 7) AUTO FALLBACK (if none of the above triggered)
        # ---------------------------------------------------------------------
        else:
            # If "auto" is requested but no conditions match (e.g. no numeric columns),
            # we can simply return None or produce a default plot. Here, we'll do None.
            return None

        # ---------------------------------------------------------------------
        # Final styling touches
        # ---------------------------------------------------------------------
        # Remove top & right spines, style bottom & left
        plt.gca().spines['top'].set_visible(False)
        plt.gca().spines['right'].set_visible(False)
        plt.gca().spines['bottom'].set_linewidth(1.2)
        plt.gca().spines['left'].set_linewidth(1.2)
        plt.gca().spines['bottom'].set_color('#555555')
        plt.gca().spines['left'].set_color('#555555')

        # Improve tick appearance
        plt.tick_params(axis='both', which='major', labelsize=10, colors='#555555', length=5)

        # Ensure y-axis uses a reasonable number of ticks
        plt.gca().yaxis.set_major_locator(MaxNLocator(nbins=8))

        # Adjust layout
        plt.tight_layout(pad=3.0)

        # Optional watermark
        plt.figtext(0.99, 0.01, "DataViz", fontsize=8, color='gray', ha='right', alpha=0.5)

        # Convert plot to base64 image
        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=150, bbox_inches='tight')
        buf.seek(0)
        plot_base64 = base64.b64encode(buf.read()).decode('utf-8')
        plt.close()

        return plot_base64
def visualize_dataframe(self, df, user_input):
    import matplotlib.pyplot as plt
    import seaborn as sns
    import io
    import base64
    import pandas as pd
    import numpy as np
    from matplotlib.ticker import MaxNLocator

    # Set a more modern style
    plt.style.use('seaborn-v0_8-whitegrid')

    viz_types = {
        "plot": "line",
        "graph": "line",
        "chart": "bar",
        "diagram": "scatter",
        "bar": "bar",
        "heatmap": "heatmap",
        "pie": "pie",
        "correlation": "heatmap",
        "distribution": "hist",
        "histogram": "hist",
        "visualization": "auto"
    }

    # Determine requested visualization type
    requested_viz = "auto"
    for viz_keyword in viz_types:
        if viz_keyword in user_input.lower():
            requested_viz = viz_types[viz_keyword]
            break

    # Get column types
    numeric_columns = df.select_dtypes(include=['number']).columns.tolist()
    categorical_columns = df.select_dtypes(include=['object']).columns.tolist()
    date_columns = [col for col in df.columns if pd.api.types.is_datetime64_any_dtype(df[col])]

    # Define color palette
    colors = ['#3498db', '#2ecc71', '#e74c3c', '#f39c12', '#9b59b6', '#1abc9c', '#34495e', '#d35400']

    # Handle empty dataframes or insufficient columns
    if df.empty:
        return None

    # Check if there's any column whose name contains "id"
    id_cols = [col for col in categorical_columns if 'id' in col.lower()]
    id_col = id_cols[0] if id_cols else None

    # Configure figure with high DPI and better aspect ratio
    plt.figure(figsize=(16, 9), dpi=100)

    # Add a light gradient background for visual appeal
    ax = plt.gca()
    gradient = np.linspace(0, 1, 100).reshape(-1, 1)
    gradient = np.repeat(gradient, 100, axis=1)
    ax.imshow(
        gradient, aspect='auto', extent=[0, 1, 0, 1],
        transform=ax.transAxes, alpha=0.1, cmap='Blues_r', zorder=-1
    )

    # ---------------------------------------------------------------------
    # 1) LINE PLOT
    # ---------------------------------------------------------------------
    if requested_viz == "line":
        # We need at least one numeric column for the line’s y-values
        if not numeric_columns:
            return None

        # If we have an "ID" column (object dtype), use that as x-labels;
        # otherwise, fallback to the DataFrame index.
        if id_col and df[id_col].dtype == 'object':
            x_labels = df[id_col]
        else:
            x_labels = df.index.astype(str)

        # Sort the DataFrame by the first numeric column, descending
        df = df.sort_values(by=numeric_columns[0], ascending=False)

        # Calculate marker size based on data size
        marker_size = max(60 // len(df), 6)

        # Create enhanced line plot
        plt.plot(
            range(len(df)), df[numeric_columns[0]],
            marker='o', markersize=marker_size, linestyle='-', linewidth=2.5,
            color=colors[0], alpha=0.9
        )

        # Gradient fill under the line
        plt.fill_between(range(len(df)), df[numeric_columns[0]], alpha=0.2, color=colors[0])

        # X-ticks setup
        if len(df) > 20:
            step = max(len(df) // 10, 1)
            plt.xticks(range(0, len(df), step))
            shown_indices = list(range(0, len(df), step))
            plt.gca().set_xticklabels([x_labels.iloc[i] for i in shown_indices],
                                      rotation=45, ha='right', fontsize=9)
        else:
            plt.xticks(range(len(df)))
            plt.gca().set_xticklabels(x_labels, rotation=45, ha='right', fontsize=9)

        # Annotate data values
        y_max = df[numeric_columns[0]].max()
        y_min = df[numeric_columns[0]].min()
        y_range = y_max - y_min

        for i, v in enumerate(df[numeric_columns[0]]):
            # Skip some labels if too many data points
            if len(df) > 20 and i % (len(df) // 10) != 0:
                continue

            y_pos = v + y_range * 0.03
            plt.text(
                i, y_pos, f"{v:.1f}",
                ha='center', va='bottom',
                fontsize=9, fontweight='bold', color='#555555'
            )

        plt.title(f"{numeric_columns[0]} by {id_col if id_col else 'Index'}",
                  fontsize=14, pad=20, fontweight='bold')
        plt.ylabel(numeric_columns[0], fontsize=12, labelpad=15)
        plt.xlabel(id_col if id_col else "Index", fontsize=12, labelpad=15)
        plt.grid(axis='y', linestyle='--', alpha=0.4, color='#cccccc')

        # Subtle horizontal lines
        y_ticks = plt.yticks()[0]
        for y in y_ticks:
            plt.axhline(y=y, color='#dddddd', linestyle='-', alpha=0.3, zorder=-1)

    # ---------------------------------------------------------------------
    # 2) BAR CHART
    # ---------------------------------------------------------------------
    elif requested_viz == "bar" or (requested_viz == "auto" and len(numeric_columns) > 0):
        if not numeric_columns:
            return None

        # If there's a categorical column, use that as x-labels; otherwise, use the index
        if categorical_columns:
            x_labels = df[categorical_columns[0]]
        else:
            x_labels = df.index.astype(str)

        df = df.sort_values(by=numeric_columns[0], ascending=False)

        bars = plt.bar(
            range(len(df)), df[numeric_columns[0]],
            width=0.7, color=colors[0], alpha=0.85, edgecolor='white', linewidth=1.5
        )

        # Add gradient effect to bars
        for i, bar in enumerate(bars):
            bar_color = colors[i % len(colors)]
            bar.set_color(bar_color)
            bar.set_alpha(0.85)
            bar.set_edgecolor('white')
            bar.set_linewidth(1.5)

        plt.xticks(range(len(df)))
        plt.gca().set_xticklabels(x_labels, rotation=45, ha='right', fontsize=9)

        # Value labels
        max_val = df[numeric_columns[0]].max()
        for i, v in enumerate(df[numeric_columns[0]]):
            plt.text(
                i, v + max_val * 0.02, f"{v:.1f}",
                ha='center', fontsize=10, fontweight='bold', color='#555555'
            )

        plt.title(f"{numeric_columns[0]} by {categorical_columns[0] if categorical_columns else 'Index'}",
                  fontsize=14, pad=20, fontweight='bold')
        plt.ylabel(numeric_columns[0], fontsize=12, labelpad=15)
        plt.xlabel(categorical_columns[0] if categorical_columns else "Index", fontsize=12, labelpad=15)
        plt.grid(axis='y', linestyle='--', alpha=0.3, color='#dddddd')

    # ---------------------------------------------------------------------
    # 3) SCATTER PLOT
    # ---------------------------------------------------------------------
    elif requested_viz == "scatter" or (requested_viz == "auto" and len(numeric_columns) >= 2):
        # We need at least 2 numeric columns to do a scatter
        if len(numeric_columns) < 2:
            return None

        # Enhanced scatter plot with size based on third metric if available
        if len(numeric_columns) >= 3:
            min_size, max_size = 50, 300
            third_col = numeric_columns[2]
            norm_sizes = (
                (df[third_col] - df[third_col].min()) /
                (df[third_col].max() - df[third_col].min())
            )
            scatter_size = min_size + norm_sizes * (max_size - min_size)
        else:
            scatter_size = 100

        plt.scatter(
            df[numeric_columns[0]], df[numeric_columns[1]],
            s=scatter_size, c=colors[:len(df)], alpha=0.7,
            edgecolors='white', linewidth=1.5
        )

        # Add trend line if enough points
        if len(df) > 2:
            z = np.polyfit(df[numeric_columns[0]], df[numeric_columns[1]], 1)
            p = np.poly1d(z)
            x_range = np.linspace(df[numeric_columns[0]].min(), df[numeric_columns[0]].max(), 100)
            plt.plot(x_range, p(x_range), "--", color="#555555", alpha=0.8, linewidth=2)

            # Correlation
            corr = df[numeric_columns[:2]].corr().iloc[0, 1]
            plt.annotate(
                f"Correlation: {corr:.2f}",
                xy=(0.05, 0.95), xycoords='axes fraction',
                fontsize=10, fontweight='bold',
                bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="gray", alpha=0.8)
            )

        plt.title(f"Relationship between {numeric_columns[0]} and {numeric_columns[1]}",
                  fontsize=14, pad=20, fontweight='bold')
        plt.xlabel(numeric_columns[0], fontsize=12, labelpad=15)
        plt.ylabel(numeric_columns[1], fontsize=12, labelpad=15)
        plt.grid(True, linestyle='--', alpha=0.3, color='#dddddd')

    # ---------------------------------------------------------------------
    # 4) HEATMAP
    # ---------------------------------------------------------------------
    elif requested_viz == "heatmap" and len(numeric_columns) >= 2:
        corr_matrix = df[numeric_columns].corr()
        sns.heatmap(
            corr_matrix, annot=True, cmap='coolwarm',
            linewidths=0.5, linecolor='white', fmt='.2f',
            cbar_kws={"shrink": 0.8}
        )
        plt.title("Correlation Heatmap of Numeric Variables",
                  fontsize=14, pad=20, fontweight='bold')

    # ---------------------------------------------------------------------
    # 5) PIE CHART
    # ---------------------------------------------------------------------
    elif requested_viz == "pie" and len(categorical_columns) > 0:
        counts = df[categorical_columns[0]].value_counts()

        # Limit segments if too many
        if len(counts) > 8:
            other_count = counts[8:].sum()
            counts = counts[:7]
            counts['Other'] = other_count

        wedges, texts, autotexts = plt.pie(
            counts,
            labels=counts.index,
            colors=colors[:len(counts)],
            autopct='%1.1f%%',
            startangle=90,
            wedgeprops={'edgecolor': 'white', 'linewidth': 2, 'antialiased': True},
            textprops={'fontsize': 10, 'fontweight': 'bold'},
            shadow=True,
            explode=[0.05] * len(counts)
        )

        for autotext in autotexts:
            autotext.set_color('white')
            autotext.set_fontweight('bold')

        plt.title(f"Distribution of {categorical_columns[0]}",
                  fontsize=14, pad=20, fontweight='bold')

    # ---------------------------------------------------------------------
    # 6) HISTOGRAM
    # ---------------------------------------------------------------------
    elif requested_viz == "hist" and len(numeric_columns) > 0:
        # Create enhanced histogram
        n, bins, patches = plt.hist(
            df[numeric_columns[0]],
            bins=min(20, len(df[numeric_columns[0]].unique())),
            color=colors[0],
            alpha=0.8,
            edgecolor='white',
            linewidth=1.5
        )

        # Add KDE curve
        sns.kdeplot(df[numeric_columns[0]], color='#e74c3c', linewidth=2.5)

        # Add mean and median lines
        mean_val = df[numeric_columns[0]].mean()
        median_val = df[numeric_columns[0]].median()

        plt.axvline(
            mean_val, color='#2ecc71', linestyle='--', linewidth=2.5,
            label=f'Mean: {mean_val:.2f}'
        )
        plt.axvline(
            median_val, color='#3498db', linestyle='-.', linewidth=2.5,
            label=f'Median: {median_val:.2f}'
        )

        plt.legend(fontsize=10)
        plt.title(f"Distribution of {numeric_columns[0]}",
                  fontsize=14, pad=20, fontweight='bold')
        plt.xlabel(numeric_columns[0], fontsize=12, labelpad=15)
        plt.ylabel("Frequency", fontsize=12, labelpad=15)
        plt.grid(axis='y', linestyle='--', alpha=0.3)

    # ---------------------------------------------------------------------
    # 7) AUTO FALLBACK (if none of the above triggered)
    # ---------------------------------------------------------------------
    else:
        # If "auto" is requested but no conditions match (e.g. no numeric columns),
        # we can simply return None or produce a default plot. Here, we'll do None.
        return None

    # ---------------------------------------------------------------------
    # Final styling touches
    # ---------------------------------------------------------------------
    # Remove top & right spines, style bottom & left
    plt.gca().spines['top'].set_visible(False)
    plt.gca().spines['right'].set_visible(False)
    plt.gca().spines['bottom'].set_linewidth(1.2)
    plt.gca().spines['left'].set_linewidth(1.2)
    plt.gca().spines['bottom'].set_color('#555555')
    plt.gca().spines['left'].set_color('#555555')

    # Improve tick appearance
    plt.tick_params(axis='both', which='major', labelsize=10, colors='#555555', length=5)

    # Ensure y-axis uses a reasonable number of ticks
    plt.gca().yaxis.set_major_locator(MaxNLocator(nbins=8))

    # Adjust layout
    plt.tight_layout(pad=3.0)

    # Optional watermark
    plt.figtext(0.99, 0.01, "DataViz", fontsize=8, color='gray', ha='right', alpha=0.5)

    # Convert plot to base64 image
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=150, bbox_inches='tight')
    buf.seek(0)
    plot_base64 = base64.b64encode(buf.read()).decode('utf-8')
    plt.close()

    return plot_base64
