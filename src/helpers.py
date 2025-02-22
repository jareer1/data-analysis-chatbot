from chatbotService import ChatbotAgentService
class helpers:
    def getConnectionString():
        return "mysql://root:jareer@localhost:3306/autobench"

    def initializeAgents(db):
        chatbotAgentService = ChatbotAgentService()
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

        return listTablesTool, getSchemaTool, dbQueryTool, queryCheck, workflow