from services.sql_agent import SQLAgent

class AnalyticsService:

    def __init__(self):
        self.agent = SQLAgent()

    def ask(self, question):
        return self.agent.ask(question)