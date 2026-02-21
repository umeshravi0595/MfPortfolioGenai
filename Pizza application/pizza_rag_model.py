# pizza_rag_agent.py

import mlflow
from mlflow.pyfunc import ChatAgent
from mlflow.types.agent import ChatAgentMessage, ChatAgentResponse
import uuid

class PizzaRAGAgent(ChatAgent):

    def load_context(self, context):
        # Load your RAG model
        self.rag_model = mlflow.pyfunc.load_model(
            "models:/retail_analytics.pizza.pizza_rag_models/11"
        )

    def predict(self, messages, context=None, custom_inputs=None):
        user_query = messages[-1].content

        rag_result = self.rag_model.predict({"query": user_query})

        # Handle list of dicts if returned
        if isinstance(rag_result, list):
            rag_result_dict = rag_result[0]
        else:
            rag_result_dict = rag_result

        return ChatAgentResponse(
            messages=[
                ChatAgentMessage(
                    role="assistant",
                    content=rag_result_dict["answer"],
                    id=str(uuid.uuid4())
                )
            ]
        )
