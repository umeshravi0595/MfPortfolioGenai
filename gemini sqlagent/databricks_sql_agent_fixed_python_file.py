# ============================================================
# Databricks SQL Agent — Fixed Full Python File
# Save as: sql_agent_app.py
# Run: python sql_agent_app.py
# ============================================================
# pip install google-generativeai databricks-sql-connector pandas gradio==4.44.1

import re
import pandas as pd
import google.generativeai as genai
from databricks import sql as dbsql
import gradio as gr

# ============================================================
# CONFIGURATION
# ============================================================




class SQLAgent:

    def __init__(self):
        print("Starting Databricks SQL Agent...")
        self._init_gemini()
        self._init_databricks()
        print("Agent ready")

    # -----------------------------------------------------
    # Gemini
    # -----------------------------------------------------

    def _init_gemini(self):
        genai.configure(api_key=GEMINI_API_KEY)

        self.gemini_model = genai.GenerativeModel(
            model_name=MODEL,
            system_instruction=self._system_instruction()
        )

        self.chat = self.gemini_model.start_chat(history=[])

    def _system_instruction(self):
        return f"""
You are an expert Databricks SQL analyst.
Convert natural language into Databricks SQL.
Always wrap SQL in ```sql blocks.

Rules:
- Use simple 3-part table names ONLY: {CATALOG}.{SCHEMA}.<table_name>
- Use Spark SQL syntax
- Limit results to 100 unless requested otherwise
- Add brief insights after SQL.
"""

    # -----------------------------------------------------
    # Databricks
    # -----------------------------------------------------

    def _init_databricks(self):
        try:
            self.conn = dbsql.connect(
                server_hostname=DATABRICKS_HOST,
                http_path=DATABRICKS_PATH,
                access_token=DATABRICKS_TOKEN
            )

            print("Connected to Databricks")
            self.schema_info = self._discover_schemas()

        except Exception as e:
            print("Connection failed:", e)
            print("Running demo mode")
            self.conn = None
            self.schema_info = self._demo_schema()

    def _demo_schema(self):
        return {
            "orders": [
                {"column":"order_id","type":"BIGINT"},
                {"column":"customer_id","type":"BIGINT"},
                {"column":"amount","type":"DECIMAL"},
                {"column":"order_date","type":"DATE"},
                {"column":"region","type":"STRING"}
            ],
            "customers": [
                {"column":"customer_id","type":"BIGINT"},
                {"column":"name","type":"STRING"},
                {"column":"segment","type":"STRING"}
            ]
        }

    def _discover_schemas(self):


        cursor = self.conn.cursor()

        print('connection successfull')

        cursor.execute(
            f"SHOW TABLES IN {CATALOG}.{SCHEMA}"
        )
        print("tables list")
        rows = cursor.fetchall()


        # some connectors return table name in index 1 or 2,
        # this handles both
        tables = []
        for r in rows:

            if len(r) >= 3:
                tables.append(r[1] if r[1] else r[2])
            else:
                tables.append(r[-1])

        schema = {}

        for table in tables:

            full_name=f"{CATALOG}.{SCHEMA}.{table}"

            try:
                cursor.execute(
                    f"DESCRIBE TABLE {full_name}"
                )

                rows = cursor.fetchall()

                schema[table] = [
                    {
                        'column': r[0],
                        'type': r[1]
                    }
                    for r in rows
                    if r[0]
                    and not str(r[0]).startswith('#')
                ]

            except Exception as e:
                print(
                    f"Describe failed {table}: {e}"
                )

        return schema

    def refresh_schemas(self):
        self.schema_info = self._discover_schemas()

    # -----------------------------------------------------
    # Helpers
    # -----------------------------------------------------

    def _schema_context(self):
        lines = [f"AVAILABLE TABLES ({CATALOG}.{SCHEMA}):"]

        for table, cols in self.schema_info.items():
            lines.append(
                    f"Table: {CATALOG}.{SCHEMA}.{table}"
                        )
            for c in cols:
                lines.append(
                    f"- {c['column']} ({c['type']})"
                )

        return "\n".join(lines)

    @staticmethod
    def _extract_sql(text):
        match = re.search(
            r"```sql\s*([\s\S]*?)```",
            text,
            re.IGNORECASE
        )

        if match:
            return match.group(1).strip()

        return None

    def _execute_sql(self, query):

        if not self.conn:
            return None

        try:
            cursor = self.conn.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            cols = [d[0] for d in cursor.description]
            return pd.DataFrame(rows, columns=cols)

        except Exception as e:
            print("SQL error:", e)
            return None

    # -----------------------------------------------------
    # CLI method
    # -----------------------------------------------------

    def ask(self, question):

        full_prompt = question + "\n" + self._schema_context()

        response = self.chat.send_message(
            full_prompt
        )

        reply = response.text
        print(reply)

        sql = self._extract_sql(reply)

        if sql:
            return self._execute_sql(sql)

        return None

    # -----------------------------------------------------
    # Gradio methods
    # -----------------------------------------------------

    def ask_ui(self, question):
        try:
            full_prompt = question + self._schema_context()

            response = self.chat.send_message(
                full_prompt
            )

            reply = response.text
            sql = self._extract_sql(reply)

            if not sql:
                return reply, pd.DataFrame()

            df = self._execute_sql(sql)

            reply_text = (
                "Generated SQL:\n\n"
                f"```sql\n{sql}\n```"
            )

            if df is None:
                return reply_text, pd.DataFrame()

            return reply_text, df

        except Exception as e:
            return f"Error: {str(e)}", pd.DataFrame()

    def get_tables(self):

        if not self.schema_info:
            return "No tables found"

        md=[]

        for table, cols in self.schema_info.items():
            md.append(f"### {table}")

            for c in cols:
                md.append(
                    f"- **{c['column']}** ({c['type']})"
                )

            md.append("")

        return "\n".join(md)

    def refresh(self):
        self.refresh_schemas()
        return f"Loaded {len(self.schema_info)} tables"

    def reset(self):
        self.chat = self.gemini_model.start_chat(
            history=[]
        )

    def close(self):
        if self.conn:
            self.conn.close()


# ============================================================
# GRADIO APP
# ============================================================

def build_ui(agent):

    with gr.Blocks(
        title="Databricks SQL Agent",
        theme=gr.themes.Soft()
    ) as app:

        gr.Markdown("# Databricks SQL Agent")

        gr.Markdown(
            f"Connected to {DATABRICKS_HOST} | "
            f"Schema: {SCHEMA} | "
            f"Tables: {len(agent.schema_info)}"
        )

        with gr.Row():

            with gr.Column(scale=3):

                chatbot = gr.Chatbot(
                            height=500,
                            type="messages"
                        )

                with gr.Row():
                    user_input = gr.Textbox(
                        placeholder="Ask your data questions...",
                        scale=5
                    )

                    send_btn = gr.Button(
                        "Send",
                        variant="primary"
                    )

                with gr.Row():
                    reset_btn = gr.Button("Reset")
                    refresh_btn = gr.Button(
                        "Refresh Schema"
                    )

                refresh_status = gr.Textbox(
                    label="Status",
                    interactive=False
                )

                gr.Markdown("## Quick Questions")

                with gr.Row():
                    ex1=gr.Button("Top Customers")
                    ex2=gr.Button("Monthly Trends")

                with gr.Row():
                    ex3=gr.Button("Detect Anomalies")
                    ex4=gr.Button("Revenue Category")

            with gr.Column(scale=2):

                results_df = gr.Dataframe(
                    label="Query Results"
                )

                with gr.Accordion(
                    "Table Schemas",
                    open=False
                ):
                    schema_md = gr.Markdown(
                        agent.get_tables()
                    )

        # --------------------------------------
        # Handlers
        # --------------------------------------

        def respond(message, history):

            if history is None:
                history=[]

            reply, df = agent.ask_ui(message)

            history = history + [
                {
                    "role":"user",
                    "content":message
                },
                {
                    "role":"assistant",
                    "content":reply
                }
            ]

            return history, df, ""


        def reset_chat():
            agent.reset()
            return [], None


        def refresh_schema():
            msg = agent.refresh()
            return msg, agent.get_tables()


        send_btn.click(
            respond,
            inputs=[user_input, chatbot],
            outputs=[chatbot, results_df, user_input]
        )

        user_input.submit(
            respond,
            inputs=[user_input, chatbot],
            outputs=[chatbot, results_df, user_input]
        )

        reset_btn.click(
            reset_chat,
            outputs=[chatbot, results_df]
        )

        refresh_btn.click(
            refresh_schema,
            outputs=[refresh_status, schema_md]
        )

        ex1.click(
            lambda:"Top 10 customers by revenue",
            outputs=user_input
        )

        ex2.click(
            lambda:"Monthly revenue trend last 6 months",
            outputs=user_input
        )

        ex3.click(
            lambda:"Find anomalies in order volume",
            outputs=user_input
        )

        ex4.click(
            lambda:"Revenue by product category",
            outputs=user_input
        )

    return app


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    agent = SQLAgent()

    app = build_ui(agent)

    app.queue()

    app.launch(
        server_name="0.0.0.0",
        server_port=7860,
        inbrowser=True
    )