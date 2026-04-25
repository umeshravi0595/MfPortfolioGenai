
# ============================================================
# Databricks SQL Agent — Powered by Google Gemini (FREE)
# Works with Databricks Community Edition
# ============================================================
#
# SETUP (run once on your laptop):
#   pip install google-generativeai databricks-sql-connector pandas
#
# FREE GEMINI API KEY:
#   → https://aistudio.google.com
#   → Sign in with Google → Get API Key → Create API Key
#
# DATABRICKS JDBC DETAILS:
#   → Compute → your cluster → Advanced Options → JDBC/ODBC
#   → Copy: Server Hostname, HTTP Path
#   → User Settings → Access Tokens → Generate New Token
# ============================================================
 
import re
import pandas as pd
import google.generativeai as genai
from databricks import sql as dbsql
import gradio as gr 
 
# ════════════════════════════════════════════════════════════
# CONFIGURATION — Fill these in before running
# ════════════════════════════════════════════════════════════
 
                          # max rows sent to Gemini for analysis
 
# ════════════════════════════════════════════════════════════
 
 
class SQLAgent:
    """
    Conversational SQL Agent for Databricks Community Edition.
    Uses Google Gemini (free) to convert natural language → SQL,
    executes on Databricks, and analyzes the results.
    """
 
    def __init__(self):
        print("🚀 Starting Databricks SQL Agent (Gemini)...")
        self._init_gemini()
        self._init_databricks()
        print(f"\n✅ Agent ready! Ask me anything about your data.\n")
 
    # ── Gemini Initialisation ─────────────────────────────────
 
    def _init_gemini(self):
        genai.configure(api_key=GEMINI_API_KEY)
        self.gemini_model = genai.GenerativeModel(
            model_name         = MODEL,
            system_instruction = self._system_instruction()
        )
        self.chat = self.gemini_model.start_chat(history=[])
        print(f"✅ Gemini connected  →  model: {MODEL}")
 
    def _system_instruction(self):
        return f"""You are an expert Databricks SQL analyst and data engineer.
 
YOUR RESPONSIBILITIES:
1. Convert natural language questions into accurate Databricks SQL queries.
2. Always wrap SQL inside ```sql ... ``` code blocks.
3. After the SQL block, write "**Insights:**" followed by 2-3 sharp bullet points.
4. When results are provided, analyze them and highlight key trends, anomalies, or patterns.
5. Remember previous questions in the conversation for follow-up queries.
 
SQL RULES FOR DATABRICKS COMMUNITY EDITION:
- Use simple 2-part table names ONLY: {SCHEMA}.<table_name>
- Do NOT use 3-part names like hive_metastore.default.table (not supported)
- Use standard Spark SQL / Hive SQL syntax
- For date operations use: DATE_TRUNC, DATE_ADD, DATE_SUB, DATEDIFF, TO_DATE
- For window functions use: ROW_NUMBER(), RANK(), LAG(), LEAD(), SUM() OVER()
- For anomaly detection use: AVG() OVER() and STDDEV() OVER() with window frames
- Limit results to 100 rows unless user asks for more
- Add inline comments for complex logic
 
RESPONSE FORMAT:
```sql
-- your query here
SELECT ...
FROM {SCHEMA}.<table>
WHERE ...
```
 
**Insights:**
- Key insight 1 with specific numbers
- Key insight 2 highlighting trends or anomalies
- Key insight 3 with a recommended next step
"""
 
    # ── Databricks Initialisation ─────────────────────────────
 
    def _init_databricks(self):
        try:
            self.conn = dbsql.connect(
                server_hostname = DATABRICKS_HOST,
                http_path       = DATABRICKS_PATH,
                access_token    = DATABRICKS_TOKEN,
            )
            print(f"✅ Databricks connected  →  schema: {SCHEMA}")
            self.schema_info = self._discover_schemas()
            table_names = list(self.schema_info.keys())
            print(f"📂 Tables found: {', '.join(table_names) if table_names else 'none'}")
        except Exception as e:
            print(f"⚠️  Databricks connection failed: {e}")
            print("   Running in DEMO mode — SQL generated but not executed.")
            self.conn = None
            self.schema_info = self._demo_schema()
 
    def _demo_schema(self):
        """Fallback demo schema when DB is not connected."""
        return {
            "orders":    [
                {"column": "order_id",    "type": "BIGINT"},
                {"column": "customer_id", "type": "BIGINT"},
                {"column": "product_id",  "type": "BIGINT"},
                {"column": "order_date",  "type": "DATE"},
                {"column": "amount",      "type": "DECIMAL(12,2)"},
                {"column": "quantity",    "type": "INT"},
                {"column": "status",      "type": "STRING"},
                {"column": "region",      "type": "STRING"},
            ],
            "customers": [
                {"column": "customer_id",    "type": "BIGINT"},
                {"column": "name",           "type": "STRING"},
                {"column": "segment",        "type": "STRING"},
                {"column": "country",        "type": "STRING"},
                {"column": "lifetime_value", "type": "DECIMAL(14,2)"},
                {"column": "created_date",   "type": "DATE"},
            ],
            "products": [
                {"column": "product_id", "type": "BIGINT"},
                {"column": "name",       "type": "STRING"},
                {"column": "category",   "type": "STRING"},
                {"column": "price",      "type": "DECIMAL(10,2)"},
                {"column": "cost",       "type": "DECIMAL(10,2)"},
                {"column": "is_active",  "type": "BOOLEAN"},
            ],
        }
 
    # ── Schema Discovery ──────────────────────────────────────
 
    def _discover_schemas(self) -> dict:
        """Fetch all table schemas from Databricks Community Edition."""
        cursor = self.conn.cursor()
        cursor.execute(f"USE {SCHEMA}")
        cursor.execute("SHOW TABLES")
        tables = [r[1] for r in cursor.fetchall()]
 
        schema = {}
        for table in tables:
            try:
                cursor.execute(f"DESCRIBE TABLE {table}")
                rows = cursor.fetchall()
                schema[table] = [
                    {"column": r[0], "type": r[1]}
                    for r in rows
                    if r[0] and not r[0].startswith("#") and r[0].strip() != ""
                ]
            except Exception as e:
                print(f"   ⚠️  Could not describe {table}: {e}")
        return schema
 
    def refresh_schemas(self):
        """Re-discover schemas after DDL changes."""
        print("🔄 Refreshing table schemas...")
        self.schema_info = self._discover_schemas()
        print(f"✅ {len(self.schema_info)} table(s) loaded.")
 
    # ── Schema Context for Gemini ─────────────────────────────
 
    def _schema_context(self) -> str:
        """Build schema string injected into every question."""
        if not self.schema_info:
            return "\nNo tables found in schema."
 
        lines = [f"\nAVAILABLE TABLES IN SCHEMA '{SCHEMA}':"]
        for table, cols in self.schema_info.items():
            lines.append(f"\nTable: {SCHEMA}.{table}")
            lines.append("Columns:")
            for c in cols:
                lines.append(f"  - {c['column']} ({c['type']})")
        return "\n".join(lines)
 
    # ── SQL Extraction ────────────────────────────────────────
 
    @staticmethod
    def _extract_sql(text: str) -> str | None:
        match = re.search(r"```sql\s*([\s\S]*?)```", text, re.IGNORECASE)
        return match.group(1).strip() if match else None
 
    # ── SQL Execution ─────────────────────────────────────────
 
    def _execute_sql(self, query: str) -> pd.DataFrame | None:
        if not self.conn:
            print("⚠️  No DB connection. Showing SQL only.")
            return None
        try:
            cursor = self.conn.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            cols = [d[0] for d in cursor.description]
            return pd.DataFrame(rows, columns=cols)
        except Exception as e:
            print(f"❌ SQL execution error: {e}")
            return None
 
    # ══════════════════════════════════════════════════════════
    # CORE METHOD — ask()
    # ══════════════════════════════════════════════════════════
 
    def ask(self, question: str) -> pd.DataFrame | None:
        """
        Ask a natural language question about your Delta tables.
 
        Parameters
        ----------
        question : str   Plain English question about your data.
 
        Returns
        -------
        pd.DataFrame | None   Query results as a DataFrame.
 
        Example
        -------
        df = agent.ask("Top 10 customers by revenue this month")
        df = agent.ask("Break that down by region")   # follow-up works!
        """
        print(f"\n{'='*60}")
        print(f"🧑 You: {question}")
        print(f"{'='*60}")
 
        # ── Step 1: Send question + schema to Gemini ──────────
        # Schema is appended each time because system_instruction
        # is set at init (before schema is loaded from DB)
        full_prompt = question + self._schema_context()
 
        try:
            response = self.chat.send_message(full_prompt)
            reply    = response.text
        except Exception as e:
            print(f"❌ Gemini API error: {e}")
            return None
 
        # ── Step 2: Extract SQL ───────────────────────────────
        sql = self._extract_sql(reply)
 
        if not sql:
            print(f"\n🤖 Agent: {reply}")
            return None
 
        print(f"\n📝 Generated SQL:\n{'-'*50}\n{sql}\n{'-'*50}")
 
        # ── Step 3: Execute SQL on Databricks ─────────────────
        df = self._execute_sql(sql)
 
        if df is not None:
            row_count = len(df)
            print(f"\n📊 Results — {row_count} rows × {len(df.columns)} columns")
            print(df.head(10).to_string(index=False))
 
            # ── Step 4: Send results back to Gemini for analysis
            if row_count > 0:
                result_text = df.head(MAX_RESULT_ROWS).to_string(index=False)
                analysis_prompt = (
                    f"The query returned {row_count} rows. "
                    f"Here are the results:\n\n{result_text}\n\n"
                    f"Please analyze and provide key insights."
                )
                try:
                    analysis_resp = self.chat.send_message(analysis_prompt)
                    analysis = analysis_resp.text
                    print(f"\n💡 Analysis:\n{'-'*50}\n{analysis}\n{'-'*50}")
                except Exception as e:
                    print(f"⚠️  Analysis step failed: {e}")
        else:
            # Show insights from the initial response even without results
            if "**Insights:**" in reply:
                insights = reply.split("**Insights:**")[1].strip()
                print(f"\n💡 Insights:\n{insights}")
 
        return df
 
    # ══════════════════════════════════════════════════════════
    # UTILITY METHODS
    # ══════════════════════════════════════════════════════════
 
    def show_tables(self):
        """Print all discovered tables and their columns."""
        if not self.schema_info:
            print("No tables found.")
            return
        print(f"\n📂 Tables in schema '{SCHEMA}':")
        for table, cols in self.schema_info.items():
            print(f"\n  📋 {table}")
            for c in cols:
                print(f"     {c['column']:35s} {c['type']}")
 
    def reset(self):
        """Clear conversation history and start fresh."""
        self.chat = self.gemini_model.start_chat(history=[])
        print("🔄 Conversation reset — ready for a new topic.")
 
    def close(self):
        """Close the Databricks connection."""
        if self.conn:
            self.conn.close()
            print("🔌 Databricks connection closed.")
 
 
# ════════════════════════════════════════════════════════════
# INTERACTIVE CHAT LOOP
# ════════════════════════════════════════════════════════════
 
def run_chat(agent: SQLAgent):
    print("\n" + "="*60)
    print("  Databricks SQL Agent  |  Powered by Gemini")
    print("  Commands: !tables  !reset  !refresh  exit")
    print("="*60 + "\n")
 
    while True:
        try:
            user_input = input("🧑 You: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nGoodbye!")
            break
 
        if not user_input:
            continue
 
        if user_input.lower() in ("exit", "quit"):
            agent.close()
            break
        elif user_input == "!tables":
            agent.show_tables()
        elif user_input == "!reset":
            agent.reset()
        elif user_input == "!refresh":
            agent.refresh_schemas()
        else:
            agent.ask(user_input)

# ============================================================
# GRADIO UI
# ============================================================

def build_ui(agent: SQLAgent):

    with gr.Blocks(
        title="Databricks SQL Agent",
        theme=gr.themes.Soft(primary_hue="emerald"),
        css="""
        .sql-output { font-family: monospace; }
        #results-df { max-height: 300px; overflow-y: auto; }
        .gr-button-primary { background: #1D9E75 !important; }
        """
    ) as app:

        gr.Markdown("## 🔗 Databricks SQL Agent")
        gr.Markdown(
            f"Connected to **{DATABRICKS_HOST}** · Schema: `{SCHEMA}` · "
            f"{len(agent.schema_info)} table(s) loaded"
        )

        with gr.Row():

            # ── Left: Chat ──────────────────────────────────
            with gr.Column(scale=3):
                chatbot = gr.Chatbot(
                    label="Chat",
                    show_label=False,
                    
                )

                with gr.Row():
                    user_input = gr.Textbox(
                        placeholder="Ask about your data… e.g. 'Show top 10 customers by revenue'",
                        show_label=False,
                        scale=5,
                        container=False,
                    )
                    send_btn = gr.Button("Send", variant="primary", scale=1)

                with gr.Row():
                    reset_btn = gr.Button("🔄 Reset chat", size="sm")
                    refresh_btn = gr.Button("♻️ Refresh schema", size="sm")
                    refresh_status = gr.Textbox(show_label=False, container=False, scale=3, interactive=False)

                gr.Markdown("**Quick questions:**")
                with gr.Row():
                    ex1 = gr.Button("Top customers by revenue", size="sm")
                    ex2 = gr.Button("Monthly trend analysis", size="sm")
                with gr.Row():
                    ex3 = gr.Button("Detect anomalies in orders", size="sm")
                    ex4 = gr.Button("Revenue by category", size="sm")

            # ── Right: Results + Schema ──────────────────────
            with gr.Column(scale=2):
                gr.Markdown("### Query Results")
                results_df = gr.DataFrame(
                    label="",
                    elem_id="results-df",
                    wrap=True,
                    
                )

                with gr.Accordion("📋 Table Schemas", open=False):
                    schema_md = gr.Markdown(agent.get_tables())

        # ── Event handlers ───────────────────────────────────

        def respond(message, history):
            if not message.strip():
                return history, None, ""
            agent_text, df = agent.chat(message, history)
            history.append((message, agent_text))
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
            outputs=[chatbot, results_df, user_input],
        )
        user_input.submit(
            respond,
            inputs=[user_input, chatbot],
            outputs=[chatbot, results_df, user_input],
        )
        reset_btn.click(reset_chat, outputs=[chatbot, results_df])
        refresh_btn.click(refresh_schema, outputs=[refresh_status, schema_md])

        ex1.click(lambda: "Top 10 customers by total revenue", outputs=user_input)
        ex2.click(lambda: "Show monthly revenue trend for the last 6 months", outputs=user_input)
        ex3.click(lambda: "Detect days where order count is more than 2 standard deviations from the 30-day average", outputs=user_input)
        ex4.click(lambda: "Compare revenue by product category vs last quarter", outputs=user_input)

    return app


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    print("🚀 Starting Databricks SQL Agent...")
    agent = SQLAgent()
    app = build_ui(agent)
    app.launch(
        server_name="0.0.0.0",
        server_port=7860,
        share=False,          # Set to True to get a public URL
        inbrowser=True,       # Auto-opens browser
    )
