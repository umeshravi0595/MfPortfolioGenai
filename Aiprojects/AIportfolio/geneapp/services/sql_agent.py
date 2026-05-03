from config.settings import *
import re
import pandas as pd
import streamlit as st
import google.generativeai as genai
from databricks import sql as dbsql

class SQLAgent:

    def __init__(self):
        self._init_gemini()
        self._init_databricks()

    def _init_gemini(self):
        genai.configure(api_key=GEMINI_API_KEY)

        self.model = genai.GenerativeModel(
            model_name=MODEL,
            system_instruction=self._system_prompt()
        )

        self.chat = self.model.start_chat(history=[])

    def _system_prompt(self):
        return f"""
You are a Databricks SQL expert.

Rules:
- Use ONLY 3-part names: {CATALOG}.{SCHEMA}.table
- Use Spark SQL
- Wrap SQL in ```sql ``` blocks
- Add brief insights
- Never generate scalar subqueries returning multiple columns
- Use ORDER BY ... LIMIT 1 for latest data
"""

    def _init_databricks(self):
        try:
            self.conn = dbsql.connect(
                server_hostname=DATABRICKS_HOST,
                http_path=DATABRICKS_PATH,
                access_token=DATABRICKS_TOKEN
            )

            self.schema_info = self._discover()

        except Exception as e:
            st.error(f"Connection failed: {e}")
            self.conn = None
            self.schema_info = {}

    def _discover(self):
        cursor = self.conn.cursor()

        cursor.execute(f"SHOW TABLES IN {CATALOG}.{SCHEMA}")
        rows = cursor.fetchall()

        tables = []

        for r in rows:
            if len(r) >= 3:
                tables.append(r[1] if r[1] else r[2])
            else:
                tables.append(r[-1])

        schema = {}

        for table in tables:
            full_name = f"{CATALOG}.{SCHEMA}.{table}"

            try:
                cursor.execute(f"DESCRIBE TABLE {full_name}")
                rows = cursor.fetchall()

                schema[table] = [
                    {"column": r[0], "type": r[1]}
                    for r in rows
                    if r[0] and not str(r[0]).startswith("#")
                ]

            except Exception as e:
                print(f"Describe failed {table}: {e}")

        return schema

    def schema_markdown(self):
        text = ""

        for table, cols in self.schema_info.items():
            text += f"### {table}\n"
            for c in cols:
                text += f"- {c['column']} ({c['type']})\n"
            text += "\n"

        return text

    def _schema_context(self):
        lines = []

        for table, cols in self.schema_info.items():
            lines.append(f"Table: {CATALOG}.{SCHEMA}.{table}")
            for c in cols:
                lines.append(f"- {c['column']} ({c['type']})")

        return "\n".join(lines)

    def _extract_sql(self, text):
        match = re.search(
            r"```sql\s*([\s\S]*?)```",
            text,
            re.IGNORECASE
        )

        if match:
            return match.group(1).strip()

        return None

    def execute(self, query):
        try:
            cursor = self.conn.cursor()
            cursor.execute(query)

            rows = cursor.fetchall()
            cols = [d[0] for d in cursor.description]

            return pd.DataFrame(rows, columns=cols)

        except Exception as e:
            st.error(f"SQL Error: {e}")
            return None

    def ask(self, question):
        prompt = question + "\n" + self._schema_context()

        resp = self.chat.send_message(prompt)

        reply = resp.text
        sql = self._extract_sql(reply)

        df = None
        if sql:
            df = self.execute(sql)

        return reply, sql, df
