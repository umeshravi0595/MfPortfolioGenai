import re
import pandas as pd
import streamlit as st
import google.generativeai as genai
from databricks import sql as dbsql


# =========================================================
# CONFIG
# =========================================================




# =========================================================
# SQL AGENT
# =========================================================

class SQLAgent:

    def __init__(self):
        self._init_gemini()
        self._init_databricks()

    def _init_gemini(self):

        genai.configure(
            api_key=GEMINI_API_KEY
        )

        self.model = genai.GenerativeModel(
            model_name=MODEL,
            system_instruction=self._system_prompt()
        )

        self.chat = self.model.start_chat(
            history=[]
        )

    def _system_prompt(self):

        return f"""
You are a Databricks SQL expert.

Rules:
- Use ONLY 3-part names:
  {CATALOG}.{SCHEMA}.table

- Use Spark SQL
- Wrap SQL in ```sql code blocks
- Add brief insights
- Never generate scalar subqueries returning multiple columns.
For latest year-month use ORDER BY ... LIMIT 1 or CTEs.
Do not use (col1,col2)=(
subquery
) patterns.
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
            st.error(
                f"Connection failed: {e}"
            )
            self.conn=None
            self.schema_info={}

    def _discover(self):


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


    def schema_markdown(self):

        text=""

        for table,cols in self.schema_info.items():

            text += f"### {table}\n"

            for c in cols:
                text += (
                    f"- {c['column']} "
                    f"({c['type']})\n"
                )

            text+="\n"

        return text


    def _schema_context(self):

        lines=[]

        for table, cols in self.schema_info.items():

            lines.append(
                f"Table: {CATALOG}.{SCHEMA}.{table}"
            )

            for c in cols:
                lines.append(
                    f"- {c['column']} ({c['type']})"
                )

        return "\n".join(lines)


    def _extract_sql(self,text):

        m=re.search(
            r"```sql\s*([\s\S]*?)```",
            text,
            re.I
        )

        if m:
            return m.group(1).strip()

        return None


    def execute(self,query):

        try:
            cursor=self.conn.cursor()

            cursor.execute(query)

            rows=cursor.fetchall()

            cols=[
                d[0]
                for d in cursor.description
            ]

            return pd.DataFrame(
                rows,
                columns=cols
            )

        except Exception as e:
            st.error(
                f"SQL Error: {e}"
            )

            return None


    def ask(self,question):

        prompt=(
            question
            + "\n"
            + self._schema_context()
        )

        resp=self.chat.send_message(
            prompt
        )

        reply=resp.text

        sql=self._extract_sql(
            reply
        )

        df=None

        if sql:
            df=self.execute(sql)

        return reply,sql,df



# =========================================================
# STREAMLIT UI
# =========================================================

st.set_page_config(
    page_title="Databricks SQL Agent",
    layout="wide"
)


@st.cache_resource
def load_agent():
    return SQLAgent()


agent=load_agent()

st.title("Databricks SQL Agent")

col1,col2=st.columns([3,1])

with col2:
    st.subheader("Schema")

    if st.button("Refresh Schema"):
        agent.schema_info=agent._discover()

    st.markdown(
        agent.schema_markdown()
    )


if "messages" not in st.session_state:
    st.session_state.messages=[]


for msg in st.session_state.messages:

    with st.chat_message(
        msg["role"]
    ):
        st.markdown(
            msg["content"]
        )


prompt=st.chat_input(
    "Ask about your data..."
)


if prompt:

    st.session_state.messages.append(
        {
            "role":"user",
            "content":prompt
        }
    )

    with st.chat_message("user"):
        st.write(prompt)


    with st.spinner(
        "Generating SQL..."
    ):

        reply,sql,df=agent.ask(
            prompt
        )


    st.session_state.messages.append(
        {
            "role":"assistant",
            "content":reply
        }
    )


    with st.chat_message(
        "assistant"
    ):
        st.markdown(reply)

        if sql:
            st.code(
                sql,
                language="sql"
            )

        if df is not None:
            st.subheader(
                "Query Results"
            )

            st.dataframe(
                df,
                use_container_width=True
            )


# Sidebar quick prompts
with st.sidebar:

    st.header("Quick Questions")

    if st.button(
        "Top customers"
    ):
        st.session_state.messages.append(
            {
                "role":"user",
                "content":"Top 10 customers by revenue"
            }
        )

    if st.button(
        "Monthly trend"
    ):
        st.session_state.messages.append(
            {
                "role":"user",
                "content":"Monthly revenue trend"
            }
        )