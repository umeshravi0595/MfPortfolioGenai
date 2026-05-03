import streamlit as st
from services.analytics_service import AnalyticsService
from services.databricks_client import DatabricksClient
from services.pdf_service import generate_pdf

st.set_page_config(layout="wide")

st.title("📊 AI Portfolio Assistant")

agent = AnalyticsService()
db = DatabricksClient()

# -------------------------------
# 📂 LEFT PANEL (UPLOAD + JOB)
# -------------------------------
with st.sidebar:

    st.header("⚙️ Data Pipeline")

    uploaded_file = st.file_uploader("Upload CSV")

    year = st.number_input("Year", 2020, 2030, 2026)
    month = st.number_input("Month", 1, 12, 1)

    if st.button("Upload File"):

        if uploaded_file:
            db.upload_file(uploaded_file, uploaded_file.name)

    if st.button("Trigger Job"):

        if uploaded_file:
            db.trigger_job(uploaded_file.name, year, month)

# -------------------------------
# 💬 CHAT UI
# -------------------------------

if "messages" not in st.session_state:
    st.session_state.messages = []

for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

prompt = st.chat_input("Ask about portfolio...")

if prompt:

    st.session_state.messages.append({
        "role": "user",
        "content": prompt
    })

    with st.chat_message("user"):
        st.write(prompt)

    reply, sql, df = agent.ask(prompt)

    st.session_state.messages.append({
        "role": "assistant",
        "content": reply
    })

    with st.chat_message("assistant"):
        st.markdown(reply)

        if sql:
            st.code(sql, language="sql")

        if df is not None:
            st.dataframe(df)

        # ✅ PDF button
        if st.button("Download Report"):
            file = generate_pdf(reply)
            with open(file, "rb") as f:
                st.download_button(
                    "Download PDF",
                    f,
                    file_name="portfolio_report.pdf"
                )