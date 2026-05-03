import streamlit as st
import pandas as pd
import time
from Aiprojects.AIportfolio.zerodhaconnect.auth import get_login_url, generate_session
from Aiprojects.AIportfolio.zerodhaconnect.zerodha_client import ZerodhaClient
from Aiprojects.AIportfolio.zerodhaconnect.databricks_client import upload_file, trigger_job
from Aiprojects.AIportfolio.zerodhaconnect.analytics import process_holdings
from sql_agent import SQLAgent
from kiteconnect import KiteConnect
from config import KITE_API_KEY, KITE_API_SECRET
  

def is_token_valid():
    if "access_token" not in st.session_state:
        return False

    login_time = st.session_state.get("login_time", 0)

    # ~1 day validity (keep buffer)
    if time.time() - login_time > 80000:
        return False

    return True

st.set_page_config(layout="wide")

st.title("📊 AI Portfolio Assistant")

# -------------------------------
# 🔐 Handle Zerodha Redirect
# -------------------------------

params = st.query_params

# if "request_token" in params:
#     token = params["request_token"]

#     access_token = generate_session(token)

#     st.session_state["access_token"] = access_token
#     st.session_state["login_time"] = time.time()

#     st.success("✅ Zerodha Connected")
if "request_token" in params and "access_token" not in st.session_state:

    request_token = params["request_token"]

    kite = KiteConnect(api_key=KITE_API_KEY)

    try:
        data = kite.generate_session(
            request_token,
            api_secret=KITE_API_SECRET
        )

        st.session_state["access_token"] = data["access_token"]
        st.session_state["login_time"] = time.time()   # 🔥 FIX

        st.success("✅ Zerodha Connected Successfully")

        st.query_params.clear()  # also important

    except Exception as e:
        st.error(f"Authentication failed: {e}")

# -------------------------------
# 🔗 Connect Button
# -------------------------------

if not is_token_valid():
    st.warning("🔐 Session expired. Please reconnect.")

    login_url = get_login_url()

    st.markdown(f"""
    <div style='text-align:center'>
        <a href="{login_url}">
            <button>🔗 Reconnect Zerodha</button>
        </a>
    </div>
    """, unsafe_allow_html=True)

    st.stop()

# -------------------------------
# 📅 Year Input
# -------------------------------

year = st.number_input("Select Year", 2020, 2030, 2026)

if st.button("Fetch & Process Data"):

    if not is_token_valid():
        st.error("Session expired. Please reconnect.")
        st.stop()
    else:
      try:  
        client = ZerodhaClient(st.session_state["access_token"])
        holdings = client.get_holdings()

        df = pd.DataFrame(holdings)

        st.subheader("📊 Your Holdings")
        st.dataframe(df)
        if "analyze_clicked" not in st.session_state:
            st.session_state.analyze_clicked = False

        if st.button("📈 Analyze Portfolio"):
            st.session_state.analyze_clicked = True

        if st.session_state.analyze_clicked:

            st.subheader("📂 Upload Monthly Reports")

            uploaded_files = st.file_uploader(
                "Upload month-wise CSV files",
                type=["csv"],
                accept_multiple_files=True
                    )
        if uploaded_files:

            all_dfs = []

            for file in uploaded_files:
                df = pd.read_csv(file)

                # Optional: extract month from filename
                df["source_file"] = file.name

                all_dfs.append(df)

            final_df = pd.concat(all_dfs)

            st.success("✅ Files uploaded successfully")

            st.dataframe(final_df.head())
            final_df.to_csv("portfolio.csv", index=False)

            upload_file("portfolio.csv", "/Volumes/main/portfolio/data.csv")

            trigger_job()

            st.success("🚀 Data sent to Databricks")
            st.session_state["data_ready"] = True
            if "data_ready" not in st.session_state:
                st.info("Upload and process data to enable insights")
        # client = ZerodhaClient(st.session_state["access_token"])

        # all_data = []

        # for m in range(1, 13):
        #     trades = client.get_trades(f"{year}-{m:02d}-01", f"{year}-{m:02d}-28")
        #     df = process_holdings(trades)
        #     df["month"] = m
        #     all_data.append(df)

        # final_df = pd.concat(all_data)

        # final_df.to_csv("portfolio.csv", index=False)

        # upload_file("portfolio.csv", "/Volumes/main/portfolio/data.csv")
        # trigger_job()

        # st.session_state["data_ready"] = True

        # st.success("✅ Data processed in Databricks")
      except Exception as e:
            if "TokenException" in str(e):
                st.error("🔐 Session expired. Please reconnect.")
                st.session_state.clear()
                st.stop()
            else:
                st.error(f"Error: {e}")
                st.stop()

# -------------------------------
# 💬 Chat UI
# -------------------------------

agent = SQLAgent()

if "messages" not in st.session_state:
    st.session_state.messages = []

for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

prompt = st.chat_input("Ask about portfolio")

if prompt:

    st.session_state.messages.append({
        "role": "user",
        "content": prompt
    })

    with st.chat_message("user"):
        st.write(prompt)
    if not is_token_valid():
        st.error("Session expired. Reconnect Zerodha.")
        st.stop()

    if "data_ready" not in st.session_state:
        st.error("Process data first")
    else:
        reply, sql, df = agent.ask(prompt)

        st.session_state.messages.append({
            "role": "assistant",
            "content": reply
        })

        with st.chat_message("assistant"):
            st.markdown(reply)
            st.code(sql, language="sql")

            if df is not None:
                st.dataframe(df)