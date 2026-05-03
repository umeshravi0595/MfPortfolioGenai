import streamlit as st
from kiteconnect import KiteConnect
from auth import KITE_API_KEY, KITE_API_SECRET
from zerodha_client import ZerodhaClient
from analytics import process_holdings
 

st.set_page_config(layout="wide")

st.title("📊 AI Portfolio Assistant")

# -------------------------------
# 🔐 Step 1: Handle redirect token
# -------------------------------

query_params = st.query_params

if "request_token" in query_params and "access_token" not in st.session_state:
    try:
        request_token = query_params["request_token"]

        kite = KiteConnect(api_key=KITE_API_KEY)

        data = kite.generate_session(
            request_token,
            api_secret=KITE_API_SECRET
        )

        st.session_state["access_token"] = data["access_token"]

        st.success("✅ Zerodha Connected Successfully")

        # 🔥 prevent reuse bug
        st.query_params.clear()

    except Exception as e:
        st.error(f"Login failed: {e}")

# -------------------------------
# 🔘 Connect Button (bottom)
# -------------------------------

st.markdown("---")

kite = KiteConnect(api_key=KITE_API_KEY)

if "access_token" not in st.session_state:
    login_url = kite.login_url()

    st.markdown(
        f"""
        <div style='text-align:center'>
            <a href="{login_url}" target="_self">
                <button style="padding:10px 20px;font-size:16px;">
                    🔗 Connect to Zerodha
                </button>
            </a>
        </div>
        """,
        unsafe_allow_html=True
    )
else:
    st.success("🟢 Connected to Zerodha")

# -------------------------------
# 💬 Chat UI
# -------------------------------

if "messages" not in st.session_state:
    st.session_state.messages = []

for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

prompt = st.chat_input("Ask about your portfolio...")

if prompt:
    st.session_state.messages.append({
        "role": "user",
        "content": prompt
    })

    with st.chat_message("user"):
        st.write(prompt)

    # -------------------------------
    # 🔥 Only run if connected
    # -------------------------------
    if "access_token" not in st.session_state:
        st.error("Please connect to Zerodha first")
    else:
        client = ZerodhaClient(
            st.session_state["access_token"]
        )

        holdings = client.get_holdings()
        df = process_holdings(holdings)

        reply = f"You have {len(df)} stocks in your portfolio"

        st.session_state.messages.append({
            "role": "assistant",
            "content": reply
        })

        with st.chat_message("assistant"):
            st.write(reply)
            st.dataframe(df)