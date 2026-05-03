# Databricks SQL Agent — Setup Guide
 


## Step 1 — Install dependencies (run once)
```bash
pip install anthropic databricks-sql-connector gradio pandas
```
pip install streamlit google-generativeai databricks-sql-connector pandas
streamlit run streamlitSql.py
python -m streamlit run streamlitSql.py
## Step 2 — Get your Databricks Community Edition credentials

### Server Hostname & HTTP Path:
1. Go to community.cloud.databricks.com
2. Click **Compute** in the left sidebar
3. Click your cluster name
4. Click **Advanced Options** → **JDBC/ODBC** tab
5. Copy:
   - **Server Hostname** → e.g. community.cloud.databricks.com
   - **HTTP Path** → e.g. /sql/protocolv1/o/1234567890/0123-456789-abc

### Access Token:
1. Click your email (top right) → **User Settings**
2. Go to **Access Tokens** tab
3. Click **Generate New Token**
4. Copy the token (shown only once)

### Anthropic API Key:
1. Go to console.anthropic.com
2. Click **API Keys** → **Create Key**
3. Copy the key

## Step 3 — Fill in the config

Open `databricks_sql_agent_local.py` and update the top section:

```python
ANTHROPIC_API_KEY   = "sk-ant-..."
DATABRICKS_HOST     = "community.cloud.databricks.com"
DATABRICKS_PATH     = "/sql/protocolv1/o/xxxx/xxxx"
DATABRICKS_TOKEN    = "dapi..."
CATALOG             = "hive_metastore"   # Default for Community Edition
SCHEMA              = "default"          # Or your schema name
```

## Step 4 — Run the agent
```bash
python databricks_sql_agent_local.py
```

Then open **http://localhost:7860** in your browser.

## Notes
- The agent auto-discovers all tables in your schema
- Use "Refresh schema" button if you add new tables
- Use "Reset chat" to start a new topic
- Set `share=True` in the last line to get a public URL (for sharing)
- Results appear as a table on the right side of the UI

## Troubleshooting

| Problem | Fix |
|---|---|
| Connection timeout | Make sure your cluster is **running** in Community Edition |
| No tables found | Check SCHEMA name — try `default` |
| API key error | Verify key starts with `sk-ant-` |
| Port 7860 in use | Change `server_port=7861` in the last line |
