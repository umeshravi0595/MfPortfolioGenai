import pandas as pd
 
def process_holdings(holdings):

    df = pd.DataFrame(holdings)
 
    if df.empty:
        return df

    df["investment"] = df["quantity"] * df["average_price"]
    df["current_value"] = df["quantity"] * df["last_price"]

    df["pnl"] = df["current_value"] - df["investment"]
    df["return_pct"] = (df["pnl"] / df["investment"]) * 100

    return df


def portfolio_summary(df):

    if df.empty:
        return {}

    total_investment = df["investment"].sum()
    total_value = df["current_value"].sum()
    total_pnl = df["pnl"].sum()

    return {
        "investment": total_investment,
        "value": total_value,
        "pnl": total_pnl,
        "return_pct": (total_pnl / total_investment) * 100
    }