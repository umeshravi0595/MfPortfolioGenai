# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# Widgets

dbutils.widgets.text("year",'')
dbutils.widgets.text("month",'')
dbutils.widgets.text("filename",'')

year_str = dbutils.widgets.get("year")
month_str = dbutils.widgets.get("month")
filename = dbutils.widgets.get("filename")

year_int = int(year_str) if year_str else None
month_int = int(month_str) if month_str else None

print(year_int, month_int, filename)

# COMMAND ----------

def rename_and_cleanup_csv(path: str, filename: str):
    files = dbutils.fs.ls(path)
    part_file = None
    for f in files:
        if f.name.startswith('part-') and f.name.endswith('.csv'):
            part_file = f.name
            break
    if not part_file:
        raise FileNotFoundError(f'No part file found in {path}')
    source = f'{path}/{part_file}'
    dest = f'{path}/{filename}'
    dbutils.fs.mv(source, dest)
    print(f'Renamed {source} to {dest}')
    # Remove all other files except the CSV
    files = dbutils.fs.ls(path)
    for f in files:
        if f.name != filename:
            dbutils.fs.rm(f'{path}/{f.name}')
            print(f'Removed {path}/{f.name}')

# Example usage:
# rename_and_cleanup_csv('/Volumes/retail_analytics/portfolio/amfi_data/stock_returns.csv', 'stock_returns.csv')

# COMMAND ----------

# Load AMFI data

df = spark.read.csv(
    '/Volumes/retail_analytics/portfolio/amfi_data/AMFI data.csv',
    header=True,
    sep=';',
    inferSchema=True
)

df.createOrReplaceTempView('amfi_data')

# COMMAND ----------

# Load Portfolio file

portfolio_df = spark.read.csv(
    f'/Volumes/retail_analytics/portfolio/amfi_data/{filename}',
    header=True,
    inferSchema=True
)

portfolio_df = portfolio_df.withColumn("year", lit(year_int)) \
                           .withColumn("month", lit(month_int))

portfolio_df.createOrReplaceTempView('portfolio_zerodha')

# COMMAND ----------

# Load stock history

stock_price_history = spark.read.csv(
    '/Volumes/retail_analytics/portfolio/amfi_data/all_stocks_monthly_returns.csv',
    header=True,
    inferSchema=True
)

stock_price_history = stock_price_history \
    .withColumn('ticker', regexp_replace(col('ticker'), '\\.NS$', '')) \
    .withColumn('YearMonth_date', to_date(col('YearMonth'), 'yyyy-MM-dd')) \
    .withColumn('year', year(col('YearMonth_date'))) \
    .withColumn('month', month(col('YearMonth_date')))

stock_price_history.createOrReplaceTempView('stock_price_history')

# COMMAND ----------

# Load MF history

mf_price_history = spark.read.csv(
    '/Volumes/retail_analytics/portfolio/amfi_data/mf_monthly_returns.csv',
    header=True,
    inferSchema=True
)

mf_price_history = mf_price_history \
    .withColumn('YearMonth_date', to_date(col('YearMonth'), 'yyyy-MM-dd')) \
    .withColumn('year', year(col('YearMonth_date'))) \
    .withColumn('month', month(col('YearMonth_date')))

mf_price_history.createOrReplaceTempView('mf_price_history')

# COMMAND ----------

# Portfolio enrichment

portfolio_zerodha = spark.sql("""
SELECT *,
ROUND(`Quantity Available` * `Average Price`, 2) AS invested_amount,
ROUND((`Quantity Available` * `Average Price`) + `Unrealized P&L`, 2) AS total_value
FROM portfolio_zerodha
""")

portfolio_zerodha.createOrReplaceTempView("portfolio_zerodha")

# COMMAND ----------

# STOCK RETURNS

stock_returns = spark.sql(f"""
SELECT 
    p.symbol,
    p.year,
    p.month,
    p.invested_amount,
    p.total_value,
    ROUND(p.total_value * st.monthly_return_pct / 100, 2) AS profit_loss,
    ROUND(p.total_value + (p.total_value * st.monthly_return_pct / 100), 2) AS final_value
FROM portfolio_zerodha p
JOIN stock_price_history st
    ON p.symbol = st.ticker
    AND p.year = st.year
    AND p.month = st.month
""")

# 🔥 PARTITION OVERWRITE
stock_returns.write \
.mode("overwrite") \
.option("replaceWhere", f"year={year_int} AND month={month_int}") \
.saveAsTable("retail_analytics.portfolio.stock_returns")

# COMMAND ----------

# MF RETURNS

mf_returns = spark.sql(f"""
WITH cte AS (
SELECT 
    p.symbol,
    p.invested_amount,
    p.total_value,
    p.year,
    p.month,
    amfi.scheme_code
FROM portfolio_zerodha p
JOIN amfi_data amfi
ON p.isin = amfi.isin
WHERE p.symbol != 'GOLDBEES'
)

SELECT 
    c.symbol,
    c.invested_amount,
    c.total_value,
    ROUND(c.total_value * mf.monthly_return_pct / 100, 2) AS profit_loss,
    c.year,
    c.month
FROM cte c
JOIN mf_price_history mf
ON c.scheme_code = mf.scheme_code
AND c.year = mf.year
AND c.month = mf.month
""")

mf_returns.write \
.mode("overwrite") \
.option("replaceWhere", f"year={year_int} AND month={month_int}") \
.saveAsTable("retail_analytics.portfolio.mf_returns")

# COMMAND ----------

# DBTITLE 1,Cell 11
# STOCK PORTFOLIO

stock_portfolio = spark.sql(f"""
SELECT 
'STOCKS' AS portfolio_type,
ROUND(SUM(invested_amount),2) invested_amount,
ROUND(SUM(total_value),2) total_value,
ROUND(SUM(total_value + profit_loss),2) month_end_value,
month,
year
FROM retail_analytics.portfolio.stock_returns
WHERE year = {year_int} AND month = {month_int}
GROUP BY month, year
""")

stock_portfolio.write \
.mode("overwrite") \
.option("replaceWhere", f"year={year_int} AND month={month_int}") \
.option("mergeSchema", "true")\
.saveAsTable("retail_analytics.portfolio.stock_portfolio")
stock_portfolio.createOrReplaceTempView("stock_portfolio")

# COMMAND ----------

# DBTITLE 1,Cell 12
# MF PORTFOLIO

mf_portfolio = spark.sql(f"""
SELECT 
'MF' AS portfolio_type,
ROUND(SUM(invested_amount),2) invested_amount,
ROUND(SUM(total_value),2) total_value,
ROUND(SUM(total_value + profit_loss),2) month_end_value,
month,
year
FROM retail_analytics.portfolio.mf_returns
WHERE year = {year_int} AND month = {month_int}
GROUP BY month, year
""")

mf_portfolio.write \
.mode("overwrite") \
.option("replaceWhere", f"year={year_int} AND month={month_int}") \
.option("mergeSchema", "true") \
.saveAsTable("retail_analytics.portfolio.mf_portfolio")
mf_portfolio.createOrReplaceTempView("mf_portfolio")

# COMMAND ----------

consolidated_df=spark.sql(""" with portfolio_returns as (
select * from mf_portfolio
union 
select * from stock_portfolio)
select 'MF+STOCK' as portfolio_type,round(sum(invested_amount),2) as invested_amount,round(sum(mnt_stock_portfolio_value),2) as mnt_stock_portfolio_value,round(sum(month_end_value),2) as month_end_value,round(sum(month_profit_loss),2) as month_profit_loss,round(sum(loss_profit_pct_month),2) as loss_profit_pct_month,month,year from portfolio_returns
group by month,year
order by year,month
                          """)

consolidated.write \
.mode("overwrite") \
.option("mergeSchema", "true") \
.option("replaceWhere", f"year={year_int} AND month={month_int}") \
.saveAsTable("retail_analytics.portfolio.consolidated_portfolio")

# COMMAND ----------

display(consolidated)

# COMMAND ----------

# stock_returns = spark.table('retail_analytics.portfolio.stock_returns')
# stock_returns.coalesce(1).write.mode('append').option('header', True).csv('/Volumes/retail_analytics/portfolio/amfi_data/stock_returns.csv')


# mf_returns = spark.table('retail_analytics.portfolio.mf_returns')
# mf_returns.coalesce(1).write.mode('append').option('header', True).csv('/Volumes/retail_analytics/portfolio/amfi_data/mf_returns.csv')

# stock_portfolio = spark.table('retail_analytics.portfolio.stock_portfolio')
# stock_portfolio.coalesce(1).write.mode('append').option('header', True).csv('/Volumes/retail_analytics/portfolio/amfi_data/stock_portfolio.csv')

# mf_portfolio = spark.table('retail_analytics.portfolio.mf_portfolio')
# mf_portfolio.coalesce(1).write.mode('append').option('header', True).csv('/Volumes/retail_analytics/portfolio/amfi_data/mf_portfolio.csv')

# consolidated_portfolio = spark.table('retail_analytics.portfolio.consolidated_portfolio')
# consolidated_portfolio.coalesce(1).write.mode('append').option('header', True).csv('/Volumes/retail_analytics/portfolio/amfi_data/consolidated_portfolio.csv')

# COMMAND ----------

mf_returns = spark.table('retail_analytics.portfolio.mf_returns')

mf_returns.coalesce(1).write \
.mode('overwrite') \
.option('header', True) \
.csv('/Volumes/retail_analytics/portfolio/amfi_data/mf_returns.csv')


stock_portfolio = spark.table('retail_analytics.portfolio.stock_portfolio')

stock_portfolio.coalesce(1).write \
.mode('overwrite') \
.option('header', True) \
.csv('/Volumes/retail_analytics/portfolio/amfi_data/stock_portfolio.csv')


mf_portfolio = spark.table('retail_analytics.portfolio.mf_portfolio')

mf_portfolio.coalesce(1).write \
.mode('overwrite') \
.option('header', True) \
.csv('/Volumes/retail_analytics/portfolio/amfi_data/mf_portfolio.csv')


consolidated_portfolio = spark.table('retail_analytics.portfolio.consolidated_portfolio')

consolidated_portfolio.coalesce(1).write \
.mode('overwrite') \
.option('header', True) \
.csv('/Volumes/retail_analytics/portfolio/amfi_data/consolidated_portfolio.csv')

# COMMAND ----------

path=['/Volumes/retail_analytics/portfolio/amfi_data/stock_returns.csv','/Volumes/retail_analytics/portfolio/amfi_data/mf_returns.csv','/Volumes/retail_analytics/portfolio/amfi_data/stock_portfolio.csv','/Volumes/retail_analytics/portfolio/amfi_data/mf_portfolio.csv','/Volumes/retail_analytics/portfolio/amfi_data/consolidated_portfolio.csv']
filename=['stock_returns.csv','mf_returns.csv','stock_portfolio.csv','mf_portfolio.csv','consolidated_portfolio.csv']
for i, n in zip(path, filename):
    print(i, n)
    rename_and_cleanup_csv(i, n)
# dbutils.fs.rm('/Volumes/retail_analytics/portfolio/amfi_data/stock_returns.csv')
# dbutils.fs.rm('/Volumes/retail_analytics/portfolio/amfi_data/mf_returns.csv')

