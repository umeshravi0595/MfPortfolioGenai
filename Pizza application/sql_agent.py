class SQLAgent:
    def __init__(self, registry):
        self.registry = registry

    def plan(self, intent):
        return {
            "entity": intent["entity"],
            "metric": intent["metric"],
            "time_grain": intent.get("time_grain"),
            "window": intent.get("window"),
            "trend": intent.get("trend"),
            "ranking": intent.get("ranking"),
            "filters": intent.get("filters", []),
            "joins": intent.get("joins", [])
        }

    def resolve_tables(self, plan):
        base_table = self.registry["entities"][plan["entity"]]["table"]
        joins = []

        for j in plan["joins"]:
            join_cfg = self.registry["entities"][plan["entity"]]["joins"][j]
            joins.append(
                f"JOIN {join_cfg['table']} USING ({join_cfg['on']})"
            )

        return base_table, joins

    def generate_sql(self, plan):
        metric_cfg = self.registry["metrics"][plan["metric"]]
        metric_sql = metric_cfg["sql"]

        base_table, joins = self.resolve_tables(plan)

        select_cols = [plan["entity"]]
        group_cols = [plan["entity"]]

        if plan["time_grain"]:
            select_cols.append(plan["time_grain"])
            group_cols.append(plan["time_grain"])

        select_expr = f"{metric_sql} AS metric_value"

        sql = f"""
        WITH base AS (
          SELECT
            {', '.join(select_cols)},
            {select_expr}
          FROM {base_table}
          {' '.join(joins)}
        """

        if plan["filters"]:
            sql += f" WHERE {' AND '.join(plan['filters'])}"

        sql += f" GROUP BY {', '.join(group_cols)} )"

        # Window logic
        if plan["window"]:
            window_sql = self.registry["window_patterns"][plan["window"]]["sql"].format(
                time=plan["time_grain"],
                metric="metric_value"
            )
            sql += f"""
            , ranked AS (
              SELECT *,
                     {window_sql} AS rank
              FROM base
            )
            """

        # Trend logic
        if plan["trend"]:
            trend_sql = self.registry["trend_patterns"][plan["trend"]]["sql"].format(
                entity=plan["entity"],
                time=plan["time_grain"],
                metric="metric_value"
            )
            sql += f"""
            , trended AS (
              SELECT *,
                     {trend_sql} AS trend_value
              FROM base
            )
            """

        final_table = "ranked" if plan["window"] else "trended" if plan["trend"] else "base"

        sql += f" SELECT * FROM {final_table}"

        if plan["ranking"]:
            rank_cfg = self.registry["ranking"][plan["ranking"]]
            sql += f" ORDER BY metric_value {rank_cfg['order']} LIMIT {rank_cfg['limit']}"

        return sql
