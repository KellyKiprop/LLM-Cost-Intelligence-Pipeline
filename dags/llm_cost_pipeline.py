import os
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

DBT_PROJECT_DIR = os.path.expanduser("/home/kelly/Documents/llm-cost-tracker/llm_costs")
VENV_PYTHON     = os.path.expanduser("/home/kelly/Documents/llm-cost-tracker/venv/bin/python")
DBT_BIN         = os.path.expanduser("/home/kelly/Documents/llm-cost-tracker/venv/bin/dbt")
SPARK_JOB       = os.path.expanduser("/home/kelly/Documents/llm-cost-tracker/spark/streaming_job.py")
SIMULATOR       = os.path.expanduser("/home/kelly/Documents/llm-cost-tracker/proxy/simulator.py")
SPARK_HOME      = os.path.expanduser("/home/kelly/Documents/llm-cost-tracker")

PG_HOST     = "your host"
PG_PORT     = port
PG_USER     = "user"
PG_PASSWORD = "password"
PG_DB       = "your db"

default_args = {
    "owner":            "kelly",
    "retries":          1,
    "retry_delay":      timedelta(minutes=2),
    "email_on_failure": False,
}

@dag(
    dag_id="llm_cost_pipeline",
    description="Full pipeline: simulate → kafka → spark → dbt → anomalies → report",
    schedule_interval="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["llm", "cost", "data-engineering"],
)
def llm_cost_pipeline():

    # Step 1 — Start Spark in background (will auto-exit after 3 mins)
    start_spark = BashOperator(
        task_id="start_spark_streaming",
        bash_command=(
            f"cd {SPARK_HOME} && "
            f"nohup {VENV_PYTHON} {SPARK_JOB} "
            f"> /tmp/spark_streaming.log 2>&1 & "
            f"echo $! > /tmp/spark_pid.txt && "
            f"echo 'Spark started with PID' $(cat /tmp/spark_pid.txt)"
        ),
    )

    # Step 2 — Run simulator (fires 200 requests to proxy → Kafka)
    run_simulator = BashOperator(
        task_id="run_simulator",
        bash_command=f"{VENV_PYTHON} {SIMULATOR}",
        execution_timeout=timedelta(minutes=10),
    )

    # Step 3 — Wait for Spark to finish processing all events
    wait_for_spark = BashOperator(
        task_id="wait_for_spark",
        bash_command=(
            "echo 'Waiting for Spark to finish processing...' && "
            "sleep 200 && "
            "echo 'Done waiting.'"
        ),
    )

    # Step 4 — dbt staging
    run_dbt_staging = BashOperator(
        task_id="run_dbt_staging",
        bash_command=(
            f"{DBT_BIN} run --select staging "
            f"--project-dir '{DBT_PROJECT_DIR}' "
            f"--profiles-dir ~/.dbt"
        ),
    )

    # Step 5 — dbt marts
    run_dbt_marts = BashOperator(
        task_id="run_dbt_marts",
        bash_command=(
            f"{DBT_BIN} run --select marts "
            f"--project-dir '{DBT_PROJECT_DIR}' "
            f"--profiles-dir ~/.dbt"
        ),
    )

    # Step 6 — dbt tests
    run_dbt_tests = BashOperator(
        task_id="run_dbt_tests",
        bash_command=(
            f"{DBT_BIN} test "
            f"--project-dir '{DBT_PROJECT_DIR}' "
            f"--profiles-dir ~/.dbt"
        ),
    )

    # Step 7 — Anomaly detection
    @task
    def check_anomalies():
        import psycopg2
        conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT, user=PG_USER,
            password=PG_PASSWORD, dbname=PG_DB, sslmode="require",
        )
        cur = conn.cursor()
        cur.execute("""
            SELECT team, model, hourly_cost_usd, cost_ratio
            FROM analytics.fct_cost_anomalies
            WHERE event_hour >= now() - interval '1 hour'
            AND is_anomaly = true
            ORDER BY cost_ratio DESC
        """)
        anomalies = cur.fetchall()
        cur.close(); conn.close()
        if anomalies:
            print("ANOMALIES DETECTED:")
            for team, model, cost, ratio in anomalies:
                print(f"  team={team} model={model} cost=${cost:.6f} ratio={ratio}x")
        else:
            print("No anomalies detected in the last hour.")
        return len(anomalies)

    # Step 8 — Cost report
    @task
    def generate_cost_report():
        import psycopg2
        conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT, user=PG_USER,
            password=PG_PASSWORD, dbname=PG_DB, sslmode="require",
        )
        cur = conn.cursor()
        cur.execute("""
            SELECT team,
                   SUM(call_count)                        AS total_calls,
                   ROUND(SUM(total_cost_usd)::numeric, 6) AS total_cost_usd,
                   ROUND(AVG(avg_latency_ms)::numeric, 0) AS avg_latency_ms
            FROM analytics.fct_cost_by_team_daily
            GROUP BY team
            ORDER BY total_cost_usd DESC
        """)
        rows = cur.fetchall()
        cur.close(); conn.close()
        print("\n===== LLM COST REPORT =====")
        print(f"{'Team':<15} {'Calls':>8} {'Cost (USD)':>12} {'Avg Latency':>12}")
        print("-" * 50)
        total = 0
        for team, calls, cost, latency in rows:
            print(f"{team:<15} {calls:>8} {float(cost):>12.6f} {float(latency):>10.0f}ms")
            total += float(cost)
        print("-" * 50)
        print(f"{'TOTAL':<15} {'':>8} {total:>12.6f}")
        return total

    anomaly_count = check_anomalies()
    cost_report   = generate_cost_report()

    (
        start_spark
        >> run_simulator
        >> wait_for_spark
        >> run_dbt_staging
        >> run_dbt_marts
        >> run_dbt_tests
        >> anomaly_count
        >> cost_report
    )

llm_cost_pipeline()
