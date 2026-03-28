# dags/dbt_clickstream_layered.py
from __future__ import annotations

from datetime import datetime
import json
import urllib.request

# Airflow 3.x 권장 import (없으면 fallback)
try:
    from airflow.sdk.dag import dag
except Exception:
    from airflow.decorators import dag

try:
    from airflow.providers.standard.operators.empty import EmptyOperator
except Exception:
    from airflow.operators.empty import EmptyOperator

try:
    from airflow.providers.standard.operators.python import PythonOperator
except Exception:
    from airflow.operators.python import PythonOperator

from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

# =========================
# Host paths (VM)
# =========================
HOST_DBT_ROOT = "/path/to/40-dbt"
HOST_DBT_DOTDBT = "/path/to/40-dbt/.dbt"
HOST_GCP_KEY = "/path/to/secrets/gcp-key.json"

# dbt image
DBT_IMAGE = "40-dbt-dbt"

# Container paths
CONTAINER_ROOT = "/usr/app"
CONTAINER_PROJECT_DIR = "/usr/app/my_dbt_project"
CONTAINER_DOTDBT = "/root/.dbt"
CONTAINER_GCP_KEY = "/usr/app/gcp-key.json"

DBT_MOUNTS = [
    Mount(target=CONTAINER_ROOT, source=HOST_DBT_ROOT, type="bind"),
    Mount(target=CONTAINER_DOTDBT, source=HOST_DBT_DOTDBT, type="bind"),
    Mount(target=CONTAINER_GCP_KEY, source=HOST_GCP_KEY, type="bind", read_only=True),
]

# =========================
# Jinja templates for window
# - dag_run.conf.start_date/end_date 우선
# - 없으면 기본 2024 전체
# =========================
WINDOW_START = "{{ (dag_run.conf.get('start_date') if dag_run and dag_run.conf else None) or '2024-01-01' }}"
WINDOW_END = "{{ (dag_run.conf.get('end_date') if dag_run and dag_run.conf else None) or '2024-12-31' }}"

# (옵션) 단일 배치일도 유지하고 싶으면 conf.batch_date 우선
BATCH_DATE = """{{
    (dag_run.conf.get('batch_date') if dag_run and dag_run.conf else None)
    or (logical_date.strftime('%Y-%m-%d') if (logical_date is defined and logical_date) else None)
    or (dag_run.logical_date.strftime('%Y-%m-%d') if (dag_run and dag_run.logical_date) else None)
    or '1970-01-01'
}}"""

# dbt vars: 따옴표 꼬임 적은 YAML 스타일
DBT_VARS = (
    "{"
    f"start_date: \"{WINDOW_START}\", "
    f"end_date: \"{WINDOW_END}\", "
    f"batch_date: \"{BATCH_DATE}\""
    "}"
)

def dbt_cmd(action: str, selector: str) -> list[str]:
    return [
        "bash",
        "-lc",
        f"cd {CONTAINER_PROJECT_DIR} && dbt {action} --select {selector} --vars '{DBT_VARS}'",
    ]

def _slack_post(webhook_url: str, text: str) -> None:
    payload = json.dumps({"text": text}).encode("utf-8")
    req = urllib.request.Request(
        webhook_url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        _ = resp.read()

def notify_slack_fn(**context):
    """
    슬랙 폭탄 방지 기본값:
    - manual 트리거만 알림 보냄
    - backfill/catchup은 기본적으로 스킵
    필요하면 dag_run.conf에 {"notify": true} 넣으면 강제 발송 가능
    """
    dag_run = context.get("dag_run")
    run_id = dag_run.run_id if dag_run else ""
    conf = (dag_run.conf if (dag_run and dag_run.conf) else {}) or {}

    # 폭탄 방지: 기본은 manual만
    notify_override = bool(conf.get("notify", False))
    is_manual = run_id.startswith("manual__")

    if not (is_manual or notify_override):
        print(f"[notify_slack] skip (run_id={run_id}, notify_override={notify_override})")
        return

    # Airflow Variable에서 가져오기: UI → Admin → Variables → SLACK_WEBHOOK_URL
    # Variable 템플릿을 안 쓰고, 실행 시점에 context로 직접 읽고 싶으면 아래처럼 사용 가능.
    # 여기서는 env 주입 없이 Variable을 import해서 직접 읽어도 되지만,
    # Airflow 3에서 Variable 접근권한/환경 따라 이슈가 있어 conf로 받거나 UI Variable 추천.
    from airflow.models import Variable
    webhook_url = Variable.get("SLACK_WEBHOOK_URL", default_var="")

    if not webhook_url:
        raise ValueError("SLACK_WEBHOOK_URL is empty (Airflow Variable)")

    window_start = conf.get("start_date", "2024-01-01")
    window_end = conf.get("end_date", "2024-12-31")

    text = (
        "✅ *dbt_clickstream_layered 성공*\n\n"
        f"📊 <https://lookerstudio.google.com/reporting/51186da6-734e-425e-824a-f2063ac1a51a|Looker 대시보드 바로가기>\n\n"
        f"- Gold marts 정상\n- DQ PASS\n- run_id: { run_id }"
        )

    _slack_post(webhook_url, text)

@dag(
    dag_id="dbt_clickstream_layered",
    start_date=datetime(2026, 1, 10),
    schedule="0 3 * * *",  # 03:00 UTC (KST 12:00)
    catchup=True,
    max_active_runs=3,
    tags=["dbt", "clickstream"],
)
def dbt_clickstream_layered():
    start = EmptyOperator(task_id="start")

    # ✅ DockerOperator 공통
    # - environment는 "여기서 한 번만" 넣고, task별로 override하지 않게 구성
    common = dict(
        image=DBT_IMAGE,
        docker_url="unix:///var/run/docker.sock",
        tls_verify=False,
        mounts=DBT_MOUNTS,
        mount_tmp_dir=False,
        auto_remove="success",
        working_dir=CONTAINER_PROJECT_DIR,
        environment={
            "DBT_PROFILES_DIR": CONTAINER_DOTDBT,
            "GOOGLE_APPLICATION_CREDENTIALS": CONTAINER_GCP_KEY,
        },
        tty=False,
        timeout=600,
        do_xcom_push=False,
    )

    seed_product_master = DockerOperator(
        task_id="seed_product_master",
        command=dbt_cmd("seed", "product_master"),
        **common,
    )

    run_bronze = DockerOperator(
        task_id="run_bronze",
        command=dbt_cmd("run", "tag:bronze"),
        **common,
    )

    run_silver = DockerOperator(
        task_id="run_silver",
        command=dbt_cmd("run", "tag:silver"),
        **common,
    )

    test_silver = DockerOperator(
        task_id="test_silver",
        command=dbt_cmd("test", "tag:silver"),
        **common,
    )

    run_gold = DockerOperator(
        task_id="run_gold",
        command=dbt_cmd("run", "tag:gold"),
        **common,
    )

    notify_slack = PythonOperator(
        task_id="notify_slack",
        python_callable=notify_slack_fn,
        trigger_rule=TriggerRule.ALL_SUCCESS,  # upstream 다 성공해야만 발송
        retries=0,  # 슬랙 중복 방지(실패 재시도 금지)
    )

    end = EmptyOperator(task_id="end")

    start >> seed_product_master >> run_bronze >> run_silver >> test_silver >> run_gold >> notify_slack >> end

dag = dbt_clickstream_layered()
