# 📊 GCP Clickstream Data Platform

## 1. Overview
This project builds an end-to-end data pipeline for analyzing clickstream data using a modern data stack on GCP.

The pipeline ingests raw event data, processes it through streaming and batch layers, and transforms it into analytics-ready datasets using the Medallion Architecture (Bronze → Silver → Gold).

---

## 2. Architecture

CSV → Kafka → Spark → GCS (Parquet)
↓
BigQuery (Bronze)
↓
dbt (Silver → Gold)
↓
Looker Studio Dashboard
↓
Airflow Orchestration + Slack Alerts

---

## 3. Tech Stack

- **Cloud**: GCP (BigQuery, GCS)
- **Streaming**: Kafka
- **Processing**: Spark (Structured Streaming)
- **Transformation**: dbt
- **Orchestration**: Airflow
- **Visualization**: Looker Studio
- **Containerization**: Docker

---

## 4. Data Pipeline Flow

### 1) Data Ingestion
- Clickstream data is replayed from a CSV dataset
- Events are streamed into Kafka topics

### 2) Data Processing
- Spark consumes Kafka messages
- Writes data into GCS as Parquet files

### 3) Data Warehousing
- BigQuery reads external data from GCS
- Bronze layer stores raw data

### 4) Data Transformation (dbt)
- Silver: data cleaning, parsing, standardization
- Gold: KPI-focused marts (funnel, user behavior, DQ metrics)

### 5) Orchestration (Airflow)
- DAG controls full pipeline execution
- Includes retry logic and failure handling
- Slack alerts for pipeline status

---

## 5. Data Modeling (Medallion Architecture)

- **Bronze**
  - Raw event-level data (append-only)
  - Preserves original data

- **Silver**
  - Cleaned and structured data
  - Deduplication, schema normalization

- **Gold**
  - Business-ready metrics
  - Funnel analysis, user metrics, DQ monitoring

---

## 6. Key Features

- End-to-end streaming + batch pipeline
- Data quality validation using dbt tests
- Failure-safe orchestration with Airflow
- Modular and scalable architecture
- Container-based reproducibility (Docker)

---

## 7. Sample Outputs

> 📌 Looker Dashboard (Example)
- Funnel Conversion Analysis
- Daily User Metrics
- Data Quality Monitoring

(📸 Add screenshots here)

---

## 8. How to Run

### Prerequisites
- Docker & Docker Compose
- GCP credentials (service account key)

### Setup

```bash
git clone https://github.com/<your-id>/ecommerce.git
cd ecommerce/github-export
```

Run
```
docker compose up -d
```

Airflow
	•	http://localhost:8080

⸻

9. Environment Variables

Sensitive configurations are excluded from the repository.

Example:

```
HOST_GCP_KEY=/path/to/gcp-key.json
```


⸻

## 10. Limitations
	•	Local environment setup required for full execution
	•	GCP billing required for full-scale operation
	•	일부 설정은 환경 변수 기반으로 구성 필요

⸻

## 11. Future Improvements
	•	Real-time streaming pipeline (Kafka → Spark → BigQuery)
	•	Data freshness monitoring
	•	Observability integration (e.g., Datadog)
	•	Advanced anomaly detection (funnel drop-offs)

⸻

## 12. What I Learned
	•	Designing scalable data pipelines using modern data stack
	•	Managing data quality using dbt tests
	•	Building resilient orchestration workflows
	•	Handling environment separation (dev vs production)
	•	Securing sensitive configuration in real-world projects


⸻


# 📊 GCP 기반 Clickstream 데이터 파이프라인 구축

## 1. 프로젝트 개요

본 프로젝트는 클릭스트림 데이터를 기반으로 **실시간 + 배치 데이터 파이프라인**을 구축하고,  
분석 가능한 형태로 가공하여 **비즈니스 지표(KPI)**를 도출하는 것을 목표로 합니다.

Kafka, Spark, Airflow, dbt, BigQuery 등 **모던 데이터 스택**을 활용하여  
데이터 수집 → 처리 → 적재 → 분석까지의 전체 흐름을 구현했습니다.

⸻

## 2. 전체 아키텍처

```
CSV → Kafka → Spark → GCS (Parquet)
↓
BigQuery (Bronze)
↓
dbt (Silver → Gold)
↓
Looker Studio Dashboard
↓
Airflow Orchestration + Slack Alert
```

⸻


## 3. 기술 스택

- **Cloud**: GCP (BigQuery, GCS)
- **Streaming**: Kafka
- **Processing**: Spark (Structured Streaming)
- **Transformation**: dbt
- **Orchestration**: Airflow
- **Visualization**: Looker Studio
- **Infra**: Docker

⸻

## 4. 데이터 파이프라인 흐름

### 1) 데이터 수집
- CSV 기반 클릭스트림 데이터를 Replay하여 Kafka로 전송
- 이벤트 단위 데이터 스트리밍 처리

### 2) 데이터 처리
- Spark에서 Kafka 메시지를 소비
- GCS에 Parquet 형식으로 저장

### 3) 데이터 적재
- BigQuery에서 GCS 데이터를 외부 테이블로 연결
- Bronze 레이어에 원천 데이터 적재

### 4) 데이터 변환 (dbt)
- Silver: 정제, 스키마 표준화, 중복 제거
- Gold: KPI 중심 마트 생성 (Funnel, User, DQ)

### 5) 오케스트레이션 (Airflow)
- 전체 파이프라인 DAG 구성
- 실패 시 retry 및 Slack 알림
- 데이터 품질 테스트 통과 시에만 Gold 레이어 생성

⸻

## 5. 데이터 모델링 (Medallion Architecture)

### Bronze
- 원본 데이터 저장 (append-only)
- 데이터 보존 및 재처리 가능

### Silver
- 정제 및 구조화된 데이터
- 데이터 품질 개선 (dedup, parsing 등)

### Gold
- 비즈니스 지표 중심 데이터
- Funnel 분석, 사용자 행동 분석, DQ 모니터링

⸻

## 6. 주요 특징

- 스트리밍 + 배치 통합 데이터 파이프라인
- dbt 기반 데이터 품질 검증 (test)
- Airflow 기반 안정적인 orchestration
- Docker 기반 환경 재현 가능
- 모듈화된 구조로 확장성 고려

⸻

## 7. 결과 (대시보드)

> 📌 Looker Studio 기반 대시보드

- Funnel 전환율 분석
- 사용자 행동 분석
- 데이터 품질(DQ) 모니터링

⸻

## 8. 실행 방법

### 사전 준비
- Docker / Docker Compose
- GCP 서비스 계정 키

### 실행

```bash
git clone https://github.com/<your-id>/ecommerce.git
cd ecommerce/github-export
docker compose up -d
```

### Airflow 접속
	•	http://localhost:8080

⸻

## 9. 환경 변수

민감 정보는 repository에 포함하지 않고 환경 변수로 관리합니다.

예시:

```
HOST_GCP_KEY=/path/to/gcp-key.json
```

⸻

## 10. 한계 및 개선 방향
	•	로컬 환경 설정 의존성 존재
	•	GCP 비용 발생 가능
	•	일부 설정은 환경 변수 기반으로 추가 구성 필요

⸻

## 11. 향후 개선 방향
	•	실시간 스트리밍 처리 고도화
	•	데이터 freshness 모니터링
	•	Observability (Datadog 등) 연동
	•	Funnel 이탈 분석 고도화

⸻

## 12. 프로젝트를 통해 배운 점
	•	모던 데이터 스택 기반 파이프라인 설계 경험
	•	dbt를 활용한 데이터 품질 관리
	•	Airflow 기반 운영 자동화
	•	환경 분리 및 보안 설정 관리 경험
	•	실제 운영 환경을 고려한 구조 설계






