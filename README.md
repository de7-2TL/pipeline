# pipeline

## 프로젝트 소개


### Dashboard
![dashboard_1.png](docs/images/dashboard_1.png)
![dashboard_2.png](docs/images/dashboard_2.png)

## Data Flow
![data_flow.png](docs/images/data_flow.png)

1. **Data Fetching (Source -> S3 Data Lake)**
   - yfinance API를 통해 섹터, 산업, 회사 정보를 가져와 Snowflake의 테이블을 갱신
   - DAG가 매시간 실행되어 yfinance API에서 모든 회사의 최신 뉴스를 수집
   - DAG가 15분 간격으로 실행되어 yfinance API에서 개별 종목 및 섹터별 주가를 수집
2. **Data Loading (S3 Lake → Snowflake Warehouse)**
   - S3에 저장된 주식 데이터를 클리닝 및 변환 후 Snowflake에 적재
   - 뉴스 데이터는 Snowflake에 `EXTERNAL TABLE`로 생성
   - 
3. **Data Transformation (dbt)**
   - 저장된 Snowflake의 Row Data를 기반으로 다양한 분석용 테이블 생성  
        _가장 변동이 큰 섹터 및 섹터의 산업 정보, 해당 섹터의 뉴스 정보_
## 🛠️ 기술 스택

### Orchestration

<div>
    <img src="https://img.shields.io/badge/apacheairflow-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white" alt=""/>
</div>

### Storage

<div>
    <img src="https://img.shields.io/badge/snowflake-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white" alt=""/>
    <img src="https://img.shields.io/badge/AWS_S3-6DB33F?style=for-the-badge&logoColor=white" alt=""/>
</div>

### ETL/ELT

<div>
    <img src="https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white" alt=""/>
    <img src="https://img.shields.io/badge/sql-231F20?style=for-the-badge&logoColor=white" alt=""/>
    <img src="https://img.shields.io/badge/pandas-150458?style=for-the-badge&logo=pandas&logoColor=white" alt=""/>
</div>

### 시각화

<div>
    <img src="https://img.shields.io/badge/apachesuperset_(preset)-20A6C9?style=for-the-badge&logo=apachesuperset&logoColor=white" alt=""/>
</div>

### 인프라

<div>
    <img src="https://img.shields.io/badge/git-F05032?style=for-the-badge&&logo=git&logoColor=white" alt=""/>
    <img src="https://img.shields.io/badge/git_flow-F05032?style=for-the-badge&&logo=git&logoColor=white" alt=""/>
    <img src="https://img.shields.io/badge/githubactions-2088FF?style=for-the-badge&&logo=githubactions&logoColor=white" alt=""/>
    <img src="https://img.shields.io/badge/slack-4A154B?style=for-the-badge&logo=slack&logoColor=white" alt=""/>
    <img src="https://img.shields.io/badge/uv-DE5FE9?style=for-the-badge&&logo=uv&logoColor=white" alt=""/>
</div>

## Getting Started

> docker-compose 기반의 Airflow 환경을 설정합니다.

- **Airflow 실행**

```bash
git clone https://github.com/de7-2TL/pipeline.git
cd pipeline/scripts

sh docker-compose-up.sh
```

- **Local 환경 구축**

```bash
uv sync
```

### Airflow Variables & Connections

- **connections:**  
    snowflake_conn: Snowflake connection  
    aws_conn : Amazon Web Service connection  

- **pools:**  
    sector_upload_pool : 기본 slot은 3개. 사양에 따라 조정

- **variables:**  
    AWS_BUCKET: bucket 이름

### Scripts

- docker-compose-up.sh : Docker Compose를 통해 Airflow 환경을 실행합니다.
- sync-pip-from-reqs.sh : requirements.txt 파일에 명시된 패키지를 Docker 컨테이너에 동기화합니다.

## Project Structure

```
.
├── ansible # Docker Container에 패키지 설치를 위한 Ansible 플레이북
├── scripts # Env 관련 실행 스크립트
├── src # Airflow Home Directory
│   ├── Dockerfile
│   ├── dags
│   │   └── dbt # DBT Home Directory
│   ├── docker-compose.yaml
│   ├── plugins
│   │   ├── hooks # Custom Hook
│   │   ├── operators # Custom Operator
│   │   └── utils # 유틸리티 함수
│   ├── requirements-base.txt # Airflow 기본 패키지
│   └── requirements.txt # 추가 패키지
├── tests # Pytest 기반의 테스트 코드
└── uv.lock
```

### Code Conventions

> Based on ~/ruff.toml

- DAG: 동사_*_dag.py (예: extract_user_data_dag.py)  
  _DBT DAG는 dbt_*_dag.py (예: dbt_transform_dag.py) #DBT를 동사로 간주합니다_
- DAG ID: `file_name`에서 subfix(_dag) 제거

## 🧑‍💻 팀원 소개 & Roles

| <img src="https://avatars.githubusercontent.com/u/74689888?v=4" width="130" height="130"> | <img src="https://avatars.githubusercontent.com/u/96654005?v=4" width="130" height="130"> | <img src="https://avatars.githubusercontent.com/u/63443366?v=4" width="130" height="130"> | <img src="https://avatars.githubusercontent.com/u/171410678?v=4" width="130" height="130"> |
|:-----------------------------------------------------------------------------------------:|:-----------------------------------------------------------------------------------------:|:-----------------------------------------------------------------------------------------:|:------------------------------------------------------------------------------------------:|
|                           [김민국](https://github.com/minguk-cucu)                           |                             [박제현](https://github.com/JehyunP)                             |                            [정성길](https://github.com/seon99il)                             |                             [천준규](https://github.com/done0173)                             |

### 📑 주요 담당 업무

#### **김민국**

> 담당 업무 Title

- feature1
- feature2
- feature3

#### **박제현**

> 담당 업무 Title

- feature1
- feature2
- feature3

#### **정성길**

> 담당 업무 Title

- feature1
- feature2
- feature3

#### **천준규**

> 담당 업무 Title

- feature1
- feature2
- feature3
