# 📦 async-etl-pipeline-template

> 실전형 비동기 데이터 수집 · 전처리 · 분석 파이프라인 템플릿

Python + aiohttp + pandas 기반으로 설계된, **재시도 로직/전처리/고급분석**까지 포함된 고성능 데이터 파이프라인 템플릿. 
주석 일부러 달았음, 실제 서비스 확장을 고려한 구조, 테스트 가능한 아키텍처를 제공.

---

## 🚀 Features

- ✅ **비동기 외부 API 수집** (`aiohttp`, `asyncio.Semaphore`, `재시도 지수 백오프`)
- ✅ **전처리 구조 분리** (`DataTransformer`)
- ✅ **고급 분석 구조화** (`DataAnalyzer`)
- ✅ **DI 기반 오케스트레이터 설계** (`DataPipelineOrchestrator`)
- ✅ **실전 대응 로깅** (`logging`, `exc_info`, 사용자 정의 config)
- ✅ **테스트/확장성을 고려한 구조**

---

## 🧱 Architecture

```plaintext
main()
  └─ aiohttp.ClientSession
       └─ ExternalAPIService (API 호출)
       └─ DataTransformer (전처리)
       └─ DataAnalyzer (고급 분석)
       └─ DataPipelineOrchestrator (전체 흐름)
```

---

## 📦 설치 방법

```bash
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

> ※ `async-retrying`, `aiohttp`, `pandas`는 필수

---

## ✅ 실행 예시

```bash
python main.py
```

```json
{
  "user_post_counts": {"1": 2, ...},
  "top_5_active_users": [["1", 5], ...]
}
```

---

## 🧪 테스트 (선택)

```bash
pytest tests/
```

---

## 📁 파일 구조

```bash
.
├── main.py                     # entry point
├── services/
│   ├── api_service.py         # API 호출 전용 서비스
│   ├── transformer.py         # 전처리 로직
│   └── analyzer.py            # 분석 로직
├── orchestrator.py            # 전체 파이프라인 조합
├── config.py                  # 전역 설정값
├── requirements.txt
└── README.md
```

---

## 📌 사용 예시 (공공데이터 수집, 로그 집계, 쇼츠 분석 등)

- 공공 API 또는 외부 크롤링 기반 데이터 수집 → 병렬 처리
- Pandas 기반 정형 분석 or 마케팅 리포트 자동화
- 유튜브, 인스타 등 메타 데이터 수집 → 전처리 + 통계

---

## 🧠 설계 컨셉 (Why 이 구조인가)

- **SOLID 원칙 준수 (Single Responsibility 분리)**
- **Retry / Timeout / 동시성 등 실전 인프라 대응 고려**
- **주석은 'FM 스타일'로 설계 이유와 처리 흐름 설명**
- **진입장벽 낮추되 확장성은 열려 있음 (testable)**

---

## 🛠 requirements.txt 예시

```txt
aiohttp
pandas
async-retrying
pydantic  # (선택)
pytest    # (테스트용)
```

---

## 🧪 test 예시 코드

```python
# tests/test_api_mock.py
import pytest
from unittest.mock import AsyncMock

@pytest.mark.asyncio
async def test_fetch_data():
    from services.api_service import ExternalAPIService
    dummy_session = AsyncMock()
    dummy_session.get.return_value.__aenter__.return_value.json = AsyncMock(return_value={"test": 1})

    svc = ExternalAPIService(dummy_session)
    data = await svc.fetch_data("test-endpoint")
    assert data == {"test": 1}
```

---

## ✍️ 저작권 / 라이선스
Kyu Ho, CHOI
email: smicer.prederik@gmail.com

MIT License. 자유롭게 사용하시되, 출처 표시해주시면 감사합니다.
