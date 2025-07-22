import asyncio
import aiohttp
import pandas as pd
from typing import List, Dict, Any, Tuple, Optional
import logging
import time
from functools import wraps
# pip install async-retrying (실제 사용 시 필요)
# from async_retrying import retry  # 실제 환경에서는 pip install async-retrying 필요
# pip install pydantic (실제 사용 시 필요)
# from pydantic import BaseModel, Field, ValidationError

# 로깅 설정: 엔터프라이즈 환경에 적합한 상세 로깅 구성
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        # logging.FileHandler("pipeline.log") # 실제 서비스 환경에서는 파일 또는 로깅 시스템 연동
    ]
)
logger = logging.getLogger(__name__)

# --- 설정 관리 (실제 프로젝트에서는 별도의 config.py 또는 환경 변수 활용) ---
class AppConfig:
    API_BASE_URL: str = "https://jsonplaceholder.typicode.com"
    REQUEST_TIMEOUT: int = 15
    RETRY_ATTEMPTS: int = 3
    RETRY_FACTOR: float = 0.5  # 지수 백오프를 위한 기본 지연 시간 (0.5s, 1s, 2s ...)
    MAX_CONCURRENT_REQUESTS: int = 10 # 동시 요청 제한

# --- 재시도 데코레이터 (async-retrying 라이브러리 가정) ---
# 실제 사용 시, pip install async-retrying 필요. 없으면 간단한 자체 구현으로 대체
def async_retry(attempts: int = AppConfig.RETRY_ATTEMPTS, factor: float = AppConfig.RETRY_FACTOR):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            for i in range(attempts):
                try:
                    return await func(*args, **kwargs)
                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                    if i < attempts - 1:
                        delay = factor * (2 ** i) # 지수 백오프
                        logger.warning(f"재시도 ({i+1}/{attempts}) - {func.__name__} 실패: {e}. {delay:.2f}초 후 재시도...")
                        await asyncio.sleep(delay)
                    else:
                        logger.error(f"최대 재시도 횟수 초과 - {func.__name__} 최종 실패: {e}")
                        raise # 최종 실패 시 예외 다시 발생
        return wrapper
    return decorator

# --- 데이터 모델 (pydantic 사용 예시 - 실제 사용 시 pip install pydantic 필요) ---
# class User(BaseModel):
#     id: int
#     name: str
#     email: str = Field(..., pattern=r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
#     username: str
#     phone: Optional[str] = None
#     website: Optional[str] = None

# class Post(BaseModel):
#     userId: int
#     id: int
#     title: str
#     body: str

class ExternalAPIService:
    """
    외부 API와의 통신을 담당하는 서비스 계층입니다.
    비동기 요청, 재시도 로직, 동시성 제어를 관리합니다.
    """
    def __init__(self, session: aiohttp.ClientSession):
        self._session = session
        self._semaphore = asyncio.Semaphore(AppConfig.MAX_CONCURRENT_REQUESTS) # 동시 요청 제한 세마포어

    @async_retry(attempts=AppConfig.RETRY_ATTEMPTS)
    async def fetch_data(self, endpoint: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        주어진 API 엔드포인트에서 데이터를 비동기적으로 가져옵니다.
        재시도 로직과 동시성 제어를 포함합니다.
        """
        url = f"{AppConfig.API_BASE_URL}/{endpoint}"
        async with self._semaphore: # 동시 요청 제한 적용
            try:
                # with aiohttp.Timeout(AppConfig.REQUEST_TIMEOUT): # aiohttp 3.x 이전 버전
                async with self._session.get(url, params=params, timeout=AppConfig.REQUEST_TIMEOUT) as response:
                    response.raise_for_status() # HTTP 4xx/5xx 에러 발생 시 예외
                    data = await response.json()
                    logger.info(f"데이터 성공적으로 가져옴: {endpoint}")
                    return data
            except aiohttp.ClientConnectorError as e:
                logger.error(f"네트워크 연결 오류 발생 ({endpoint}): {e}")
                raise # 네트워크 연결 오류는 재시도 후에도 치명적일 수 있음
            except aiohttp.ClientResponseError as e:
                logger.error(f"API 응답 오류 ({endpoint}): {e.status} - {e.message}")
                raise
            except asyncio.TimeoutError:
                logger.error(f"API 요청 시간 초과 ({endpoint})")
                raise
            except Exception as e:
                logger.error(f"예기치 않은 오류 발생 ({endpoint}): {e}")
                raise

class DataTransformer:
    """
    원시 데이터를 전처리하고 Pandas DataFrame으로 변환하는 책임을 가집니다.
    데이터 무결성 검사 및 정제 로직을 포함합니다.
    """
    def __init__(self):
        pass

    def preprocess(self, raw_data: Dict[str, List[Dict[str, Any]]]) -> pd.DataFrame:
        """
        가져온 원시 데이터를 Pandas DataFrame으로 변환하고 전처리합니다.
        데이터 유효성 검사 및 통합 로직을 포함합니다.
        """
        processed_dfs = {}
        for key, data_list in raw_data.items():
            if not data_list:
                logger.warning(f"'{key}' 데이터가 비어있어 전처리를 건너뜁니다.")
                continue

            try:
                # 옵션: Pydantic 모델을 통한 데이터 유효성 검사 및 정제 (주석 처리됨)
                # if key == 'users':
                #     validated_data = [User(**item).model_dump() for item in data_list]
                # elif key == 'posts':
                #     validated_data = [Post(**item).model_dump() for item in data_list]
                # else:
                #     validated_data = data_list
                # df = pd.DataFrame(validated_data)
                
                df = pd.DataFrame(data_list)
                
                # 공통 전처리 로직 (예: ID 컬럼 인덱싱)
                if 'id' in df.columns:
                    df = df.set_index('id', drop=False) # id 컬럼을 인덱스로 두되, 컬럼에도 유지
                
                # 엔드포인트별 특정 전처리 (예: 날짜 형식 변환)
                if key == 'posts' and 'created_at' in df.columns: # 가상 컬럼
                    df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
                
                # 컬럼명 충돌 방지를 위한 접두사 추가
                df = df.add_prefix(f'{key}_')
                processed_dfs[key] = df
                logger.info(f"'{key}' 데이터 전처리 완료. Shape: {df.shape}")

            # except ValidationError as e: # Pydantic 사용 시
            #     logger.error(f"'{key}' 데이터 유효성 검사 실패: {e}")
            except Exception as e:
                logger.error(f"'{key}' 데이터 전처리 중 예외 발생: {e}", exc_info=True) # exc_info로 스택 트레이스 출력
        
        if not processed_dfs:
            logger.warning("처리할 유효한 데이터프레임이 없어 빈 DataFrame을 반환합니다.")
            return pd.DataFrame()

        # 데이터프레임 병합 전략: 공통 키를 기준으로 외부 조인
        # 실제 시나리오에서는 각 데이터셋 간의 관계를 정의하고 적절한 Join 전략 (inner, left, outer) 사용
        # 예시: users와 posts를 userId 기준으로 병합
        final_df = None
        if 'users' in processed_dfs:
            final_df = processed_dfs['users']
        
        if 'posts' in processed_dfs:
            if final_df is not None:
                # 'users_id'와 'posts_userId'를 기준으로 병합 (컬럼명 조정 필요)
                # left_on='users_id'와 right_on='posts_userId'로 join할 수 있도록 컬럼명 조정 예시
                # 실제 데이터에서는 이 부분이 훨씬 복잡해질 수 있음.
                # 여기서는 단순히 인덱스 기반으로 병합하여 컬럼명 충돌을 피하는 방법 사용
                posts_df = processed_dfs['posts'].rename(columns={'posts_userId': 'users_id'}) # join을 위해 컬럼명 맞춤
                final_df = pd.merge(final_df, posts_df, left_index=True, right_on='users_id', how='left', suffixes=('_user', '_post'))
            else:
                final_df = processed_dfs['posts']

        if 'comments' in processed_dfs and final_df is not None:
            comments_df = processed_dfs['comments'].rename(columns={'comments_postId': 'posts_id'}) # join을 위해 컬럼명 맞춤
            final_df = pd.merge(final_df, comments_df, left_index=True, right_on='posts_id', how='left', suffixes=('_base', '_comment'))

        if final_df is None:
            logger.warning("모든 데이터프레임 병합 실패. 개별 처리된 데이터프레임 중 첫 번째를 반환합니다.")
            return next(iter(processed_dfs.values())) if processed_dfs else pd.DataFrame()
        
        logger.info(f"모든 데이터프레임 병합 완료. 최종 Shape: {final_df.shape}")
        return final_df

class DataAnalyzer:
    """
    전처리된 DataFrame을 기반으로 복잡한 데이터 분석을 수행합니다.
    비즈니스 인텔리전스 및 통찰력 도출을 위한 핵심 로직을 담당합니다.
    """
    def __init__(self):
        pass

    def perform_advanced_analysis(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        전처리된 DataFrame을 기반으로 복잡한 데이터 분석을 수행합니다.
        도메인 특화된 지표 계산 및 패턴 인식 로직을 포함합니다.
        """
        if df.empty:
            logger.warning("분석할 데이터가 없습니다. 빈 분석 결과를 반환합니다.")
            return {"status": "no_data_for_analysis"}

        analysis_results = {}
        
        try:
            # 예시 1: 사용자별 게시글 수 및 댓글 수 (병합된 데이터 기준)
            if 'users_id' in df.columns and 'posts_id' in df.columns and 'comments_id' in df.columns:
                # 각 사용자가 작성한 게시글 수
                user_post_counts = df.groupby('users_id')['posts_id'].nunique().to_dict()
                analysis_results['user_post_counts'] = user_post_counts
                logger.info("사용자별 게시글 수 분석 완료.")
                
                # 각 사용자가 작성한 댓글 수 (여기서는 게시글에 달린 댓글을 사용자에 연결 - 복잡한 조인 필요)
                # 좀 더 정확한 댓글-사용자 연결을 위해서는 원본 데이터 스키마 파악 필요.
                # 현재는 단순히 병합된 DF에서 댓글이 달린 게시글의 작성자 기준으로 카운트 (간단화된 예시)
                user_comment_counts = df.groupby('users_id')['comments_id'].nunique().to_dict()
                analysis_results['user_comment_counts'] = user_comment_counts
                logger.info("사용자별 댓글 수 분석 완료.")
            
            # 예시 2: 가장 활동적인 사용자 TOP 5 (게시글 + 댓글 합산)
            if 'user_post_counts' in analysis_results and 'user_comment_counts' in analysis_results:
                user_activity = {}
                for user_id in set(analysis_results['user_post_counts'].keys()) | set(analysis_results['user_comment_counts'].keys()):
                    total_activity = analysis_results['user_post_counts'].get(user_id, 0) + analysis_results['user_comment_counts'].get(user_id, 0)
                    user_activity[user_id] = total_activity
                
                sorted_activity = sorted(user_activity.items(), key=lambda item: item[1], reverse=True)
                analysis_results['top_5_active_users'] = sorted_activity[:5]
                logger.info("가장 활동적인 사용자 TOP 5 분석 완료.")
            
            # 예시 3: 게시글 제목 길이 분포 (간단한 텍스트 분석)
            if 'posts_title' in df.columns:
                df['posts_title_length'] = df['posts_title'].astype(str).apply(len)
                title_length_stats = df['posts_title_length'].describe().to_dict()
                analysis_results['post_title_length_stats'] = title_length_stats
                logger.info("게시글 제목 길이 분포 분석 완료.")

        except Exception as e:
            logger.error(f"고급 분석 중 오류 발생: {e}", exc_info=True)
            analysis_results['error'] = str(e)

        return analysis_results

class DataPipelineOrchestrator:
    """
    데이터 수집, 전처리, 분석의 전체 파이프라인을 오케스트레이션합니다.
    서비스 간 의존성 주입 및 전체 흐름을 제어합니다.
    """
    def __init__(self, api_service: ExternalAPIService, transformer: DataTransformer, analyzer: DataAnalyzer):
        self._api_service = api_service
        self._transformer = transformer
        self._analyzer = analyzer

    async def run(self, endpoints_with_params: List[Tuple[str, Dict[str, Any]]]) -> Dict[str, Any]:
        """
        데이터 파이프라인의 전체 흐름을 실행하고 최종 분석 결과를 반환합니다.
        """
        start_time = time.time()
        logger.info("--- 데이터 파이프라인 실행 시작 ---")
        
        raw_data = {}
        # 병렬 데이터 수집
        tasks = [self._api_service.fetch_data(endpoint, params) for endpoint, params in endpoints_with_params]
        results = await asyncio.gather(*tasks, return_exceptions=True) # return_exceptions=True로 개별 오류에도 전체 진행
        
        for i, (endpoint, _) in enumerate(endpoints_with_params):
            base_key = endpoint.split('/')[-1]
            if not isinstance(results[i], Exception): # 오류가 아닌 경우에만 저장
                raw_data[base_key] = results[i]
            else:
                logger.error(f"엔드포인트 '{endpoint}' 데이터 수집 중 치명적 오류: {results[i]}")
                raw_data[base_key] = [] # 오류 발생 시 빈 리스트로 처리하여 다음 단계 진행

        logger.info("1. 원시 데이터 수집 완료.")

        # 데이터 전처리
        processed_df = self._transformer.preprocess(raw_data)
        logger.info("2. 데이터 전처리 완료.")

        # 고급 분석
        analysis_results = self._analyzer.perform_advanced_analysis(processed_df)
        logger.info("3. 고급 분석 완료.")

        end_time = time.time()
        logger.info(f"--- 데이터 파이프라인 실행 완료. 총 소요 시간: {end_time - start_time:.2f}초 ---")
        
        return analysis_results

# --- 메인 실행 함수 ---
async def main():
    # aiohttp ClientSession은 애플리케이션 생명주기 동안 유지되어야 성능 이점이 극대화됩니다.
    # context manager를 사용하여 세션이 자동으로 닫히도록 관리합니다.
    async with aiohttp.ClientSession() as session:
        api_service = ExternalAPIService(session)
        data_transformer = DataTransformer()
        data_analyzer = DataAnalyzer()
        
        orchestrator = DataPipelineOrchestrator(api_service, data_transformer, data_analyzer)

        endpoints_to_fetch = [
            ("users", {"_limit": 5}),
            ("posts", {"_limit": 5}),
            ("comments", {"postId": 1, "_limit": 3}) # postId 1번에 대한 댓글
        ]
        
        final_results = await orchestrator.run(endpoints_to_fetch)
        
        import json
        logger.info("\n--- 최종 분석 결과 ---")
        print(json.dumps(final_results, indent=4, ensure_ascii=False))

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("파이프라인 실행이 사용자 요청으로 중단되었습니다.")
    except Exception as e:
        logger.critical(f"파이프라인 실행 중 치명적인 오류 발생: {e}", exc_info=True)
