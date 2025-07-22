import asyncio
import aiohttp
import pandas as pd
from typing import List, Dict, Any, Tuple
import logging
import time

# 로깅 설정: 실제 서비스 환경에서 디버깅 및 모니터링에 필수적입니다.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class AdvancedDataProcessor:
    """
    고성능 데이터 처리 및 분석을 위한 클래스입니다.
    비동기 웹 요청, 데이터 전처리, 복잡한 분석 로직을 포함합니다.
    """
    def __init__(self, api_base_url: str, request_timeout: int = 10):
        self.api_base_url = api_base_url
        self.request_timeout = request_timeout
        # aiohttp.ClientSession은 비동기 HTTP 요청을 위한 세션을 관리합니다.
        # 연결 풀링 등을 통해 성능을 최적화합니다.
        self.session = aiohttp.ClientSession()

    async def _fetch_data_from_api(self, endpoint: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        주어진 API 엔드포인트에서 비동기적으로 데이터를 가져옵니다.
        네트워크 지연을 시뮬레이션하고, 견고한 오류 처리를 보여줍니다.
        """
        url = f"{self.api_base_url}/{endpoint}"
        try:
            async with self.session.get(url, params=params, timeout=self.request_timeout) as response:
                response.raise_for_status()  # 200번대 응답이 아니면 예외 발생
                data = await response.json()
                logging.info(f"데이터를 성공적으로 가져옴: {endpoint}")
                await asyncio.sleep(0.1)  # 네트워크 지연 시뮬레이션
                return data
        except aiohttp.ClientError as e:
            logging.error(f"API 요청 실패 ({endpoint}): {e}")
            return {"error": str(e)}
        except asyncio.TimeoutError:
            logging.error(f"API 요청 시간 초과 ({endpoint})")
            return {"error": "Request timed out"}

    async def fetch_multiple_datasets(self, endpoints_with_params: List[Tuple[str, Dict[str, Any]]]) -> Dict[str, List[Dict[str, Any]]]:
        """
        여러 API 엔드포인트에서 동시에 데이터를 가져옵니다.
        asyncio.gather를 사용하여 비동기 동시성을 극대화합니다.
        """
        tasks = []
        for endpoint, params in endpoints_with_params:
            tasks.append(self._fetch_data_from_api(endpoint, params))
        
        results = await asyncio.gather(*tasks, return_exceptions=True) # return_exceptions=True로 개별 실패에도 전체 작동 유지
        
        collected_data = {}
        for i, (endpoint, _) in enumerate(endpoints_with_params):
            if isinstance(results[i], dict) and "error" not in results[i]:
                collected_data[endpoint.split('/')[-1]] = results[i] # endpoint의 마지막 부분 (예: 'users')을 키로 사용
            else:
                logging.warning(f"엔드포인트 '{endpoint}' 데이터 가져오기 실패 또는 오류: {results[i]}")
                collected_data[endpoint.split('/')[-1]] = [] # 실패 시 빈 리스트 반환 또는 적절한 기본값
        return collected_data

    def preprocess_data(self, raw_data: Dict[str, List[Dict[str, Any]]]) -> pd.DataFrame:
        """
        가져온 원시 데이터를 Pandas DataFrame으로 변환하고 전처리합니다.
        결측치 처리, 데이터 타입 변환 등 실용적인 데이터 전처리 로직을 포함합니다.
        """
        processed_dfs = []
        for key, data_list in raw_data.items():
            if data_list: # 데이터가 있을 경우에만 처리
                try:
                    df = pd.DataFrame(data_list)
                    # 예시: 'id' 컬럼이 있으면 인덱스로 설정
                    if 'id' in df.columns:
                        df = df.set_index('id')
                    # 예시: 문자열 'created_at'을 datetime으로 변환 (오류 무시)
                    if 'created_at' in df.columns:
                        df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
                    # 예시: 수치형 컬럼의 결측치를 0으로 채우기
                    for col in ['views', 'likes']: # 가상의 수치형 컬럼
                        if col in df.columns:
                            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                    processed_dfs.append(df.add_prefix(f'{key}_')) # 컬럼명 충돌 방지를 위해 접두사 추가
                    logging.info(f"'{key}' 데이터 전처리 완료.")
                except Exception as e:
                    logging.error(f"'{key}' 데이터 전처리 중 오류 발생: {e}")
            else:
                logging.warning(f"'{key}' 데이터가 없어 전처리를 건너뜁니다.")
        
        if not processed_dfs:
            logging.warning("처리할 데이터프레임이 없습니다. 빈 데이터프레임을 반환합니다.")
            return pd.DataFrame()
        
        # 여러 데이터프레임을 병합 (공통 컬럼 또는 인덱스를 기준으로)
        # 실제 시나리오에서는 적절한 merge/join 전략이 필요
        try:
            # 예시: 모든 데이터프레임을 외부 조인 (인덱스 기준)
            # 복잡도에 따라 더 정교한 병합 로직 필요
            final_df = processed_dfs[0]
            for i in range(1, len(processed_dfs)):
                final_df = final_df.join(processed_dfs[i], how='outer')
            logging.info("모든 데이터프레임 병합 완료.")
            return final_df
        except Exception as e:
            logging.error(f"데이터프레임 병합 중 오류 발생: {e}")
            # 병합 실패 시, 개별 처리된 데이터프레임 리스트 반환을 고려하거나 오류 처리
            return pd.DataFrame() # 오류 발생 시 빈 DataFrame 반환

    def perform_advanced_analysis(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        전처리된 DataFrame을 기반으로 복잡한 데이터 분석을 수행합니다.
        이는 통계 분석, 패턴 인식, 예측 모델링 등 실제 비즈니스 로직을 나타낼 수 있습니다.
        """
        if df.empty:
            logging.warning("분석할 데이터가 없어 고급 분석을 수행할 수 없습니다.")
            return {"message": "No data for analysis."}

        analysis_results = {}
        
        # 예시 1: 'users_age' 컬럼이 있다면 연령대별 통계 분석
        if 'users_age' in df.columns:
            age_bins = [0, 19, 29, 39, 49, 59, 100]
            age_labels = ['<20', '20s', '30s', '40s', '50s', '60+']
            df['users_age_group'] = pd.cut(df['users_age'], bins=age_bins, labels=age_labels, right=False)
            analysis_results['age_group_distribution'] = df['users_age_group'].value_counts().to_dict()
            logging.info("연령대별 분포 분석 완료.")
            
        # 예시 2: 'products_price' 와 'products_sales_volume' 간의 상관관계 분석 (가상의 컬럼)
        if 'products_price' in df.columns and 'products_sales_volume' in df.columns:
            try:
                correlation = df['products_price'].corr(df['products_sales_volume'])
                analysis_results['price_sales_correlation'] = correlation
                logging.info(f"가격-판매량 상관관계 분석 완료: {correlation:.2f}")
            except Exception as e:
                logging.warning(f"가격-판매량 상관관계 분석 실패: {e}")

        # 예시 3: 시간 기반 데이터 (가령 'created_at')에 대한 일별/주별 트렌드 분석
        # 예시로 'users_created_at' 컬럼이 있다고 가정
        if 'users_created_at' in df.columns:
            df['users_created_date'] = df['users_created_at'].dt.date
            daily_signups = df.groupby('users_created_date').size().to_dict()
            analysis_results['daily_signups'] = {str(k): v for k, v in daily_signups.items()}
            logging.info("일별 사용자 가입 트렌드 분석 완료.")

        # 분석 결과를 JSON 형태로 반환하거나, 특정 형식으로 저장할 수 있습니다.
        return analysis_results

    async def run_data_pipeline(self, endpoints_with_params: List[Tuple[str, Dict[str, Any]]]) -> Dict[str, Any]:
        """
        데이터 수집, 전처리, 분석의 전체 파이프라인을 실행합니다.
        비동기적으로 데이터를 가져오고, 동기적으로 Pandas를 이용한 분석을 수행합니다.
        """
        start_time = time.time()
        logging.info("데이터 파이프라인 시작...")

        # 1. 데이터 수집 (비동기)
        raw_data = await self.fetch_multiple_datasets(endpoints_with_params)
        
        # 2. 데이터 전처리 (동기)
        processed_df = self.preprocess_data(raw_data)
        
        # 3. 고급 분석 (동기)
        analysis_results = self.perform_advanced_analysis(processed_df)

        await self.session.close() # 세션 명시적 종료
        end_time = time.time()
        logging.info(f"데이터 파이프라인 완료. 총 실행 시간: {end_time - start_time:.2f}초")
        return analysis_results

# --- 실행 예제 ---
async def main():
    # 가상의 API 엔드포인트 URL
    # 실제 환경에서는 모의 서버 (예: FastAPI, Flask)를 구축하여 테스트하거나, 공개 API를 활용할 수 있습니다.
    MOCK_API_BASE_URL = "https://jsonplaceholder.typicode.com" # 공개 더미 API 사용

    processor = AdvancedDataProcessor(MOCK_API_BASE_URL)

    # 가져올 데이터셋과 각 데이터셋에 대한 매개변수
    # users: 사용자 정보 (10명), posts: 게시글 정보 (10개)
    # comments: 댓글 정보 (10개) -> comments 엔드포인트는 존재하지만, 예제에서는 posts의 id를 넘겨줄 수 있도록 수정
    endpoints_to_fetch = [
        ("users", {"_limit": 5}), # 사용자 데이터 5개만 가져오기
        ("posts", {"_limit": 5}), # 게시글 데이터 5개만 가져오기
        # 실제 데이터 조인 시나리오를 위해 post ID가 1인 댓글만 가져오기
        ("comments", {"postId": 1, "_limit": 3}) 
    ]
    
    # 파이프라인 실행
    results = await processor.run_data_pipeline(endpoints_to_fetch)
    
    print("\n--- 최종 분석 결과 ---")
    import json
    print(json.dumps(results, indent=4, ensure_ascii=False))

    # 추가적으로, DataFrame의 일부를 출력하여 처리 결과를 시각적으로 보여줄 수 있습니다.
    # print("\n--- 전처리된 데이터프레임 샘플 (일부) ---")
    # if not processor.preprocess_data(await processor.fetch_multiple_datasets(endpoints_to_fetch)).empty:
    #     print(processor.preprocess_data(await processor.fetch_multiple_datasets(endpoints_to_fetch)).head())


if __name__ == "__main__":
    # Python 3.7+에서 asyncio.run()을 사용하여 비동기 코드를 실행합니다.
    asyncio.run(main())