# from __future__ import annotations

# import pendulum

# from airflow.models.dag import DAG
# from airflow.operators.python import PythonOperator
# from airflow.utils.trigger_rule import TriggerRule

# # Selenium imports - Airflow worker에 selenium 패키지가 설치되어 있어야 합니다.
# # Dockerfile에 추가하거나, _PIP_ADDITIONAL_REQUIREMENTS 환경 변수를 통해 설치할 수 있습니다.
# from selenium import webdriver
# from selenium.webdriver.common.by import By
# from selenium.webdriver.remote.webdriver import WebDriver as RemoteWebDriver
# from selenium.webdriver.common.options import ArgOptions

# # 예시 작업: Selenium Grid를 사용하여 웹사이트 타이틀 가져오기
# def get_website_title_with_selenium(dag_run=None, **kwargs):
#     print(f"Running with dag_run: {dag_run}")
#     ti = kwargs['ti']
    
#     # Selenium Hub 주소 (docker-compose에서 설정한 서비스 이름 사용)
#     SELENIUM_HUB_URL = "http://selenium-hub:4444/wd/hub"
    
#     # 브라우저 옵션 (예: Chrome)
#     chrome_options = webdriver.ChromeOptions()
#     chrome_options.add_argument("--headless")  # Headless 모드로 실행 (GUI 없는 환경)
#     chrome_options.add_argument("--no-sandbox")
#     chrome_options.add_argument("--disable-dev-shm-usage")
#     # chrome_options.add_argument("--window-size=1920,1080") # 필요시 창 크기 지정

#     print(f"Attempting to connect to Selenium Hub at {SELENIUM_HUB_URL}")
#     driver = None
#     try:
#         driver = webdriver.Remote(
#             command_executor=SELENIUM_HUB_URL,
#             options=chrome_options
#         )
#         print("Successfully connected to Selenium Hub.")
        
#         target_url = "http://www.google.com" # 테스트할 웹사이트 주소
#         print(f"Navigating to {target_url}")
#         driver.get(target_url)
        
#         page_title = driver.title
#         print(f"Page title: {page_title}")
        
#         ti.xcom_push(key="website_title", value=page_title)
        
#     except Exception as e:
#         print(f"An error occurred: {e}")
#         raise
#     finally:
#         if driver:
#             print("Quitting WebDriver.")
#             driver.quit()

# # 만약 Non-headless (GUI가 있는) 브라우저를 Selenium Grid 노드에서 실행하고 싶다면,
# # 해당 노드(예: selenium/node-chrome-debug, selenium/node-firefox-debug 이미지)를 사용하고
# # VNC 클라이언트로 접속하여 GUI를 확인할 수 있습니다.
# # 이 경우 DAG 코드 자체는 크게 달라지지 않지만, Docker 이미지와 설정이 달라집니다.
# # headless=False 옵션을 주려면, Selenium Grid 노드가 GUI를 지원해야 합니다.
# # 현재 추가된 node-chrome, node-firefox는 headless 실행에 적합합니다.

# with DAG(
#     dag_id="example_selenium_financial_crawl",
#     schedule=None, # 필요에 따라 스케줄 설정 (예: "0 0 * * *")
#     start_date=pendulum.datetime(2023, 10, 26, tz="UTC"),
#     catchup=False,
#     tags=["example", "selenium", "crawling"],
# ) as dag:
    
#     crawl_task = PythonOperator(
#         task_id="crawl_google_title",
#         python_callable=get_website_title_with_selenium,
#         # 특정 _큐_를 사용하는 워커에서 이 태스크를 실행하도록 지정
#         # docker-compose-airflow.yaml 파일에서 worker의 command에 -q <큐이름> 으로 설정한 경우
#         # 예를 들어 airflow-worker_1 이 'financial_data_queue'를 담당한다면:
#         queue="financial_data_queue", 
#     )

#     # 만약 다른 종류의 작업을 다른 큐에서 실행하고 싶다면:
#     # process_task = PythonOperator(
#     #     task_id="process_data_on_another_worker",
#     #     python_callable=my_data_processing_function,
#     #     queue="data_processing_queue", # airflow-worker_2 가 이 큐를 담당한다고 가정
#     # )
#     # crawl_task >> process_task 