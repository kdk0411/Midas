from selenium import webdriver
from selenium.webdriver.chrome.options import Options

def setup_driver(selenium_hub_url):
    """Selenium WebDriver 설정"""
    chrome_options = Options()
    
    # 필수 옵션들
    chrome_options.add_argument('--no-sandbox')  # Docker 환경에서 필수
    chrome_options.add_argument('--headless')    # 헤드리스 모드
    chrome_options.add_argument('--disable-dev-shm-usage')  # Docker 환경에서 메모리 관련 이슈 방지
    
    # 성능 최적화 옵션들
    chrome_options.add_argument('--disable-gpu')  # GPU 가속 비활성화
    chrome_options.add_argument('--disable-extensions')  # 확장 프로그램 비활성화
    chrome_options.add_argument('--disable-infobars')  # 정보 표시줄 비활성화
    chrome_options.add_argument('--disable-notifications')  # 알림 비활성화
    
    # 메모리 최적화
    chrome_options.add_argument('--disable-application-cache')  # 애플리케이션 캐시 비활성화
    chrome_options.add_argument('--disable-browser-side-navigation')  # 브라우저 사이드 네비게이션 비활성화
    chrome_options.add_argument('--disable-sync')  # 동기화 비활성화
    
    # 보안 관련 옵션
    chrome_options.add_argument('--disable-web-security')  # 웹 보안 비활성화 (필요한 경우에만)
    chrome_options.add_argument('--allow-running-insecure-content')  # 안전하지 않은 콘텐츠 허용 (필요한 경우에만)
    
    # 로깅 최적화
    chrome_options.add_argument('--log-level=3')  # 로그 레벨 최소화
    chrome_options.add_argument('--silent')  # 로그 출력 최소화
    
    # Selenium Grid 연결 설정
    chrome_options.set_capability('browserName', 'chrome')
    chrome_options.set_capability('platform', 'LINUX')  # Docker 환경은 보통 Linux
    
    # WebDriver 생성
    driver = webdriver.Remote(
        command_executor=selenium_hub_url,
        options=chrome_options
    )
    
    # 타임아웃 설정
    driver.set_page_load_timeout(30)
    driver.implicitly_wait(10)
    
    return driver 