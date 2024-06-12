import pandas as pd
from datetime import datetime, timedelta
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from fake_headers import Headers
from bs4 import BeautifulSoup
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# 엑셀 파일 경로
file_path = 'TalkFile_추가요청.xlsx'

# 엑셀 파일을 DataFrame으로 읽기
df = pd.read_excel(file_path)

def generate_payload(start_date, end_date):
    current_date = datetime.now().strftime("%Y-%m-%d")
    return {
        "CSRFToken": "G1P5qi2QQzdb48cAT4etN5FEw5HKBf3uH2jfE9rWzEI",
        "loadEnd": "0",
        "curTime": current_date,
        "totSeatCntRatioOrder": "",
        "totSeatCntOrder": "",
        "totShowAmtOrder": "",
        "addTotShowAmtOrder": "",
        "totShowCntOrder": "",
        "addTotShowCntOrder": "",
        "dmlMode": "search",
        "startDate": start_date,
        "endDate": end_date,
        "searchType": "",
        "repNationCd": "",
        "wideareaCd": ""
    }

def fetch_data(url, payload, period, week, data_type):
    headers = Headers(headers=True).generate()
    
    session = requests.Session()
    retry = Retry(
        total=10,
        read=10,
        connect=10,
        backoff_factor=10,
        status_forcelist=[429, 500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    
    time.sleep(1)  # 각 작업 시작 전에 1초 대기
    try:
        response = session.post(url, headers=headers, data=payload, timeout=20)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Error fetching data during {period}: {e}")
        return []

    soup = BeautifulSoup(response.text, 'html.parser')
    table_datas = soup.find('tbody', {'id': 'mainTbody'}).find_all('tr')
    results = []

    for row in table_datas:
        movie_name = row.find('td', {'id': 'td_movie'}).get_text(strip=True)
        if data_type == 'seat':
            data = {
                'rank': row.find('td', {'id': 'td_rank'}).get_text(strip=True),
                'open_date': row.find('td', {'id': 'td_openDt'}).get_text(strip=True),
                'sale_cnt_ratio': row.find('td', {'id': 'td_totSaleCntRatio'}).get_text(strip=True),
                'seat_cnt_ratio': row.find('td', {'id': 'td_totSeatCntRatio'}).get_text(strip=True),
                'seat_cnt': row.find('td', {'id': 'td_totSeatCnt'}).get_text(strip=True),
                'show_amt': row.find('td', {'id': 'td_totShowAmt'}).get_text(strip=True),
                'add_show_amt': row.find('td', {'id': 'td_addTotShowAmt'}).get_text(strip=True),
                'show_cnt': row.find('td', {'id': 'td_totShowCnt'}).get_text(strip=True),
                'add_show_cnt': row.find('td', {'id': 'td_addTotShowCnt'}).get_text(strip=True),
            }
        elif data_type == 'show':
            data = {
                'rank': row.find('td', {'id': 'td_rank'}).get_text(strip=True),
                'open_date': row.find('td', {'id': 'td_openDt'}).get_text(strip=True),
                'dsp_cnt': row.find('td', {'id': 'td_totDspCnt'}).get_text(strip=True),
                'dsp_cnt_ratio': row.find('td', {'id': 'td_totDspCntRatio'}).get_text(strip=True),
                'issu_amt': row.find('td', {'id': 'td_totIssuAmt'}).get_text(strip=True),
                'add_issu_amt': row.find('td', {'id': 'td_addTotIssuAmt'}).get_text(strip=True),
                'issu_cnt': row.find('td', {'id': 'td_totIssuCnt'}).get_text(strip=True),
                'add_issu_cnt': row.find('td', {'id': 'td_addTotIssuCnt'}).get_text(strip=True),
            }
        elif data_type == 'screen':
            data = {
                'rank': row.find('td', {'id': 'td_rank'}).get_text(strip=True),
                'open_date': row.find('td', {'id': 'td_openDt'}).get_text(strip=True),
                'scrn_cnt': row.find('td', {'id': 'td_totScrnCnt'}).get_text(strip=True),
                'scrn_cnt_ratio': row.find('td', {'id': 'td_totScrnCntRatio'}).get_text(strip=True),
                'issu_amt': row.find('td', {'id': 'td_totIssuAmt'}).get_text(strip=True),
                'add_issu_amt': row.find('td', {'id': 'td_addTotIssuAmt'}).get_text(strip=True),
                'issu_cnt': row.find('td', {'id': 'td_totIssuCnt'}).get_text(strip=True),
                'add_issu_cnt': row.find('td', {'id': 'td_addTotIssuCnt'}).get_text(strip=True),
            }
        data.update({'movie_name': movie_name, 'period': period, 'week': week})
        results.append(data)
    
    return results

def get_movie_ratios(release_date_str, target_movie):
    release_date = datetime.strptime(release_date_str, "%Y-%m-%d")
    periods = [(0, 6), (7, 13), (14, 20)]
    week_labels = ["1주차", "2주차", "3주차"]

    urls = {
        "seat": "https://kobis.or.kr/kobis/business/stat/boxs/findPeriodSeatTicketList.do",
        "show": "https://kobis.or.kr/kobis/business/stat/boxs/findPeriodShowTicketList.do",
        "screen": "https://kobis.or.kr/kobis/business/stat/boxs/findPeriodScreenTicketList.do"
    }

    results = {
        "seat": [],
        "show": [],
        "screen": []
    }

    for i, (start, end) in enumerate(periods):
        start_date = (release_date + timedelta(days=start)).strftime("%Y-%m-%d")
        end_date = (release_date + timedelta(days=end)).strftime("%Y-%m-%d")
        period = f"{start_date} ~ {end_date}"
        week = week_labels[i]
        payload = generate_payload(start_date, end_date)

        for url_type in ["seat", "show", "screen"]:
            results[url_type].extend(fetch_data(urls[url_type], payload, period, week, url_type))

    return target_movie, results

# 결과를 저장할 DataFrame 생성
result_columns = [
    '영화명', '기간', '주차', '좌석점유율', '좌석점유율-순위', '좌석점유율-개봉일', '좌석점유율-판매비율', '좌석점유율-좌석수', '좌석점유율-총액', '좌석점유율-추가총액', '좌석점유율-횟수', '좌석점유율-추가횟수',
    '상영점유율', '상영점유율-순위', '상영점유율-개봉일', '상영점유율-상영횟수', '상영점유율-상영비율', '상영점유율-발행액', '상영점유율-추가발행액', '상영점유율-발행수', '상영점유율-추가발행수',
    '스크린점유율', '스크린점유율-순위', '스크린점유율-개봉일', '스크린점유율-스크린수', '스크린점유율-스크린비율', '스크린점유율-발행액', '스크린점유율-추가발행액', '스크린점유율-발행수', '스크린점유율-추가발행수',
    'target_movie'
]
result_df = pd.DataFrame(columns=result_columns)

# 각 영화별로 데이터 수집 및 저장
processed_movies = set()
tasks = []

with ThreadPoolExecutor(max_workers=10) as executor:
    for index, row in tqdm(df.drop_duplicates(subset=['영화명']).iterrows(), total=len(df.drop_duplicates(subset=['영화명'])['영화명'])):
        movie_name = row['영화명']
        release_date_str = row['개봉일'].strftime("%Y-%m-%d")

        if movie_name not in processed_movies:
            processed_movies.add(movie_name)
            # time.sleep(1)  # 각 프로세스 시작 전에 1초 대기

            target_movies = df[df['개봉일'] == row['개봉일']]['영화명'].tolist()
            
            tasks.append(executor.submit(get_movie_ratios, release_date_str, movie_name))
    
    results_list = []
    for future in tqdm(as_completed(tasks), total=len(tasks)):
        try:
            target_movie, ratios = future.result()

            all_periods = set(period_data['period'] for period_data in ratios["seat"])
            all_periods.update(period_data['period'] for period_data in ratios["show"])
            all_periods.update(period_data['period'] for period_data in ratios["screen"])

            for period in all_periods:
                seat_data = next((data for data in ratios["seat"] if data['period'] == period), {})
                show_data = next((data for data in ratios["show"] if data['period'] == period), {})
                screen_data = next((data for data in ratios["screen"] if data['period'] == period), {})

                results_list.append({
                    '영화명': seat_data.get('movie_name', show_data.get('movie_name', screen_data.get('movie_name', target_movie))),
                    '기간': period,
                    '주차': seat_data.get('week', show_data.get('week', screen_data.get('week', 'N/A'))),
                    '좌석점유율': seat_data.get('seat_cnt_ratio', ''),
                    '좌석점유율-순위': seat_data.get('rank', ''),
                    '좌석점유율-개봉일': seat_data.get('open_date', ''),
                    '좌석점유율-판매비율': seat_data.get('sale_cnt_ratio', ''),
                    '좌석점유율-좌석수': seat_data.get('seat_cnt', ''),
                    '좌석점유율-총액': seat_data.get('show_amt', ''),
                    '좌석점유율-추가총액': seat_data.get('add_show_amt', ''),
                    '좌석점유율-횟수': seat_data.get('show_cnt', ''),
                    '좌석점유율-추가횟수': seat_data.get('add_show_cnt', ''),
                    '상영점유율': show_data.get('dsp_cnt_ratio', ''),
                    '상영점유율-순위': show_data.get('rank', ''),
                    '상영점유율-개봉일': show_data.get('open_date', ''),
                    '상영점유율-상영횟수': show_data.get('dsp_cnt', ''),
                    '상영점유율-상영비율': show_data.get('dsp_cnt_ratio', ''),
                    '상영점유율-발행액': show_data.get('issu_amt', ''),
                    '상영점유율-추가발행액': show_data.get('add_issu_amt', ''),
                    '상영점유율-발행수': show_data.get('issu_cnt', ''),
                    '상영점유율-추가발행수': show_data.get('add_issu_cnt', ''),
                    '스크린점유율': screen_data.get('scrn_cnt_ratio', ''),
                    '스크린점유율-순위': screen_data.get('rank', ''),
                    '스크린점유율-개봉일': screen_data.get('open_date', ''),
                    '스크린점유율-스크린수': screen_data.get('scrn_cnt', ''),
                    '스크린점유율-스크린비율': screen_data.get('scrn_cnt_ratio', ''),
                    '스크린점유율-발행액': screen_data.get('issu_amt', ''),
                    '스크린점유율-추가발행액': screen_data.get('add_issu_amt', ''),
                    '스크린점유율-발행수': screen_data.get('issu_cnt', ''),
                    '스크린점유율-추가발행수': screen_data.get('add_issu_cnt', ''),
                    'target_movie': target_movie
                })
        except Exception as e:
            print(f"Error processing future result: {e}")

# 최종적으로 DataFrame에 데이터를 추가
result_df = pd.DataFrame(results_list)

# 결과를 새로운 엑셀 파일에 저장
output_file_path = 'movie_ratios_additional_tasks.xlsx'
result_df.to_excel(output_file_path, index=False)

print(f"결과가 {output_file_path}에 저장되었습니다.")
