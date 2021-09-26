import pandas as pd
import requests
from bs4 import BeautifulSoup
import concurrent.futures
import time
from glob import glob
import helper
import conn_db

suffix = helper.get_time_suffix()
# user_agent = helper.user_agent
# max_workers = 3


# 네이버증권에서 종목의 기업실적분석 표 가져오기
def _get_fs_from_naver(code):
    '''
    종목코드 넣으면 네이버증권에서 그 종목의 기업실적분석 표 가져오기
    '''
    url = f'https://finance.naver.com/item/main.nhn?code={code}'
    r = requests.get(url, headers={'User-Agent': user_agent}, timeout=3)
    dom = BeautifulSoup(r.content, "lxml")
    try:
        # 컬럼 header가 들어 있는 변수
        selector = '#content > div.section.cop_analysis > div.sub_section > table > thead > tr > th'
        col_header = dom.select(selector)

        # data가 들어 있는 변수
        selector = '#content > div.section.cop_analysis > div.sub_section > table > tbody > tr'
        data_by_accounts = dom.select(selector)

        # 취합할 dataframe
        df = pd.DataFrame()

        # 1. 계정별 data 가져오기
        for i in range(len(data_by_accounts)):
            data = pd.DataFrame([data.text.strip() for data in data_by_accounts[i].select('td')])
            df = pd.concat([df, data], axis=1)

        # 2. 계정 명칭 가져오기
        header_info = []
        for i in range(len(data_by_accounts)):
            header = data_by_accounts[i].select('th')[0].text.strip()
            header_info.append(header)
        df.columns = header_info

        # 3. 날짜 컬럼 가져오기
        all_col = [data.text.strip() for data in col_header[3:]]  # 앞부분은 불필요한 컬럼 정보가 있어서 제외
        estimate_dates = [x for x in all_col if '(E)' in x]  # 예상치가 있는 컬럼

        # 연간 컬럼 날짜 정리
        y = all_col.index(estimate_dates[0])+1  # 연간실적 컬럼이 끝나는 위치
        y_date = all_col[:y]
        dummy = [all_col.remove(date) for date in y_date]
        y_date = [date+'_연간' for date in y_date]

        # 분기 컬럼 날짜 정리
        q = all_col.index(estimate_dates[1])+1  # 분기실적 컬럼이 끝나는 위치
        q_date = all_col[:q]
        dummy = [all_col.remove(date) for date in q_date]
        q_date = [date+'_분기' for date in q_date]

        # 날짜 컬럼
        date_col = pd.DataFrame(y_date + q_date).rename(columns={0: '날짜'})

        # 재무제표 기준 컬럼
        fstype_col = pd.DataFrame(all_col).rename(columns={0: '재무제표기준'})
        df = pd.concat([date_col, fstype_col, df], axis=1)
        df = df.melt(id_vars=['날짜', '재무제표기준'], var_name='항목', value_name='값')
        df[['날짜', '주기']] = df['날짜'].str.split('_', expand=True)

        # 날짜 컬럼 값에 'E'가 있으면 전망치임. 별도 컬럼을 만든 다음에 날짜컬럼에서는 삭제
        df['실적/전망'] = ['전망' if 'E' in x else '실적' for x in df['날짜']]
        df['날짜'] = df['날짜'].str.replace('\(E\)', '')
        df['종목코드'] = code
        print(f'{code} 완료')
        return df
    except:
        print(f'{code} 데이터가져오기 실패')
        pass

# 업데이트할 종목 가져오기
def _get_codes_to_update():
    cols = ['KEY', '종목코드']
    new = conn_db.from_("DB_기업정보", 'FS_update_list')[cols]
    old_df = conn_db.from_("from_naver증권", 'naver_최근값만')[['KEY']].drop_duplicates()

    code_list_added = pd.DataFrame(list(set(new['KEY']) - set(old_df)))
    code_list_added = code_list_added.merge(new, left_on=0, right_on='KEY')['종목코드']
    return code_list_added

# 네이버증권 기업실적분석표 전체종목 업데이트
@helper.timer
def update_naver_fs(param='all'):
    df = pd.DataFrame()

    if param =='all':
        codes = conn_db.from_("DB_기업정보", 'FS_update_list')['종목코드']

        # with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        #     result = executor.map(_get_fs_from_naver, codes)
        #     df = pd.concat([df for df in result], axis=0)
        for code in codes:
            try:
                df = df.append(_get_fs_from_naver(code), ignore_index=True)
                time.sleep(1)
            except:
                pass

    else: # 새로운 값만 업데이트
        codes = _get_codes_to_update()
        if len(codes)>0:
            df = pd.concat([_get_fs_from_naver(code) for code in codes], axis=0)
            df.reset_index(drop=True, inplace=True)
        else:
            pass

    if len(df)>0:
        # 새로 가져온 것 전처리
        df = helper.clean_numeric_value(df)
        dcols = set(df.columns.tolist()) - set(['항목','값',])
        df = df.pivot_table(index = dcols, columns = '항목', values='값',).reset_index()
        df.columns.name = None

        # 취합본에 합혀서 저장
        file = conn_db.get_path('folder_naver_fs_raw')  + "fs_from_naver_원본_취합본.pkl"
        old_df = pd.read_pickle(file)

        cols = ['날짜', '재무제표기준', '종목코드', '주기']
        df = helper.add_df(df, old_df, check_cols = cols)
        df.to_pickle(file)

    else: #새로 가져온 df가 값이 없을때
        print('업데이트할 내용 없음')
