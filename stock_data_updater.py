import pandas as pd
import concurrent.futures
from bs4 import BeautifulSoup
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import os
import time
import conn_db
import helper

user_agent = helper.user_agent

def get_trade_volume_from_krx(from_date, to_date):
    '''
    krx에서 종목별 기관,외국인 거래량 가져오기
    '''
    print(f'KRX에서 {from_date}~{to_date} 기관과 외국인 거래량 가져오기 시작')
    folder = r"C:\Users\bong2\Downloads\\"
    save_folder = r"C:\Users\bong2\OneDrive\DataArchive\DB_주식관련\KRX_80021_기관외국인순매수추이\\"
    code_list = conn_db.from_("DB_기업정보", 'FS_update_list')[['종목명','종목코드']]
    #---------------------------------------------------------------------------------
    def change_dates(from_date, to_date):
        # 시작날짜
        xpath = "//*[starts-with(@id,'fromdate')]"
        driver.find_element_by_xpath(xpath).clear()
        driver.find_element_by_xpath(xpath).send_keys(from_date)
        # 종료날짜
        xpath = "//*[starts-with(@id,'todate')]"
        driver.find_element_by_xpath(xpath).clear()
        driver.find_element_by_xpath(xpath).send_keys(to_date)
    #---------------------------------------------------------------------------------
    driver = webdriver.Chrome()
    # 종목별 기관, 외국인 거래량 가져오기
    codes = code_list['종목코드'].tolist()
    file_count = len(glob(folder+'*.csv'))
    driver.get('http://marketdata.krx.co.kr/mdi#document=13020304')
    for code in codes:
        from_date = '20190101'
        to_date = '20191231'
        change_dates(from_date, to_date)
        code = 'A'+ code
        driver.find_element_by_xpath("//*[starts-with(@id,'isu_')]").clear() # 코드명 비우기
        # 코드 검색
        driver.find_element_by_xpath("//*[starts-with(@id,'finderbtn')]").click() # 검색 클릭
        # 검색창 loading확인
        xpath = '/html/body/div[1]/div[2]/div/div[2]/div/div[1]/div[3]/div/fieldset/form/dl[1]/dd/div[2]/div/div/dl/dd/div/div[1]/div[1]/div[2]/div/div/table/tbody/tr/td[2]/a'
        WebDriverWait(driver,100).until(EC.element_to_be_clickable((By.XPATH, xpath)))

        # 검색코드 입력
        xpath = "//*[starts-with(@id,'searchText')]"
        driver.find_element_by_xpath(xpath).send_keys(code)
        time.sleep(1)

        # 검색결과 선택
        xpath = '/html/body/div[1]/div[2]/div/div[2]/div/div[1]/div[3]/div/fieldset/form/dl[1]/dd/div[2]/div/div/dl/dd/div/div[1]/div[1]/div[2]/div/div/table/tbody/tr/td[2]/a'
        driver.find_element_by_xpath(xpath).click() #

        # 조회버튼 클릭
        driver.find_element_by_xpath("//*[starts-with(@id,'btnidc')]").click()


        # load 확인
        xpath = '/html/body/div[1]/div[2]/div/div[2]/div/div[1]/div[3]/div/div[1]/div/div[1]/div[1]/div[1]'
        WebDriverWait(driver,100).until(EC.element_to_be_clickable((By.XPATH, xpath)))
        time.sleep(1)
        xpath = '/html/body/div[1]/div[2]/div/div[2]/div/div[1]/div[3]/div/fieldset/form/div/span/button[3]'
        driver.find_element_by_xpath(xpath).click() # csv 다운로드 클릭
        # 파일명 변경
        while len(glob(folder+'*.csv'))==file_count:
            time.sleep(1)
        time.sleep(2)
        if len(glob(folder+'data*.csv'))>0:
            os.rename(folder+"data.csv", folder+f"{code}_{from_date}_{to_date}.csv")
        time.sleep(1.5)
        file_count = len(glob(folder+'*.csv'))
        #---------------------------------------------------------------------------------------------------
        from_date = '20200101'
        to_date = '20200823'
        change_dates(from_date, to_date)
        driver.find_element_by_xpath("//*[starts-with(@id,'btnidc')]").click() # 조회버튼 클릭
        # load 확인
        xpath = '/html/body/div[1]/div[2]/div/div[2]/div/div[1]/div[3]/div/div[1]/div/div[1]/div[1]/div[1]'
        WebDriverWait(driver,100).until(EC.element_to_be_clickable((By.XPATH, xpath)))
        time.sleep(1)
        xpath = '/html/body/div[1]/div[2]/div/div[2]/div/div[1]/div[3]/div/fieldset/form/div/span/button[3]'
        driver.find_element_by_xpath(xpath).click() # csv 다운로드 클릭
        # 파일명 변경
        while len(glob(folder+'*.csv'))==file_count:
            time.sleep(1)
        time.sleep(2)
        if len(glob(folder+'data*.csv'))>0:
            os.rename(folder+"data.csv", folder+f"{code}_{from_date}_{to_date}.csv")
        time.sleep(1.5)
        file_count = len(glob(folder+'*.csv'))
        #---------------------------------------------------------------------------------
    df = pd.DataFrame()
    for file in glob(folder+'*.csv'):
        temp = pd.read_csv(file, encoding='utf-8')
        # 파일명에 있는 코드부분을 컬럼으로 추가
        temp['종목코드'] = file.split('_')[0].split('\\')[-1][1:]
        df = df.append(temp)
    df.rename(columns={'년/월/일':'날짜'}, inplace=True)
    filt = df['종가'].notna()
    df = df.filt[filt].copy()
    df = df.drop_duplicates(subset=['종목코드','날짜']).reset_index(drop=True)
    df = helper.remove_str_from_colname(df,'(주)')
    cols = ['종가', '대비','거래량',
            '기관_매수량', '기관_매도량', '기관_순매수',
            '외국인_매수량', '외국인_매도량','외국인_순매수']
    for col in cols:
        df[col] = df[col].str.replace(',','')
        df[col] = df[col].astype(float)
    # 저장하고 다운로드 폴더에 있는 모든 파일 삭제
    df.to_pickle(save_folder+'기관+외국인거래실적.pkl')
    helper.del_all_files_in_download()

# 코스피, 코스닥 지수 업데이트
def update_market_index():
    '''
    코스피, 코스닥 지수 업데이트
    '''
    import FinanceDataReader as fdr

    mkt = {'ks11':'import_kospi_all',
            'kq11':'import_kosdaq_all'}
    for x in mkt.keys():
        df = fdr.DataReader(x, start= '19800101').reset_index()
        conn_db.to_(df, '코스피_코스닥지수', mkt[x])
    print('코스피, 코스닥 지수 업데이트 완료')

# PER/PBR/배당수익률_하루치
def get_per_pbr_div_yld_from_krx(update_date):
    '''
    yyyy-mm-dd 형식으로 날짜 입력
    [12021] PER/PBR/배당수익률(개별종목)
    '''
    url = 'http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd'
    header_info = {'User-Agent': user_agent}
    params ={'bld':'dbms/MDC/STAT/standard/MDCSTAT03501',
             'searchType':'1',
             'mktId':'ALL',
             'trdDd': str(update_date)}
    r = requests.post(url, data=params, headers=header_info)

    df = pd.DataFrame(r.json()['output'])
    df['날짜'] = str(update_date)
    df['날짜'] = pd.to_datetime(df['날짜'])

    cols = ['날짜', 'ISU_SRT_CD','EPS','PER','BPS','PBR','DPS','DVD_YLD']
    names = {'ISU_SRT_CD':'종목코드',
             'DVD_YLD':'배당수익률'}
    df = df[cols].rename(columns=names)

    cols = ['EPS','PER','BPS','PBR','DPS','배당수익률']
    for col in cols:
        df[col] = df[col].str.replace(',','')
        df[col] = pd.to_numeric(df[col],errors='coerce')

    return df
# PER/PBR/배당수익률
def update_per_pbr_div_from_krx():
    '''
    KRX에서 [12021] PER/PBR/배당수익률(개별종목) 업데이트
    '''
    new_dates = conn_db.from_('코스피_코스닥지수', '최근날짜')[['Date']]
    new_dates = new_dates['Date'].str.replace('-','').tolist()

    df = conn_db.import_('PER_PBR_배당수익률_전체취합본')[['날짜']]
    df = df.drop_duplicates().reset_index(drop=True)
    df = df['날짜'].astype(str).str.replace('-','')

    new_dates = list(set(new_dates) - set(df))
    new_dates.sort(reverse=True)
    folder = conn_db.get_path('perpbr배당수익률_raw')

    if len(new_dates)>0: 
        for update_date in new_dates:
            try:
                df = get_per_pbr_div_yld_from_krx(update_date)
            except:
                time.sleep(2)
                df = get_per_pbr_div_yld_from_krx(update_date)
            df.to_pickle(folder+f'{update_date}'+'_PERPBR배당수익률.pkl')
            time.sleep(5)

        # 새로 받은 파일 취합
        files = glob(folder+'*.pkl')
        df = pd.concat([pd.read_pickle(file) for file in files], ignore_index=True)
        df = df.drop_duplicates().reset_index(drop=True)
        df = helper.make_keycode(df) # KEY 컬럼 추가

        # 전체취합본 불러와서 추가한 다음에 저장
        old_df = conn_db.import_('PER_PBR_배당수익률_전체취합본')
        cols = ['KEY', '날짜']
        df = helper.add_df(df, old_df, check_cols=cols)
        df.to_pickle(conn_db.get_path('PER_PBR_배당수익률_전체취합본')+'.pkl')
    else:
        print('업데이트 내역 없음')

# 시가총액, 주가, 거래량 하루치
def get_market_cap(update_date):
    url = 'http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd'
    header_info = {'User-Agent': user_agent}
    params = {'bld': 'dbms/MDC/STAT/standard/MDCSTAT01501',
              'share': '1',
              'mktId': 'ALL',
              'trdDd': str(update_date)}

    r = requests.post(url, data=params, headers=header_info)
    df = pd.DataFrame(r.json()['OutBlock_1'])
    df.insert(0, 'DATE', pd.to_datetime(update_date, format='%Y%m%d'))
    df['DATE'] = df['DATE'].astype(str)

    # 형변환할 컬럼들
    cols = ['TDD_CLSPRC', 'TDD_OPNPRC',
            'TDD_HGPRC', 'TDD_LWPRC', 'ACC_TRDVOL',
            'ACC_TRDVAL', 'MKTCAP', 'LIST_SHRS']
    for col in cols:
        df[col] = df[col].str.replace(',', '')
        df[col] = df[col].astype('int64')
    # 사용하지 않는 컬럼 삭제
    for col in ['FLUC_TP_CD', 'MKT_ID',
                'CMPPREVDD_PRC',  # w전일대비
                'FLUC_RT',  # 전일비
                'SECT_TP_NM']:  # 소속부
        df.drop(columns=col, inplace=True)
    # 컬럼 이름 변경
    names = {'ISU_SRT_CD': '종목코드',
             'ISU_ABBRV': '종목명',
             'MKT_NM': '시장',
             'TDD_CLSPRC': 'close',
             'TDD_OPNPRC': 'open',
             'TDD_HGPRC': 'high',
             'TDD_LWPRC': 'low',
             'ACC_TRDVOL': '거래량',
             'ACC_TRDVAL': '거래대금',
             'MKTCAP': '시가총액',
             'LIST_SHRS': '상장주식수'}
    df.rename(columns=names, inplace=True)
    df['DATE'] = pd.to_datetime(df['DATE'])
    return df
# 시가총액, 주가, 거래량
def update_market_cap():
    '''
    krx에서 시가총액 데이터 받아와서 저장
    '''
    new_dates = conn_db.from_('코스피_코스닥지수', '최근날짜')[['Date']]
    new_dates = new_dates['Date'].str.replace('-', '').tolist()

    df = conn_db.import_('시가총액_전체취합본')[['DATE']]
    df = df.drop_duplicates().reset_index(drop=True)
    df = df['DATE'].astype(str).str.replace('-', '')

    new_dates = list(set(new_dates) - set(df))
    new_dates.sort(reverse=True)

    if len(new_dates)>0:
        folder = conn_db.get_path('시가총액_raw')
        for update_date in new_dates:
            try:
                df = get_market_cap(update_date)
            except:
                time.sleep(2)
                df = get_market_cap(update_date)
            df.to_pickle(folder+f'{update_date}'+'_시가총액.pkl')
            time.sleep(5)

        # 새로 받은 파일 취합
        files = glob(folder+'*.pkl')
        df = pd.concat([pd.read_pickle(file) for file in files], ignore_index=True)
        df = df.drop_duplicates().reset_index(drop=True)
        df = helper.make_keycode(df) # KEY 컬럼 추가

        # 전체취합본 불러와서 추가한 다음에 저장
        old_df = pd.read_pickle(path)
        cols = ['KEY', 'DATE']
        helper.add_df(df, old_df, check_cols=cols)
        df.to_pickle(conn_db.get_path('시가총액_전체취합본')+'.pkl')
    else:
        print('업데이트 내역 없음')

    #################################################
    # Financial Datareader에서 받아온 과거 자료 전체 전처리
    # names = {'Date':'DATE',
    #      'Code': '종목코드',
    #      'Name': '종목명',
    #      'Market': '시장',
    #      'Close': 'close',
    #      'Open': 'open',
    #      'High': 'high',
    #      'Low': 'low',
    #      'Volume': '거래량',
    #      'Amount': '거래대금',
    #      'Marcap': '시가총액',
    #      'Stocks': '상장주식수'}

    # dtypes = {'Code': str,
    #         'Name': str,
    #         'Market': str,
    #         'Close': 'int64',
    #         'Open': 'int64',
    #         'High': 'int64',
    #         'Low': 'int64',
    #         'Volume': 'int64',
    #         'Amount': 'int64',
    #         'Marcap': 'int64',
    #         'Stocks': 'int64'}
    # df = pd.DataFrame()
    # files = glob(r"C:\Users\bong2\marcap\data\\"+'*.gz')
    # for file in files:
    #     temp = pd.read_csv(file, dtype=dtypes, parse_dates=['Date'])
    #     temp = temp[names.keys()]
    #     for x in list(names.keys())[4:]:
    #         try:
    #             temp[x] = temp[x].astype('int64')
    #         except:
    #             print(x)
    #     df = df.append(temp, ignore_index=True)

    # df.drop(columns='Dept', inplace=True)
    # df.rename(columns=names, inplace=True)
    #################################################

def get_market_index_type(update_date, index_type):
    url = 'http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd'
    header_info = {'User-Agent': user_agent}
    params = {'bld': 'dbms/MDC/STAT/standard/MDCSTAT00101',
              'idxIndMidclssCd': f'{index_type}',
              'share': 1,
              'money': 1,
              'trdDd': str(update_date)}

    r = requests.post(url, data=params, headers=header_info)
    df = pd.DataFrame(r.json()['output'])

    col_names = {'IDX_NM': '지수명',
                 'CLSPRC_IDX': '종가',
                 'OPNPRC_IDX': '시가',
                 'HGPRC_IDX': '고가',
                 'LWPRC_IDX': '저가',
                 'ACC_TRDVOL': '거래량',
                 'ACC_TRDVAL': '거래대금',
                 'MKTCAP': '시가총액'}
    df.rename(columns=col_names, inplace=True)

    num_cols = ['종가', '시가', '고가', '저가',
                '거래량', '거래대금', '시가총액']
    for col in num_cols:
        df[col] = df[col].str.replace(',', '')
        df[col] = df[col].str.replace('-', '')
        df[col] = pd.to_numeric(df[col])

    cols = list(col_names.values())
    return df[cols]
def get_market_index_types(update_date):
    index_types = {'01': 'KRX',
                   '02': 'KOSPI',
                   '03': 'KOSDAQ',
                   '04': '테마'}
    df = pd.DataFrame()
    for index_type in index_types.keys():
        temp = get_market_index_type(update_date, index_type)
        temp['계열구분'] = index_types[index_type]
        df = df.append(temp, ignore_index=True)

    cols = ['지수명', '종가', '시가', '고가', '저가',
            '거래량', '거래대금', '시가총액']
    df['날짜'] = str(update_date)
    df['날짜'] = pd.to_datetime(df['날짜'])
    cols = ['날짜', '계열구분'] + cols

    # 저장
    return df[cols]

def update_market_index():
    new_dates = conn_db.from_('코스피_코스닥지수', '최근날짜')[['Date']]
    new_dates = new_dates['Date'].str.replace('-', '').tolist()
    path = conn_db.get_path('업종별지수')

    for new_date in new_dates:
        df = get_market_index_types(new_date)
        df.to_pickle(path + f'업종별지수_{new_date}.pkl')
        time.sleep(5)

    df = pd.concat([pd.read_pickle(file)
                   for file in glob(path+'*.pkl')], ignore_index=True)
    df.to_pickle(conn_db.get_path('업종별지수_취합본')+'.pkl')
    print('업종별 지수 업데이트 완료')

def get_market_index_type_per(update_date, index_type):
    url = 'http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd'
    header_info = {'User-Agent': user_agent}
    params = {'bld': 'dbms/MDC/STAT/standard/MDCSTAT00701',
              'searchType': 'A',
              'idxIndMidclssCd': f'{index_type}',
              'trdDd': str(update_date)}
    r = requests.post(url, data=params, headers=header_info)
    df = pd.DataFrame(r.json()['output'])

    col_names = {'IDX_NM': '지수명',
                 'CLSPRC_IDX': '종가',
                 'WT_PER': 'PER',
                 'WT_STKPRC_NETASST_RTO': 'PBR',
                 'DIV_YD': '배당수익률'}

    df.rename(columns=col_names, inplace=True)

    num_cols = ['종가', 'PER', 'PBR', '배당수익률']
    for col in num_cols:
        df[col] = df[col].str.replace(',', '')
        df[col] = df[col].str.replace('-', '')
        df[col] = pd.to_numeric(df[col])

    cols = list(col_names.values())
    return df[cols]
def get_market_index_types_per(update_date):
    index_types = {'01': 'KRX',
                   '02': 'KOSPI',
                   '03': 'KOSDAQ',
                   '04': '테마'}
    df = pd.DataFrame()
    for index_type in index_types.keys():
        temp = get_market_index_type(update_date, index_type)
        temp['계열구분'] = index_types[index_type]
        df = df.append(temp, ignore_index=True)

    cols = ['지수명', '종가', 'PER', 'PBR', '배당수익률']
    df['날짜'] = str(update_date)
    df['날짜'] = pd.to_datetime(df['날짜'])
    cols = ['날짜', '계열구분'] + cols

    # 저장
    return df[cols]

def update_market_index_per():
    new_dates = conn_db.from_('코스피_코스닥지수', '최근날짜')[['Date']]
    new_dates = new_dates['Date'].str.replace('-', '').tolist()
    path = conn_db.get_path('업종별지수_PERPBR배당수익률')

    for new_date in new_dates:
        df = get_market_index_types_per(new_date)
        df.to_pickle(path + f'업종별지수_PERPBR배당수익률{new_date}.pkl')
        time.sleep(5)

    df = pd.concat([pd.read_pickle(file)
                   for file in glob(path+'*.pkl')], ignore_index=True)
    df.to_pickle(conn_db.get_path('업종별지수_PERPBR배당수익률_취합본')+'.pkl')
    print('업종별 지수 PER,PBR,배당수익률 업데이트 완료')


