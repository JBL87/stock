import pandas as pd
from bs4 import BeautifulSoup
import requests, time
import conn_db
import helper
user_agent = helper.user_agent

# 네이버 증권에 있는 테마별 회사정보 가져오기
def update_co_by_theme_from_naver():
    '''
    네이버 증권에 있는 테마별 회사정보 가져오기
    '''
    #append 시킬 비어있는 df 생성
    theme_list = pd.DataFrame()
    #네이버 테마 페이지가 7페이지까지 있음. 1페이지씩 돌면서 정보 가져오기
    for page in range(1,8):
        time.sleep(2)
        url = "https://finance.naver.com/sise/theme.nhn?&page={}".format(page)  #loop에서 page 받기
        response = requests.get(url, headers = {'User-Agent': user_agent})
        dom = BeautifulSoup(response.text, "html.parser")
        elements = dom.select("#contentarea_left > table.type_1.theme > tr")
        # 빈 리스트 생성
        temp = []
        for element in elements[3:-2]:
            try:
                data = {
                    "theme": element.select("td")[0].text,
                    "link": "https://finance.naver.com/" + element.select("td")[0].select_one("a").get("href")
                }
            except:
                pass
            temp.append(data) #비어있는 리스트에 채우기
        #회별 loop 완료후 theme_list에 채우기
        theme_list = theme_list.append(temp, ignore_index=True)
    
    co_by_theme = pd.DataFrame() #append 시킬 비어있는 df 생성
    for theme, link in theme_list.values:
        response = requests.get(link, headers = {'User-Agent': user_agent})
        parse = BeautifulSoup(response.text, "html.parser")
        co_list = parse.find_all('div',{'class': 'name_area'}) # 종목명과 종목 코드 추출할 리스트
        co_info = parse.find_all('p',{'class': 'info_txt'}) # 종목설명 추출할 리스트
    
        # 종목별 회사명, 종목코드 추출
        temp_name = [item.text.split('*')[0] for item in co_list] 
        temp_code = [item.select_one('a').get('href').split('=')[1] for item in co_list]
    
        #종목별 설명 추출
        temp_info = [info.text.split('*')[0] for info in co_info]
        temp_theme_info = temp_info[0] # 0번있는 테마 설명 저장
        temp_co_info = temp_info[1:] # 0번에는 테마 설명은 제외하고 그 다음부터 있는 종목 설명 저장
        
        #df로 만들기
        temp_df = pd.DataFrame(temp_name) #종목명 추가
        temp_df['종목코드'] = pd.DataFrame(temp_code) #종목코드 추가
        temp_df['종목설명'] = pd.DataFrame(temp_co_info) #종목설명 추가
        temp_df['테마설명'] = str(temp_theme_info) #테마설명 추가
        temp_df['테마명'] = str(theme)  #테마명 추가
        
        #최종 df에 추가
        co_by_theme = co_by_theme.append(temp_df, ignore_index=False)
    
    # 컬럼명 0으로 되어 있는 '종목명'은 삭제. KRX와 종목코드로 mapping해서 거기 있는 종목명 사용
    co_by_theme = co_by_theme.drop(columns=[0]).reset_index(drop=True)

    # KRX df 불러와서 KEY 컬럼 만들기
    df_map_code = conn_db.from_('DB_기업정보', 'from_krx')[['종목코드', 'KEY','종목명']]
    co_by_theme = co_by_theme.merge(df_map_code, on='종목코드')


    # 오류수정 ---------------------------------------------------------
    # co_by_theme.loc[co_by_theme['종목코드']=='091340', '테마명'] = 'OLED(유기 발광 다이오드)'
    
    co_by_theme = co_by_theme.drop_duplicates().reset_index(drop=True)
    conn_db.to_(co_by_theme, 'DB_기업정보', 'from_naver_theme')

    # 회사별 테마 한줄 짜리 df 만들어서 저장
    co_by_theme.drop(columns=['테마설명'], inplace=True)
    cols = ['종목명', '종목코드','KEY']
    co_by_theme = co_by_theme.groupby(cols)['테마명'].apply(', '.join).reset_index()
    co_by_theme = co_by_theme.drop_duplicates().reset_index(drop=True)
    conn_db.to_(co_by_theme, 'DB_기업정보', 'from_naver_theme2')

# 네이버 증권에 있는 회사별 업종정보 가져오기
def update_co_by_industry_from_naver():
    '''
    네이버 증권에 있는 회사별 업종정보 가져오기
    '''
    url = "https://finance.naver.com/sise/sise_group.nhn?type=upjong"
    r = requests.get(url, headers = {'User-Agent': user_agent})
    dom = BeautifulSoup(r.text, "html.parser")
    elements = dom.select("#contentarea_left > table > tr > td > a")
    
    industry_list = pd.DataFrame()
    industry_list['업종명'] = [item.text for item in elements]
    industry_list['link'] = [item.get('href') for item in elements]
    industry_list['link'] = "https://finance.naver.com" + industry_list['link'].astype(str)
    
    co_by_industry = pd.DataFrame()
    for industry, link in industry_list.values:
        time.sleep(2)
        r = requests.get(link, headers = {'User-Agent': user_agent})
        parse = BeautifulSoup(r.text, "html.parser")
        items = parse.find_all('div',{'class': 'name_area'})
    
        temp_df = pd.DataFrame()
        temp_df['종목명'] = [item.text.split('*')[0].rstrip() for item in items]
        temp_df['종목코드'] = [item.select_one('a').get('href').split('=')[-1] for item in items]
        temp_df['industry'] = str(industry)
        co_by_industry = co_by_industry.append(temp_df, ignore_index=True)
    
    # '종목명'은 삭제. KRX와 종목코드로 mapping해서 거기 있는 종목명 사용
    co_by_industry.drop(columns=['종목명'], inplace=True)
    co_by_industry = co_by_industry.drop_duplicates().reset_index(drop=True)
    
    cols = ['종목코드', 'KEY','종목명']
    df_map_code = conn_db.from_('DB_기업정보', 'from_krx')[cols]
    co_by_industry = co_by_industry.merge(df_map_code, on='종목코드')
    co_by_industry = co_by_industry.drop_duplicates().reset_index(drop=True)
    conn_db.to_(co_by_industry, 'DB_기업정보', 'from_naver_industry')

# 네이버 업종별 PER 업데이트
def update_naver_industry_per():
    print('네이버 업종별 PER 가져오기 시작')
    cols = ['종목코드','industry']
    industry_df = conn_db.from_('DB_기업정보','from_naver_industry')[cols]
    industry_df = industry_df.groupby(['industry'],as_index='False').head(1)
    
    for code in industry_df['종목코드'].tolist():
        url = f'https://finance.naver.com/item/main.nhn?code={code}'
        r = requests.get(url, headers={'User-Agent': user_agent})
        dom = BeautifulSoup(r.content, "lxml")
        industry_per = dom.select('#tab_con1 > div > table > tr > td > em')[-2].text
        industry_df.loc[industry_df['종목코드']==code,'업종PER'] = industry_per
    industry_df = industry_df.rename(columns={'industry':'업종_naver'})
    industry_df = industry_df.drop(columns='종목코드').reset_index(drop=True)

    filt = industry_df['업종PER'].str.contains(',')
    industry_df.loc[filt, '업종PER'] = industry_df.loc[filt, '업종PER'].str.replace(',','')
    industry_df['업종PER'] = pd.to_numeric(industry_df['업종PER'], errors='coerce')

    conn_db.to_(industry_df,'DB_기업정보','네이버업종PER')
    print('네이버 업종별 PER 가져오기 완료')

# 네이버증권에서 종목 설명 정보 전처리
def update_company_explain_from_naver(param='all'):
    print('네이버에서 기업설명 가져오기 시작')
    def get_company_explain_from_naver(code): # 네이버증권에서 종목 설명 정보 가져오기
        # time.sleep(2)
        url = f'https://finance.naver.com/item/main.nhn?code={code}'
        r = requests.get(url, headers={'User-Agent': user_agent})
        dom = BeautifulSoup(r.content, "lxml")
        try:
            co_info = ' '.join([text.text.strip() for text in dom.select('#summary_info > p')])
            # 코드 추가해서 df로 만들기 + 행/열 전환 한 다음에 return
            return pd.DataFrame([code, co_info]).T
        except:
            print(f'{code} 데이터가져오기 실패')
    
    if param=='all':
        df = pd.concat([get_company_explain_from_naver(code) for code in code_list]).reset_index(drop=True)
        # with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        #     result = executor.map(get_company_explain_from_naver, code_list)
        #     df = pd.concat([df for df in result], axis=0)
        #     del result
    else:
        new = conn_db.from_("DB_기업정보", 'FS_update_list')[['KEY','종목코드']]
        old = conn_db.from_("DB_기업정보", '기업설명_naver')['KEY']
        code_list_added = pd.DataFrame(list(set(new['KEY']) - set(old)))
    
        if len(code_list_added)>0:
            code_list_added = code_list_added.merge(new, left_on=0, right_on='KEY')['종목코드']
            df = pd.concat([get_company_explain_from_naver(code) for code in code_list_added], axis=0)
        else:
            print('업데이트할 내역 없음')
        del new, old, code_list_added
    
    # 전처리
    df.rename(columns={0: '종목코드', 1: '종목설명'}, inplace=True)
    df['종목설명'] = df['종목설명'].str.replace('동사는', '').str.strip()
    df = helper.make_keycode(df)  # KEY 컬럼 만들기
    df = df.drop(columns=['종목코드', '종목명']).reset_index(drop=True) # KEY컬럼과 종목설명 컬럼만 남기기
    
    # 업데이트
    old_df = conn_db.from_('DB_기업정보', '기업설명_naver')
    df = pd.concat([df, old_df], axis=0).drop_duplicates(subset=['KEY'])
    conn_db.to_(df, 'DB_기업정보', '기업설명_naver')

# KRX와 KIND에서 받은 기업정보 가져오기
def update_co_from_krx_n_kind():
    '''
    KRX와 KIND에서 받은 기업정보 가져오기
    '''
    krx_import = conn_db.from_('KRX&KIND_원본업로드','KRX_new_clean')
    kind_import = conn_db.from_('KRX&KIND_원본업로드','import_KIND_상장법인목록')

    kind_import.drop(columns = ['대표자명'], inplace=True)
    kind_import['상장일'] = pd.to_datetime(kind_import['상장일'])

    # 유효한 종목만 필터링. 구글시트에서 미리 처리
    valid_codes = krx_import['종목코드'] # 구글시트 KRX&KIND_원본업로드에 있는 "KRX_new_clean" 시트가 기준
    kind_import = kind_import[kind_import['종목코드'].isin(valid_codes)].copy().reset_index(drop=True)

    # 글자랑 쉼표사이 공백이 없는 경우가 있음
    kind_import['주요제품'] = kind_import['주요제품'].str.replace(',', ', ')
    # 공란2개를 1개로 처리
    kind_import['주요제품'] = kind_import['주요제품'].str.replace('  ', ' ')

    list_co_info = pd.merge(krx_import, kind_import, how='inner', on='종목코드')

    list_co_info['KEY'] = list_co_info['시장'] + list_co_info['종목코드']
    cols = ['종목명','종목코드','업종','주요제품','상장일','결산월',
            '대표이사','홈페이지','지역','시장','업종코드','상장주식수(주)',
            '자본금(원)','액면가(원)','통화구분','대표전화','주소','KEY']
    list_co_info = list_co_info[cols].drop_duplicates().reset_index(drop=True)
    conn_db.to_(list_co_info, 'DB_기업정보', 'from_krx')

    print('KRX, KIND 업데이트 완료')

# Dart에 있는 기업정보 업데이트
def update_co_from_dart(param='all'):
    '''
    Dart에 있는 기업정보 업데이트
    '''
    import dart_fss as dart
    dart.set_api_key(helper.dart_api_key)

    # 전체 회사 코드 list
    raw_corp_code_list = pd.DataFrame(dart.api.filings.get_corp_code())

    #stock_code가 null인것 제거(비상장사 제외하기)
    if param != 'all':
        # 업데이트 필요한 종목코드만 불러와서 종목코드별 회사코드 얻기
        all_codes = conn_db.from_('DB_기업정보', 'dart_update_codes')
        filt = all_codes['update_codes'].apply(len)>1
        valid_codes = all_codes[filt]['update_codes'].tolist()
        filt = raw_corp_code_list['stock_code'].isin(valid_codes)
        del all_codes
    else:
        filt = raw_corp_code_list.stock_code.notnull()

    listcorp_list = raw_corp_code_list[filt]['corp_code'].tolist()

    if len(listcorp_list)>0:
        #기업코드별로 기업정보 가져오기
        corp_info_raw = pd.DataFrame()
        for code in listcorp_list:
            temp = dart.api.filings.get_corp_info(code)
            temp = pd.DataFrame.from_dict(temp, orient='index').T
            corp_info_raw = corp_info_raw.append(temp)

        # 제외조건 : status != 000 | corp_cls = 'E'
        filt1 = corp_info_raw['corp_cls'] != 'E'  # 법인유형이 '기타'가 아닌 것만 선택
        filt2 = corp_info_raw['status'] == '000'  # status가 000인것만 선택
        corp_info = corp_info_raw.loc[filt1 & filt2, :].copy()

        # 법인구분 : Y(유가), K(코스닥), N(코넥스), E(기타)_ from 다트홈페이지
        # 영문코드로 되어 있는 corp_cls 컬럼을 한글로 변환
        corp_mkt_map = {'Y':'KOSPI',
                        'K':'KOSDAQ',
                        'N':'코넥스',
                        'E':'기타'}
        corp_info['corp_cls'] = corp_info['corp_cls'].map(corp_mkt_map).copy()

        #export할 컬럼만 선택
        cols = ['corp_code', 'corp_name', 'stock_code', 'corp_cls',
                'jurir_no', 'bizr_no', 'adres', 'induty_code', 'est_dt', 'acc_mt']

        # 필요한 컬럼만 선택
        corp_info = corp_info.loc[:, cols].copy()

        #영문으로 되어 있는 컬럼명 한글로 수정
        names = {'corp_code':'회사코드',
                 'corp_name':'회사명',
                 'stock_code':'종목코드',
                 'corp_cls':'시장',
                 'jurir_no':'법인등록번호',
                 'bizr_no':'사업자등록번호',
                 'adres':'주소',
                 'induty_code':'업종코드',
                 'est_dt':'설립일자',
                 'acc_mt':'결산월'}
        corp_info.rename(columns=names, inplace=True)

        corp_info['업종코드기준'] = corp_info['업종코드'].apply(lambda x: len(x))
        mapinfo = {5:'세세분류',
                   4:'세분류',
                   3:'소분류'}
        corp_info['업종코드기준'] = corp_info['업종코드기준'].map(mapinfo)

        corp_info = helper.make_keycode(corp_info)

        # 취합후 업로드
        old = conn_db.from_('DB_기업정보', 'from_dart')
        corp_info = corp_info.append(old, ignore_index=True)
        corp_info = corp_info.drop_duplicates().reset_index(drop=True)
        conn_db.to_(corp_info, 'DB_기업정보', 'from_dart') 
    else:
        print('DART 기업정보 업데이할 내역 없음')

# Finance DataReader에 있는 기업정보
def update_co_from_fdr():
    '''
    FinanceDataReader에 있는 기업정보
    '''
    import FinanceDataReader as fdr

    df_krx = fdr.StockListing('KRX')  # krx종목별 정보 df 불러오기
    df_krx.rename({'Symbol': '종목코드'}, axis=1, inplace=True)
    
    # 종목 리스트에서 우량주는 제외. 제외할 종목코드 필터링 방법은 'SettleMonth'컬럼이 null인것
    rows = df_krx['SettleMonth'].notna()
    cols = ['종목코드', 'Sector', 'Industry'] # 사용할 컬럼
    co_info = df_krx.loc[rows, cols].reset_index(drop=True) # 실제 사용할 종목코드가 있는 df
    
    co_info = helper.make_keycode(co_info)
    conn_db.to_(co_info, 'DB_기업정보', 'from_fdr')  

# KRX 기업정보 취합
def get_all_code_info_from_krx():
    '''
    기업정보_간단한 버전 + 전체 버전 업로드
    '''
    url = 'http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd'
    header_info = {'User-Agent': user_agent}

    df = pd.DataFrame()
    params = {'bld': 'dbms/MDC/STAT/standard/MDCSTAT01901',
              'share': '1',
              'mktId': 'ALL',
              'csvxls_isNo': 'false'}
    r = requests.post(url, data=params, headers=header_info)
    df = pd.DataFrame(r.json()['OutBlock_1'])
    names = {'ISU_SRT_CD': '종목코드',
             'ISU_NM': '종목명(전체)',
             'ISU_ENG_NM': '영문',
             'ISU_ABBRV': '종목명',
             'LIST_DD': '상장일',
             'MKT_TP_NM': '시장',
             'SECT_TP_NM': '소속부',
             'KIND_STKCERT_TP_NM': '주식종류',
             'PARVAL': '액면가(원)',
             'LIST_SHRS': '상장주식수(주)'}
    df = df[names.keys()].rename(columns=names)
    conn_db.to_(df, 'KRX&KIND_원본업로드', 'import_KRX_new')
    #----------------------------------------------------------------

    url = 'http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd'
    header_info = {'User-Agent': user_agent}

    df = pd.DataFrame()
    params = {'bld': 'dbms/MDC/STAT/standard/MDCSTAT03402',
              'mktTpCd': '0',
              'tboxisuSrtCd_finder_listisu0_2': '전체',
              'isuSrtCd': 'ALL',
              'isuSrtCd2': 'ALL',
              'mktcap': 'ALL', }
    r = requests.post(url, data=params, headers=header_info, timeout=5)
    df = pd.DataFrame(r.json()['block1'])
    names = {'REP_ISU_SRT_CD': '종목코드',
             'COM_ABBRV': '종목명',
             'MKT_NM': '시장',
             'STD_IND_CD': '업종코드',
             'IND_NM': '업종명',
             'ACNTCLS_MM': '결산월',
             'PARVAL': '액면가(원)',
             'CAP': '자본금(원)',
             'ISO_CD': '통화구분',
             'CEO_NM': '대표이사',
             'TEL_NO': '대표전화',
             'ADDR': '주소',
             'LIST_SHRS': '상장주식수(주)'}
    cols = ['SECT_TP_NM', 'SUB_IDX_IND_NM', 'MKTPARTC_NM']
    df = df.rename(columns=names).drop(columns=cols)
    conn_db.to_(df, 'KRX&KIND_원본업로드', 'import_KRX_long')

# 다른 출처에서 가져온 기업정보 전체 취합
def get_all_co_info():
    '''
    다른 출처에서 가져온 기업정보 전체 취합
    '''
    # 출처별 기업정보 합치기 전에 컬럼 정리
    # KRX
    co_info = conn_db.from_('DB_기업정보', 'from_krx')
    co_info.drop(columns={'홈페이지','주소','대표전화'}, inplace=True)
    co_info.rename(columns={'업종': '업종_krx'}, inplace=True)
    # 네이버 업종
    naver_industry = conn_db.from_('DB_기업정보', 'from_naver_industry')[['KEY', 'industry']]
    naver_industry.rename(columns={'industry': '업종_naver'}, inplace=True)
    # 네이버 기업설명
    # co_explain = conn_db.from_('DB_기업정보', '기업설명_naver')[['KEY', '종목설명']]
    # → fn guide에 있는 것과 중복되어서 생략

    # 네이버 테마
    naver_theme = conn_db.from_('DB_기업정보', 'from_naver_theme2')[['KEY','테마명']].drop_duplicates()
    # DART
    co_dart = conn_db.from_('DB_기업정보', 'from_dart')[['KEY', '회사코드', '회사명', '업종코드기준','업종코드']]
    #------ ------ ------ ------ ------
    # 하나로 합치기
    list_of_dfs = [naver_industry, naver_theme, co_dart]
    for dfs in list_of_dfs:
        co_info = co_info.merge(dfs, on='KEY', how='left')
    #3번 파일 정리
    co_info['회사명'] = co_info['회사명'].fillna(co_info['종목명']) # 회사명이 null이면 종목명으로 채우기
    co_info.rename(columns={'업종코드_x': '업종코드_krx',
                            '업종코드_y': '업종코드_dart'}, inplace=True)
    
    # 오류수정 ---------------------------------------------------------
    # co_info.loc[co_info['종목명']=='바른손','결산월'] = '03월'
    
    exclude_list = conn_db.from_('DB_기업정보', '제외할list')['종목코드'].drop_duplicates()
    filt = co_info['종목코드'].isin(exclude_list)
    co_info = co_info.loc[~filt].copy()

    # 코넥스 제외
    filt = co_info['시장'] !='코넥스'
    co_info = co_info.loc[filt].copy()
    
    # 컬럼순서 정리하고 fnguide에서 가져온 내용 합친 다음 업로드
    df_fnguide = conn_db.from_('DB_기업정보','from_fnguide_기업정보').drop(columns=['종목코드','종목명'])
    df_fnguide.rename(columns={'내용':'실적내용', '요약':'실적요약',
                                'FICS':'FICS 업종','KRX':'업종_krx2'}, inplace=True)
    # 컬럼순서
    cols = [ 'KEY', '종목코드', '종목명', '시장', '업종_krx', '업종_krx2','업종_naver',
            'FICS 업종', '테마명', '주요제품', '실적요약', '실적내용', '기준날짜',
            '업종코드_dart', '업종코드기준', '업종코드_krx', '상장일', '결산월', '지역',
            '상장주식수(주)', '자본금(원)','액면가(원)', '통화구분', '회사코드', '회사명']
    co_info = co_info.merge(df_fnguide, on='KEY', how='left')[cols]
    conn_db.to_(co_info, 'DB_기업정보', '취합본')
    print('전체 기업정보 업데이트 완료')

    # 업종별 회사를 산업/업종 master에 업로드. 업종별 광공업지수와 비교분석하기 위함
    conn_db.to_(co_info.groupby(['업종_krx'])['종목명'].apply(', '.join).reset_index(),
                    'Master_산업,업종', '업종별회사')
    
    # 한국표준산업별로 종목 구분하기
    use_cols = ['KEY', '종목명', '업종_krx', '업종_krx2', '업종_naver',
                'FICS 업종', '테마명', '업종코드_dart', '업종코드기준']
    co_info = co_info[use_cols]
    df_industry = conn_db.from_('Master_산업,업종','한국표준산업분류')
    #-------
    def clean_df(string):
        temp = co_info.loc[co_info['업종코드기준'] == string].copy()
        return temp.merge(df_industry, left_on='업종코드_dart', right_on=f'{string}_코드', how='left')
    #-------
    df_1 = clean_df('소분류')
    df_2 = clean_df('세분류')
    df_3 = clean_df('세세분류')
    
    list_of_dfs = [df_1, df_2, df_3]
    for dfs in list_of_dfs:
        dfs = helper.remove_str_from_colname(helper.drop_column(dfs,'_코드'),'_항목명')
    df_1 = df_1.drop(columns=['세분류', '세세분류']).drop_duplicates().reset_index(drop=True)
    df_2 = df_2.drop(columns=['세세분류']).drop_duplicates().reset_index(drop=True)
    
    # 컬럼순서 정리
    class_cols = ['대분류', '중분류', '소분류', '세분류', '세세분류']
    use_cols = use_cols + class_cols
    df = pd.concat([df_1, df_2, df_3])[use_cols].reset_index(drop=True)
    
    for col in class_cols:
        df[col] = df[col].str.split('(',expand=True)[0]
    
    df = df.drop_duplicates().reset_index(drop=True)
    conn_db.to_(df, 'Master_산업,업종','한국표준산업분류별_종목')

    # 기업정보 취합본 + 한국표준산업분류별_종목 + 업종별 설명 + 개별테마 + 아이투자_기업정보까지 모두 다 합친 것
    # 1.전체 취합본 불러오기
    cols = ['업종코드_krx','통화구분','액면가(원)', 
            '상장주식수(주)','자본금(원)','회사코드','기준날짜']
    df = conn_db.from_('DB_기업정보','취합본').drop(columns=cols)

    # 2.아이투자_기업정보 불러와서 구분별로 컬럼 만들기
    df_description =  conn_db.from_('DB_기업정보','from_아이투자_기업정보')[['KEY','구분','내용']]
    df_all = pd.DataFrame()
    for x in df_description['구분'].unique():
        temp = df_description[df_description['구분']==x].rename(columns={'내용':x}).drop(columns='구분')
        if len(df_all)==0:
            df_all = df_all.append(temp)
        else:
            df_all = df_all.merge(temp, on='KEY')
    df_all = df_all.rename(columns={'주요제품':'주요제품 (상세)'}).drop_duplicates()
    
    # 3.한국표준산업분류별_종목 불러오기
    cols = ['KEY','대분류','중분류','소분류','세분류','세세분류']
    df_industry = conn_db.from_('Master_산업,업종','한국표준산업분류별_종목')[cols]
    
    # 4. KRX업종별 종목 가져오기
    co_by_industry = conn_db.from_('Master_산업,업종', '업종별회사')
    co_by_industry.rename(columns={'종목명':'krx업종내종목'}, inplace=True)
    
    # 5. KRX업종별 설명 가져오기
    cols = ['세세분류_코드','설명']
    name_map = {'세세분류_코드':'업종코드_dart','설명':'업종설명'}
    industry_info = conn_db.from_("Master_산업,업종", "한국표준산업분류_세부")[cols].rename(columns=name_map)
    
    # 6.전체 join해서 하나로 만들어서 업로드
    df = df.merge(df_industry, on='KEY', how='left')
    df = df.merge(df_all, on='KEY', how='left')
    df = df.merge(co_by_industry, on='업종_krx', how='left')
    df = df.merge(industry_info, on='업종코드_dart',  how='left')
    
    # cols = ['KEY', '종목코드','종목명',  '시장',  '종목설명', '업종_krx', '업종설명', 'krx업종내종목', '업종코드기준',
    #         '대분류', '중분류', '소분류', '세분류', '세세분류', '업종_krx2', '업종_naver', 'FICS 업종',
    #         '테마명', '주요제품', '주요제품 (상세)', '실적요약', '실적내용', '사업환경',
    #         '경기변동',  '원재료', '실적변수', '재무리스크', '신규사업', '상장일', '결산월', '지역', '회사명' ]
    # df = df.sort_values(by=['KEY']).reset_index(drop=True)[cols]
    
    conn_db.to_(df, 'DB_기업정보','총괄')

    # 구글시트용_사용하는 것들 합쳐놓기
    co_info_master = conn_db.from_('DB_기업정보','총괄')
    naver_industry_per =  conn_db.from_('DB_기업정보','네이버업종PER')
    naver_finance = conn_db.from_('from_naver증권','naver_최근값만')

    # co_info_master에서 사용안하는 컬럼 삭제하고 naver_industry_per랑 합치기
    cols = ['업종코드_dart', '업종코드기준', '결산월', '지역','회사명',
            '대분류', '중분류', '소분류', '세분류', '세세분류', 'krx업종내종목']
    co_info_master.drop(columns=cols, inplace=True)
    df = co_info_master.merge(naver_industry_per,on='업종_naver')

    # naver_finance + df
    df = df.merge(naver_finance,on='KEY',how='left')
    conn_db.to_(df,'data_from_krx','기업정보+재무')

# KRX에서 업종분류 현황 가져오기
def get_co_industry_from_krx(update_date):
    '''
    [12025] 업종분류 현황
    '''
    url = 'http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd'
    header_info = {'User-Agent': user_agent}

    df = pd.DataFrame()
    for market in ['STK','KSQ']:
        params ={'bld':'dbms/MDC/STAT/standard/MDCSTAT03901',
                 'money':'1',
                 'mktId': market,
                 'csvxls_isNo':'false',
                 'trdDd': str(update_date)}
        r = requests.post(url, data=params, headers=header_info, timeout=5)
        df = df.append(pd.DataFrame(r.json()['block1']), ignore_index=True)

    df['날짜'] = str(update_date)
    df['날짜'] = pd.to_datetime(df['날짜'])

    names = {'ISU_SRT_CD':'종목코드',
             'ISU_ABBRV':'종목명',
             'MKT_TP_NM':'시장',
             'IDX_IND_NM':'업종'}
    return df[names.keys()].rename(columns=names)
