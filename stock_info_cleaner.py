import pandas as pd
from bs4 import BeautifulSoup
import requests, time
import conn_db
import helper

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

# 다른 출처에서 가져온 기업정보 전체 취합
def merge_co_info():
    '''
    다른 출처에서 가져온 기업정보 전체 취합
    '''
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
    cols = ['KEY', '회사코드', '회사명', '업종코드기준','업종코드']
    co_dart = conn_db.from_('DB_기업정보', 'from_dart')[cols]

    # 하나로 합치기
    list_of_dfs = [naver_industry, naver_theme, co_dart]
    for dfs in list_of_dfs:
        co_info = co_info.merge(dfs, on='KEY', how='left')

    #3번 파일 정리
    co_info['회사명'] = co_info['회사명'].fillna(co_info['종목명']) # 회사명이 null이면 종목명으로 채우기
    names = {'업종코드_x': '업종코드_krx',
            '업종코드_y': '업종코드_dart'}
    co_info.rename(columns=names, inplace=True)

    # 오류수정 ---------------------------------------------------------
    # co_info.loc[co_info['종목명']=='바른손','결산월'] = '03월'

    exclude_list = conn_db.from_('DB_기업정보', '제외할list')['종목코드'].drop_duplicates()
    filt = co_info['종목코드'].isin(exclude_list)
    co_info = co_info.loc[~filt].copy()

    # 코넥스 제외
    filt = co_info['시장'] !='코넥스'
    co_info = co_info.loc[filt].copy()

    # 컬럼순서 정리하고 fnguide에서 가져온 내용 합친 다음 업로드
    cols = ['종목코드','종목명']
    df_fnguide = conn_db.from_('DB_기업정보','from_fnguide_기업정보').drop(columns=cols)
    names = {'내용':'실적내용',
            '요약':'실적요약',
            'FICS':'FICS 업종',
            'KRX':'업종_krx2'}
    df_fnguide.rename(columns=names, inplace=True)
    # 컬럼순서
    cols = [ 'KEY', '종목코드', '종목명', '시장', '업종_krx', '업종_krx2','업종_naver',
            'FICS 업종', '테마명', '주요제품', '실적요약', '실적내용', '기준날짜',
            '업종코드_dart', '업종코드기준', '업종코드_krx', '상장일', '결산월', '지역',
            '상장주식수(주)', '자본금(원)','액면가(원)', '통화구분', '회사코드', '회사명']
    co_info = co_info.merge(df_fnguide, on='KEY', how='left')[cols]
    conn_db.to_(co_info, 'DB_기업정보', '취합본')

# 산업별로 기업 분류하기
def classify_co_by_industry():
    '''
    산업별로 기업 분류하기
    '''
    co_info = conn_db.from_('DB_기업정보', '취합본').drop(columns=cols)

    # 업종별 회사를 산업/업종 master에 업로드. 업종별 광공업지수와 비교분석하기 위함
    df = co_info.groupby(['업종_krx'])['종목명'].apply(', '.join).reset_index()
    conn_db.to_(df, 'Master_산업,업종', '업종별회사')

    # 한국표준산업별로 종목 구분하기
    use_cols = ['KEY', '종목명', '업종_krx', '업종_krx2', '업종_naver',
                'FICS 업종', '테마명', '업종코드_dart', '업종코드기준']
    co_info = co_info[use_cols]
    df_industry = conn_db.from_('Master_산업,업종','한국표준산업분류')

    def _clean_df(string):
        temp = co_info.loc[co_info['업종코드기준'] == string].copy()
        return temp.merge(df_industry, left_on='업종코드_dart', right_on=f'{string}_코드', how='left')

    df_1 = _clean_df('소분류')
    df_2 = _clean_df('세분류')
    df_3 = _clean_df('세세분류')

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

# 기업정보 취합본 + 한국표준산업분류별_종목 + 업종별 설명 
# + 개별테마 + 아이투자_기업정보까지 모두 다 합친 것
def make_master_co_info():
    # 1.전체 취합본 불러오기
    cols = ['업종코드_krx', '통화구분', '액면가(원)',
            '상장주식수(주)', '자본금(원)', '회사코드', '기준날짜']
    df = conn_db.from_('DB_기업정보', '취합본').drop(columns=cols)

    # 2.아이투자_기업정보 불러와서 구분별로 컬럼 만들기
    cols = ['KEY', '구분', '내용']
    df_description = conn_db.from_('DB_기업정보', 'from_아이투자_기업정보')[cols]

    df_all = pd.DataFrame()
    for x in df_description['구분'].unique():
        temp = df_description[df_description['구분'] == x].rename(
            columns={'내용': x}).drop(columns='구분')
        if len(df_all) == 0:
            df_all = df_all.append(temp)
        else:
            df_all = df_all.merge(temp, on='KEY')
    names = {'주요제품': '주요제품 (상세)'}
    df_all = df_all.rename(
        columns=names).drop_duplicates().reset_index(drop=True)

    # 3.한국표준산업분류별_종목 불러오기
    cols = ['KEY', '대분류', '중분류', '소분류', '세분류', '세세분류']
    df_industry = conn_db.from_('Master_산업,업종', '한국표준산업분류별_종목')[cols]

    # 4. KRX업종별 종목 가져오기
    co_by_industry = conn_db.from_('Master_산업,업종', '업종별회사')
    co_by_industry.rename(columns={'종목명': 'krx업종내종목'}, inplace=True)

    # 5. KRX업종별 설명 가져오기
    cols = ['세세분류_코드', '설명']
    name_map = {'세세분류_코드': '업종코드_dart', '설명': '업종설명'}
    industry_info = conn_db.from_("Master_산업,업종", "한국표준산업분류_세부")[
        cols].rename(columns=name_map)

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
    conn_db.to_(df, 'DB_기업정보', '총괄')

def co_info_to_dash():
    # 구글시트용_사용하는 것들 합쳐놓기
    co_info_master = conn_db.from_('DB_기업정보', '총괄')
    naver_industry_per = conn_db.from_('DB_기업정보', '네이버업종PER')
    naver_finance = conn_db.from_('from_naver증권', 'naver_최근값만')

    # co_info_master에서 사용안하는 컬럼 삭제하고 naver_industry_per랑 합치기
    cols = ['업종코드_dart', '업종코드기준', '결산월', '지역', '회사명',
            '대분류', '중분류', '소분류', '세분류', '세세분류', 'krx업종내종목']
    co_info_master.drop(columns=cols, inplace=True)
    df = co_info_master.merge(naver_industry_per, on='업종_naver')

    # naver_finance + df
    df = df.merge(naver_finance, on='KEY', how='left')
    conn_db.to_(df, 'data_from_krx', '기업정보+재무')
