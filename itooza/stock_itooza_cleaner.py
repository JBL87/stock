import pandas as pd
import requests
from bs4 import BeautifulSoup
import concurrent.futures
import time
from glob import glob
import helper
import conn_db
# import pantab, platform

#----------------------------------------------------------------
dt = helper.now_time()
suffix = helper.get_time_suffix()
user_agent = helper.user_agent
max_workers = 3
code_list = conn_db.from_("DB_기업정보", 'FS_update_list')['종목코드']

#----------------------------------------------------------------
# 아이투자 기업정보 기업정보 가져올때 결과물 넣을 dataframes
raw_material_1_df = pd.DataFrame() # 원재료_가로형
raw_material_2_df = pd.DataFrame() # 원재료_세로형
product_1_df = pd.DataFrame() # 제품_가로형
product_2_df = pd.DataFrame()  # 제품_세로형
#----------------------------------------------------------------
# 아이투자 투자지표 가져올때 결과물 넣을 dataframes
df_short = pd.DataFrame() # 가장 최근값 테이블
df_5yr = pd.DataFrame() # 5년 평균 테이블
df_tables = pd.DataFrame() # 페이지 아래에 있는 전체 표(연환산,연간,분기)
#----------------------------------------------------------------
folder_itooza = conn_db.get_path('folder_itooza')
folder_itooza_backup = conn_db.get_path('folder_itooza_backup')


def merge_df_all_numbers(): # 아이투자, naver, fnguide 합쳐진 하나의 df만들기
    df_itooza = pd.read_pickle(folder_itooza + '장기투자지표_취합본.pkl')
    df_naver = pd.read_pickle(folder_naver + 'fs_from_naver_최근값만.pkl')
    df_fnguide_fsratio = pd.read_pickle(folder_fn + '2_fsratio_from_fnguide_최근값만.pkl')

    # 아이투자 듀퐁ROE 추가
    files = glob(folder_itooza_backup + '*_최근지표*.pkl')
    files.reverse()
    temp = pd.concat([pd.read_pickle(file) for file in files])
    temp = temp.drop_duplicates().reset_index(drop=True)
    temp = helper.make_keycode(temp).drop(columns=['종목코드','종목명'])
    temp['항목'] = temp['항목']+'_r'
    temp = temp.pivot_table(index='KEY',columns='항목', values='값').reset_index()
    temp.columns.name=None
    temp['ROE_r'] = temp['ROE_r']/100

    #--------------------------------------------------------------------
    df = df_itooza.merge(df_naver, on='KEY', how='inner')
    df = df.merge(df_fnguide_fsratio, on='KEY', how='inner')
    df = df.merge(temp, on='KEY', how='inner')
    df = df.merge(conn_db.from_('DB_기업정보','총괄') , on='KEY', how='inner')

    # 네이버업종PER 추가
    industry_per = conn_db.from_('DB_기업정보','네이버업종PER')
    # industry_per['업종PER'] = industry_per['업종PER'].astype('float')
    df = df.merge(industry_per, on='업종_naver', how='left')

    # 합친것 저장
    df.to_pickle(conn_db.get_path('장기투자지표_취합본+기업정보총괄')+'.pkl')
    conn_db.to_(df, 'Gfinance_시장data', 'import_장기투자지표_취합본+기업정보총괄')

    # vlookup이나 query문 작성할 때 편리하기 위해서 KEY 컬럼을 맨 앞으로 배치
    all_cols = df.columns.tolist()
    all_cols.remove('KEY')
    all_cols = ['KEY'] + all_cols

    loc = all_cols.index("종목코드") # 종목코드 앞에 있는 컬럼만 가져오기
    cols = all_cols[:loc]
    conn_db.to_(df[cols], '종목정리_ver1.0', 'import_fsdata')

# 페이지 아래에 있는 전체 표(연환산,연간,분기)
def clean_index_table(df):
    df = helper.clean_numeric_value(df)
    # 지표를 컬럼으로 행/열전환
    dcols = set(df.columns.tolist()) - set(['항목', '값'])
    df = df.pivot_table(index=dcols, columns='항목', values='값').reset_index()
    df.columns.name = None
    # 컬럼명 간단하게 수정 (뒤에 '(%)' 가 있는 경우 삭제하기 위함)
    df.columns = [x.split(' ')[0] for x in df.columns.tolist()]

    # 취합본과 합쳐놓기
    df_old = conn_db.from_('from_아이투자','아이투자_시계열_원본취합본')
    df = pd.concat([df, df_old], axis=0)
    cols = ['기준', '종목코드', '날짜']
    df = df.drop_duplicates(cols).reset_index(drop=True)

    # 새로 합쳐진 것 저장
    conn_db.to_(df,'from_아이투자','아이투자_시계열_원본취합본')

# 5개년 주요 투자지표와 최근 지표 요약본 정리
def clean_5yr_and_recent_index(df, table_name):
    cols = ['종목코드', '종목명']
    df = df.melt(id_vars=cols, var_name='항목', value_name='값').reset_index(drop=True)
    df.columns.name = None
    df = helper.clean_numeric_value(df.dropna())

    # 저장
    df.to_pickle(folder_itooza_backup + f'1_{table_name}_{suffix}.pkl') # 파일 백업
    conn_db.to_(df,'from_아이투자',f'{table_name}')

# 장기지표 평균치와 최근 지표 테이블 정리
@helper.timer
def clean_itooza_longterm_indexes():
    # 아이투자 시계열 지표 계산해서 합치기
    # 파일 백업중 가장 5개년 파일 불러오기
    files = glob(folder_itooza_backup + '1_장기투자지표_5개년_*.pkl')
    files.reverse()
    df_all = pd.concat([pd.read_pickle(data) for data in files])
    df_all = df_all.drop_duplicates(subset=['종목코드','종목명','항목']).reset_index(drop=True)
    df_all = df_all.pivot_table(index='종목코드', columns='항목', values='값').reset_index()
    df_all.columns.name=None

    #----------- ----------- ----------- ----------- ----------- ----------- ----------- ----------- ----------- -----------
    df_series = pd.read_pickle(folder_itooza_backup + "0_아이투자_시계열_원본취합본.pkl")

    # 1. 연간의 경우 분기값이 포함되어 있는 경우가 있기 때문에 삭제해 주어야함
    # 삭제 방법은 가장 마지막에 위치한 날짜의 월 부분이 결산월과 일치하면 keep, 아니면 제외
    df_not_year = df_series[(df_series['기준']!='연간')] # 연간이 아닌 것만 들어 있는 df
    df_year = df_series[df_series['기준']=='연간'] # 연간만 들어 있는 df
    df_year.sort_values(by=['날짜'], ascending=False, inplace=True)

    # 연간만 들어 있는 df에 대해서 아래 작업하고 나서 다시 연간이 아닌 것과 합치기
    # 날짜,기준,종목코드 key값 생성
    df_year['key'] = df_year['날짜'] + df_year['기준'] + df_year['종목코드']
    # 가장 최근값 2개만 선택
    cols = ['종목코드', '기준']
    df_year_temp = df_year.groupby(cols, as_index=False).head(2)

    # master에서 결산월 가져오기
    cols = ['종목코드','결산월']
    co_info_fiscal_year = conn_db.from_("DB_기업정보", '취합본')[cols]

    # 회사별로 가장 위에 있는 2개 날짜의 월부분이 결산월과 일치 하지 않으면 제외대상
    df_year_temp = df_year_temp.merge(co_info_fiscal_year, on='종목코드', how='inner')
    filt = df_year_temp['날짜'].str.split('년', expand=True)[1] != df_year_temp['결산월']

    exclude_list = df_year_temp[filt]['key'].tolist() # 제외대상의 key 값 list
    filt = df_year['key'].isin(exclude_list) # 년도만 있는 df의 key값이 제외대상에 있나없나 확인
    df_year = df_year.loc[~filt, : ] # 제외대상에 없는 것들만 선택
    df_year.drop(columns='key', inplace=True) # 작업하기 위해 임시로 만들었던 컬럼 삭제
    df_series = pd.concat([df_year, df_not_year]).reset_index(drop=True) # 다시 전체 df만들기

    # 날짜에 '월'만 있는 행이 생겨서 삭제
    filt = df_series['날짜'] != '월'
    df_series = df_series.loc[filt].copy()

    df_temp = df_series.copy()
    matcher = ['ROE', '률']
    all_cols = df_temp.columns.tolist()
    prcnt_cols = [col for col in all_cols if any(prcnt in col for prcnt in matcher)]
    for col in prcnt_cols:
        df_temp[col] = df_temp[col]/100

    names = {'시가': '시가배당률',
             '주당': '주당배당금',
             '주당순이익(EPS,개별)': 'EPS (개별)',
             '주당순이익(EPS,연결지배)': 'EPS (연결지배)',
             '주당순자산(지분법)' : 'BPS (지분법)'}
    df_temp.rename(columns=names, inplace=True)
    df_temp = helper.make_keycode(df_temp)  # KEY컬럼 만들기
    conn_db.to_(df_temp,'from_아이투자','아이투자_시계열_final')
    del exclude_list, filt, df_year_temp, co_info_fiscal_year, df_year, df_not_year, df_temp
    print('아이투자 시계열용 저장완료')

    '''
    시계열 파일에서 ['10년치평균값', '10분기연환산평균값', '최근연환산값', '최근연간값', '최근분기값']를 계산해야함
    그리고 5년치와 최근4분기치와 합쳐서 모든 지표가 들어가 있는 테이블을 만든다
    '''
    # 2. 10년치 평균값과 10분기 연환산 평균값 구하기
    use_cols = ['종목코드', '기준','날짜', 'PBR', 'PER', 'ROE', '순이익률',
                '영업이익률', '주당순이익(EPS,개별)', '주당순이익(EPS,연결지배)', '주당순자산(지분법)']

    df = df_series[use_cols] # 10년평균 구할수 있는 값만 선택
    
    # 연환산 10Q와 연간10Y 평균값 가져오기 (최대/최소값은 제외)
    term_types = ['연환산','연간']
    term_type_name = {'연환산':'_10Q',
                      '연간':'_10Y'}

    result_df = pd.DataFrame()
    for term_type in term_types:
        # ['연환산','연간'] 필터링
        temp_all = df.loc[df['기준']== term_type].sort_values(by='날짜', ascending=False).copy()
        # 4번부터가 투자지표 컬럼. 지표별로 돌면서 평균값구하기
        for col in temp_all.columns.tolist()[3:]:
            # 종목별로 돌면서 계산
            for co in temp_all['종목코드'].unique().tolist():
                # 단일종목, 단일지표 전체값 선택
                temp = temp_all.loc[temp_all['종목코드']==co, col].copy()
                temp_list = temp.tolist()
                new_col_name = col + term_type_name[term_type]
                try:
                    temp_list.remove(temp.max()) # 최대값제거
                    temp_list.remove(temp.min()) # 최소값제거
                    avg_10 = pd.Series(temp_list).mean() # 최대/최소 제거한 값의 평균
                     # 계산된 값의 명칭
                    result_df = result_df.append(pd.DataFrame([co, new_col_name, avg_10]).T)
                except: # 계산이 안될경우 공란으로 값 넣기
                    result_df = result_df.append(pd.DataFrame([co, new_col_name, None]).T)

    result_df.columns = ['종목코드','항목','값']
    result_df['값'] = pd.to_numeric(result_df['값'])
    result_df = result_df.pivot_table(index='종목코드', columns='항목', values = '값').reset_index()
    result_df.columns.name=None

    # 10년치 평균값과 10분기 연환산 평균값 구한 것 본래 있던 거에 합치기
    df_all = df_all.merge(result_df, on='종목코드', how='outer')
    del temp_all, temp, result_df
    print('아이투자 10년치 평균값과 10분기 연환산 평균값 구하는 작업완료')

    # 3. 연환산, 연간, 분기의 가장 최근에 있는 값만 가져오기
    # 필요한 컬럼만 필터링하고 날짜순으로 정렬
    use_cols = ['날짜', '기준','종목코드', 'PBR','PER','ROE', '순이익률', '영업이익률',
                '주당순이익(EPS,개별)', '주당순이익(EPS,연결지배)', '주당순자산(지분법)']
    df = df_series.loc[:, use_cols].sort_values(by='날짜', ascending=False)

    # 기준별로 가장 최근 날짜만 선택
    cols = ['종목코드', '기준']
    df = df.groupby(cols, as_index=False).head(1)
    cols = ['날짜','기준','종목코드']
    df = df.melt(id_vars=cols, value_name='값', var_name=['항목']) # tidy로 수정
    df = df.sort_values(by='날짜', ascending=False) # 날짜 순으로 정렬

    # 항목이름 뒤에 날짜기준 추가하기
    term_type_name = {'연환산':'_최근연환산',
                      '연간':'_최근Y',
                      '분기': '_최근Q'}
    df['항목'] = df['항목'] + [term_type_name[x] for x in df['기준']]

    # 필요없는 컬럼 삭제
    df.drop(columns=['날짜', '기준'], inplace=True)
    df = df.pivot_table(index='종목코드', columns= '항목', values='값').reset_index()
    df.columns.name= None

    # 연환산, 연간, 분기의 가장 최근에 있는 값과 본래 있던 거에 합치기
    df_all = df_all.merge(df, on='종목코드', how='outer').reset_index(drop=True)
    del df, df_series
    print('아이투자 연환산 / 연간 / 분기에서 가장 최근값만 가져오기 작업완료')

    matcher = ['ROE', '률']
    all_cols = df_all.columns.tolist()
    prcnt_cols = [col for col in all_cols if any(prcnt in col for prcnt in matcher)]
    for col in prcnt_cols:
        df_all[col] = df_all[col]/100

    col_name_change = {'주당순이익(EPS,개별)_최근Q': 'EPS_최근Q (개별)',
                        '주당순이익(EPS,개별)_최근Y': 'EPS_최근Y (개별)',
                        '주당순이익(EPS,개별)_최근연환산': 'EPS_최근연환산 (개별)',
                        '주당순이익(EPS,개별)_10Q': 'EPS_10Q (개별)',
                        '주당순이익(EPS,개별)_10Y': 'EPS_10Y (개별)',
                        '주당순이익(EPS,연결지배)_최근Q' : 'EPS_최근Q (연결지배)',
                        '주당순이익(EPS,연결지배)_최근Y': 'EPS_최근Y (연결지배)',
                        '주당순이익(EPS,연결지배)_최근연환산' : 'EPS_최근연환산 (연결지배)',
                        '주당순이익(EPS,연결지배)_10Q': 'EPS_10Q (연결지배)',
                        '주당순이익(EPS,연결지배)_10Y': 'EPS_10Y (연결지배)',
                        '주당순자산(지분법)_최근Q' : 'BPS_최근Q (지분법)',
                        '주당순자산(지분법)_최근Y' : 'BPS_최근Y (지분법)',
                        '주당순자산(지분법)_최근연환산' : 'BPS_최근연환산 (지분법)',
                        '주당순자산(지분법)_10Q' : 'BPS_10Q (지분법)',
                        '주당순자산(지분법)_10Y': 'BPS_10Y (지분법)',
                        '5년PER': 'PER_5Y',
                        '5년PBR': 'PBR_5Y',
                        '5년ROE': 'ROE_5Y',
                        '5년EPS성장률': 'EPS_5Y%',
                        '5년BPS성장률': 'BPS_5Y%'}
    for col in all_cols:
        if col in col_name_change:
            df_all.rename(columns={col: col_name_change[col]}, inplace=True)

    # DB_기업정보 FS_update_list에 있는 코드만 필터링
    global code_list
    df_all = df_all[df_all['종목코드'].isin(code_list)].copy()
    df_all = helper.make_keycode(df_all).drop(columns=['종목명', '종목코드'])

    df_all.to_pickle(folder_itooza + '장기투자지표_취합본.pkl')
    merge_df_all_numbers() # 전체 취합본 업데이트

def clean_itooza_company_description(df):
    df = helper.make_keycode(df)
    
    # 가장 최근 파일이 위로 가도록 순서 정렬해서 취합하고 과거 df랑 중복 되는거 삭제
    df['내용'] = df['내용'].apply(lambda x : x.replace('▷ ','\n▷ ' ).strip() if '▷ 'in x else x.replace('▷','\n▷ ' ).strip() if '▷' in x else x )
    old_df = conn_db.from_('DB_기업정보', 'from_아이투자_기업정보')

    # 1. 기업정보
    cols = ['구분', 'KEY']
    df = helper.add_df(df, old_df, cols)

    conn_db.to_(df, 'DB_기업정보', 'from_아이투자_기업정보')
    print('from_아이투자_기업정보에 업로드 완료')
    
    # 2. 제품, 원재료
    col_name_dict = {'주요제품':'제품명',
                    '원재료':'원재료'}
    for col_names in col_name_dict.keys():
        col_name = col_name_dict[col_names]
        df_temp = df[df['구분']==col_names].reset_index(drop=True)

        df_result = pd.DataFrame()

        # 가로형 만들기
        for x in df_temp['KEY'].unique().tolist():
            temp = df_temp.loc[df_temp['KEY']==x].copy()
            temp = temp['내용'].str.split('▷', expand=True).T
            temp.rename(columns={temp.columns.tolist()[0]:col_name}, inplace=True)
            temp['KEY'] = x
            df_result = df_result.append(temp)
        filt = df_result[col_name].apply(len)<2
        df_result = df_result.loc[~filt].reset_index(drop=True)
        df_result[col_name] = df_result[col_name].str.strip()

        # 세로형 만들기
        filt = df_result[col_name].str.contains(':')
        df_result1 = df_result[filt] # ':' 있는거
        df_result2 = df_result[~filt] # ':' 없는거

        df_result_1_1 = pd.DataFrame()
        for x in df_result1['KEY'].unique().tolist():
            temp = df_result1.loc[df_result1['KEY']==x].copy()
            temp = temp[col_name].str.split(':',1, expand=True)
            temp.rename(columns={temp.columns.tolist()[0]:col_name}, inplace=True)
            temp['KEY'] = x
            df_result_1_1 = df_result_1_1.append(temp)
        filt = df_result_1_1[col_name].apply(len)<2
        df_result_1_1 = df_result_1_1.loc[~filt].copy()
        df_result_1_1.rename(columns={1:'내용'}, inplace=True)

        df_result_2_1 = pd.DataFrame()
        for x in df_result2['KEY'].unique().tolist():
            temp = df_result2.loc[df_result2['KEY']==x].copy()
            temp = temp[col_name].str.split('(', 1, expand=True)
            temp.rename(columns={temp.columns.tolist()[0]:col_name}, inplace=True)
            temp['KEY'] = x
            df_result_2_1 = df_result_2_1.append(temp)
        filt = df_result_2_1[col_name].apply(len)<2
        df_result_2_1 = df_result_2_1.loc[~filt].copy()
        df_result_2_1.rename(columns={1:'내용'}, inplace=True)

        df_result_long = pd.concat([df_result_1_1, df_result_2_1]).reset_index(drop=True)
        df_result_long[col_name] = df_result_long[col_name].str.strip()
        df_result_long['내용'] = df_result_long['내용'].str.strip()

        cols = ['KEY','종목코드','기준날짜','종목명']
        df_temp = df_temp[cols].copy()

        df_result = df_temp.merge(df_result, on='KEY', how='right')
        df_result = df_result.sort_values(by=cols).reset_index(drop=True)
        conn_db.to_(df_result, 'DB_기업정보', f'{col_name}_가로형')

        df_result_long = df_temp.merge(df_result_long, on='KEY', how='right')
        df_result_long = df_result_long.sort_values(by=cols).reset_index(drop=True)
        conn_db.to_(df_result_long, 'DB_기업정보', f'{col_name}_세로형')

    # # 원재료_가로형------ ------- ------- ------- ------- ------- ------- -------
    # raw_material_1_df = helper.make_keycode(raw_material_1_df.reset_index(drop=True))
    # conn_db.to_(raw_material_1_df, 'DB_기업정보', '원재료_가로형')

    # # 원재료_세로형------ ------- ------- ------- ------- ------- ------- -------
    # raw_material_2_df = helper.make_keycode(raw_material_2_df.reset_index(drop=True))
    # conn_db.to_(raw_material_2_df, 'DB_기업정보', '원재료_세로형')

    # # 제품_가로형------ ------- ------- ------- ------- ------- ------- -------
    # product_1_df = helper.make_keycode(product_1_df.reset_index(drop=True))
    # conn_db.to_(product_1_df, 'DB_기업정보', '제품_가로형')

    # # 제품_세로형------ ------- ------- ------- ------- ------- ------- -------
    # product_2_df = helper.make_keycode(product_2_df.reset_index(drop=True))
    # conn_db.to_(product_2_df, 'DB_기업정보', '제품_세로형')
    #------- ------- -------
    # , raw_material_1_df, raw_material_2_df, product_1_df, product_2_df