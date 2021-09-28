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
# fsratio_class = conn_db.from_('from_fnguide_항목', '재무비율_항목정리').drop_duplicates()
# invest_class = conn_db.from_('from_fnguide_항목', '투자지표_항목정리').drop_duplicates()
#----------------------------------------------------------------
# fnguide 기업정보 가져올때 결과물 넣을 dataframes
company_info = pd.DataFrame() # 업종, business summary
financial_highlights = pd.DataFrame() # 재무제표
sales_mix = pd.DataFrame() # 제품별 매출비중 가장 최근
market_share = pd.DataFrame() # 시장점유율 가장 최근
cogs_n_oc = pd.DataFrame() # 판관비율추이, 매출원가율추이
export_n_domestic = pd.DataFrame() # 수출 및 내수 구성
#----------------------------------------------------------------
folder_fn = conn_db.get_path('folder_fn')
folder_fn_backup = conn_db.get_path('folder_fn_backup')

def clean_numeric_value(df): # ['값'] 컬럼을 문자열에서 숫자로 수정
    try:
        filt1 = df['날짜'].notna() # 날짜 컬럼 확인. 날짜컬럼 없으면 except문으로 이동
        filt2 = df['값']!= '-' # 금액에 null 대신 '-'로 되어 있는거 제거
        filt3 = df['값'].apply(len) > 0  # 길이가 0이 안되면 삭제
        filt = filt1 & filt2 & filt3
    except:
        filt1 = df['값']!= '-' # 금액에 null 대신 '-'로 되어 있는거 제거
        filt2 = df['값'].apply(len) > 0  # 길이가 0이 안되면 삭제
        filt = filt1 & filt2
    df = df.loc[filt, :].copy()
    df.reset_index(drop=True, inplace=True)
    # df['값'] = df['값'].str.replace(',', '')
    try:
        df['값'] = df['값'].str.replace(',', '')
        df['값'] = df['값'].str.replace('%', '')
    except:
        pass
    df['값'] = pd.to_numeric(df['값'], errors='coerce')
    df.dropna(subset=['값'], inplace=True)
    return df.reset_index(drop=True)

def drop_duplicate_rows(df, old_df, check_cols):
    df = pd.concat([df, old_df], axis=0)
    # check_cols 기준으로 중복제거 후 return
    return df.drop_duplicates(check_cols).reset_index(drop=True)

def merge_df_all_numbers(): # 아이투자, naver, fnguide 합쳐진 하나의 df만들기
    df_itooza = pd.read_pickle(folder_itooza + '장기투자지표_취합본.pkl')
    df_naver = pd.read_pickle(folder_naver + 'fs_from_naver_최근값만.pkl')
    df_fnguide_fsratio = pd.read_pickle(folder_fn + '2_fsratio_from_fnguide_최근값만.pkl')

    #----------------------------------------------------------------
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

    #--------------------------------------------------------------------
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
    
#--------------------------------------------------------------------------------------------------------------------
#FN GUIDE 재무제표
def _get_fs_from_fnguide(dom, tp, fstype):  # fnguide 재무제표 가져오기
    # fstypes = ['divSonikY','divSonikQ','divDaechaY','divDaechaQ','divCashY','divCashQ']
    fstypes_name = {'divSonikY': '연간손익계산서',
                    'divSonikQ': '분기손익계산서',
                    'divDaechaY': '연간재무상태표',
                    'divDaechaQ': '분기재무상태표',
                    'divCashY': '연간현금흐름표',
                    'divCashQ': '분기현금흐름표'}
    report_name = {'B':'별도',
                    'D':'연결'}
    time.sleep(1)
    try:
        datas = dom.select(f'#{fstype} > table > tbody > tr') # 계정별로 data들어 있는 것
        header_data = dom.select(f'#{fstype} > table > thead > tr > th')  # 컬럼 header
        header_data = [data.text for data in header_data]

        if '전년동기' in header_data:
            loc = int(header_data.index('전년동기')) - 1  # 컬럼에서 전년동기 컬럼의 이전컬럼 위치
            yyyy = str(int(header_data[loc][:4]) - 1)  # 년도부분 가져오기
            mm = header_data[loc][-2:]  # 월부분 가져오기
            yyyymm = yyyy + '/' + mm  # 연월 날짜 만들어주기
            header_data[loc+1] = yyyymm  # '전년동기'를 날짜값으로 변경
        else:
            pass
        
        df = pd.DataFrame(header_data)  # 컬럼만 들어가 있는 df
        for i in range(len(datas)-1):
            if i == 0:
                # 대분류 계정명 , data[0] 일때만 해당됨
                account_nm = [datas[0].select('div')[0].text]
                # 대분류 계정의 컬럼별 값
                data = [data.text for data in datas[0].select('td')]
            else:
                # 소분류 계정명 , data[0] 이상 일때만 해당됨
                account_nm = [datas[i].select('th')[0].text.strip()]
                # 소분류 계정의 컬럼별 값
                data = [data.text for data in datas[i].select('td')]
            row = pd.DataFrame(account_nm + data)  # 계정별로 행 전체
            df = pd.concat([df, row], axis=1)
        
        df = df.T
        df.columns = df.iloc[0]
        df = df.iloc[1:]
        
        first_col = df.columns.tolist()[0]
        df['재무제표기준'] = first_col  # 재무제표기준 컬럼으로 추가
        df.rename(columns={first_col: '항목'}, inplace=True)  # 첫 컬럼명을 '항목'으로 수정
        df = df.reset_index(drop=True).reset_index().rename(columns={'index': '항목순서'}) # 항목순서 컬럼 만들기 위해 2번 reset_index
        
        if '전년동기(%)' in df.columns.tolist():
            df.drop(columns='전년동기(%)', inplace=True)
        else:
            pass
        
        df = df.melt(id_vars=['재무제표기준', '항목', '항목순서'], var_name='날짜', value_name='값')
        df['재무제표기준'] = fstypes_name[fstype]
        df['연결/별도'] = report_name[tp]
        return df
    except:
        pass

def _get_all_fs_from_fnguide(code):  # fnguide 재무제표 가져오기
    '''
    fnguide에서 재무제표 가져오기
    '''
    fstypes = ['divSonikY', 'divSonikQ', 'divDaechaY', 
                'divDaechaQ', 'divCashY', 'divCashQ']
    report_types = ['B', 'D']
    # report_name = {'B': '별도', 'D': '연결'}
    df_all = pd.DataFrame()  # 연결/분기 재무제표 넣을 df

    # 연결/별도 기준별로 data 가져오기
    for report_type in report_types:
        url = f"http://comp.fnguide.com/SVO2/asp/SVD_Finance.asp?pGB=1&gicode=A{code}&cID=&MenuYn=Y&ReportGB={report_type}&NewMenuID=103&stkGb=701"
        r = requests.get(url, headers={'User-Agent': user_agent})
        dom = BeautifulSoup(r.text, "html.parser")
        try:
            df = pd.concat([_get_fs_from_fnguide(dom, report_type, fstype) for fstype in fstypes])
            df_all = df_all.append(df)
        except:
            pass
    try:
        df_all['항목'] = df_all['항목'].str.replace('계산에 참여한 계정 펼치기', '')
        df_all['주기'] = df_all['재무제표기준'].str[:2]
        df_all['종목코드'] = code
        df_all['재무제표종류'] = df_all['재무제표기준'].str[2:]
        df_all.drop(columns='재무제표기준', inplace=True)
        print(f'{code} 완료')
        return df_all.drop_duplicates().reset_index(drop=True)
    except:
        print(f'{code} 데이터가져오기 실패')

@helper.timer
def update_fnguide_fs(param='all'):  # fnguide 재무제표 업데이트
    global code_list
    global max_workers
    file = conn_db.get_path('fs_from_fnguide_raw')+".pkl"
    df = pd.DataFrame()
    if param=='all':
        for code in code_list:
            try:
                df = df.append(_get_all_fs_from_fnguide(code), ignore_index=True)
            except:
                pass
        # with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        #     result = executor.map(get_all_fs_from_fnguide, code_list)
        # df = pd.concat([df for df in result], axis=0)
    else:
        df = pd.read_pickle(file)
        new_code = list(set(df['종목코드'].unique()) - set(conn_db.from_('DB_기업정보','취합본')['종목코드']))
        df = pd.concat([_get_all_fs_from_fnguide(code) for code in new_code], axis=0)
        
    df = clean_numeric_value(df)
    df = helper.make_keycode(df)  # KEY 컬럼 추가

    # 취합본 업데이트 후 저장
    old_df = pd.read_pickle(file)
    cols = ['항목','항목순서','날짜','연결/별도','주기','종목코드','종목명','KEY','재무제표종류']
    df = drop_duplicate_rows(df, old_df, cols)
    df = df[df['종목코드'].isin(code_list)].copy()
    
    # 새로 합쳐진것 저장
    df.to_pickle(file)
    merge_df_all_numbers()

#FN GUIDE 재무비율
def get_fsratio_from_fnguide(code):  # fnguide 재무비율 가져오기
    '''
    fnguide 재무비율 가져오기
    '''
    time.sleep(1)
    report_types = ['B', 'D'] # {'B': '별도', 'D': '연결'}
    df_all = pd.DataFrame()  # 전체 재무제표 넣을 df
    # 연결/별도 기준별로 data 가져오기
    for report_type in report_types:
        url = f"http://comp.fnguide.com/SVO2/asp/SVD_FinanceRatio.asp?pGB=1&gicode=A{code}&cID=&MenuYn=Y&ReportGB={report_type}&NewMenuID=104&stkGb=701"
        r = requests.get(url, headers={'User-Agent': user_agent})
        dom = BeautifulSoup(r.text, "html.parser")
        # 컬럼정보와 데이터가 들어가 있는 변수 생성
        header_info = dom.select('#compBody > div.section.ul_de > div > div.um_table > table > thead')
        all_data = dom.select('#compBody > div.section.ul_de > div > div.um_table > table')
        # 데이터 변수 안에 있는 거에서 df로 정리하기
        temp = pd.DataFrame()
        for report in range(2): # 0은 연간, 1은 분기 데이터를 가져옴
            try:
                datas = all_data[report]  # 데이터가 들어 있는 곳
                date_cols = header_info[report]  # 컬럼 header가 들어 있는 곳
                date_cols = [data.text.strip() for data in date_cols.select('th')]  # 날짜 컬럼 header
                account_nm = [x.text.strip() for x in datas.select('tr > th > div')]  # 계정명칭
                df = pd.DataFrame(account_nm)
                data_values = [x.text.strip() for x in datas.select('td')] # 값만 들어가 있는 컬럼
                lap = len(date_cols)-1
                for i in range(lap):
                    temp_df = pd.DataFrame(data_values[i::lap]) # 컬럼의 갯수(lap)만큼 매n번째마다 합쳐줌
                    df = pd.concat([df, temp_df], axis=1)
                df.columns = date_cols
                first_col = df.columns.tolist()[0]
                df.rename(columns={first_col: '항목'}, inplace=True)  # 첫 컬럼명을 '항목'으로 수정
                df['항목'] = df['항목'].str.replace('계산에 참여한 계정 펼치기', '')
                # 항목 명칭 정리된거랑 merge
                # acc_class = fsratio_class
                # df = df.merge(acc_class, on='항목',how='left').drop_duplicates()
                df = df.melt(id_vars='항목', var_name='날짜', value_name='값')
                df = df.dropna().drop_duplicates().reset_index(drop=True)
                df['재무제표기준'] = first_col  # 재무제표기준 컬럼으로 추가
                df['주기'] = '연간' if report == 0 else '분기' # 연간/분기 컬럼 추가
                temp = temp.append(df) # 연간 / 분기 df 추가
            except:
                print(f'{code} {report} {report_type} 데이터가져오기 실패')
                pass
        if len(temp)>1:
            df_all = df_all.append(temp, ignore_index=True) # 연결 / 별도 df 추가
        else:
            # print(f'{code} {report_type} 데이터가져오기 실패')
            pass
    try:
        df_all = df_all
        df_all['종목코드'] = code
        df_all = df_all[['종목코드', '재무제표기준', '주기', '항목', '날짜', '값']]
        print(f'{code} 데이터가져오기 완료')
        return df_all.drop_duplicates().reset_index(drop=True)
    except:
        pass
    #--------------------------------------------------------------------------------------------------------------------

@helper.timer
def update_fnguide_fsratio(param='all'):  # fnguide 재무비율 업데이트
    global code_list
    global max_workers
    file = conn_db.get_path('fsratio_from_fnguide_raw')+'.pkl'
    if param=='all':
        df = pd.concat([get_fsratio_from_fnguide(code) for code in code_list], axis=0)
        # with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        #     result = executor.map(get_fsratio_from_fnguide, code_list)
        # df = pd.concat([df for df in result], axis=0)
        # del df
    else:
        df = pd.DataFrame()
        new_code = conn_db.from_('DB_기업정보', '취합본')['종목코드']
        new_code = list(set(df['종목코드'].unique()) - set(new_code))
        try:
            df = pd.concat([get_fsratio_from_fnguide(code) for code in new_code], axis=0)
            del new_code
        except:
            print('업데이트할 내역 없음')
    if len(df)>0:
        df = clean_numeric_value(df) # 값 컬럼 정리
        # 취합본 불러와서 합치기
        old_df = pd.read_pickle(file)
        df['항목'] = df['항목'].apply(lambda x : x.split(u'\xa0')[-1].strip() if '\xa0' in x else x.strip())
        cols = ['항목','날짜','재무제표기준','주기','종목코드']
        df = drop_duplicate_rows(df, old_df, cols)
        df.to_pickle(file) 
        del df, old_df, file
    else:
        print('업데이트할 내역 없음')
    #----------- ----------- ----------- ----------- ----------- ----------- ----------- -----------
def clean_fsratio_from_fnguide(): # fnguide 재무비율 전처리
    df = pd.read_pickle(conn_db.get_path('fsratio_from_fnguide_raw')+'.pkl')
    # KEY컬럼 만들기
    df = helper.make_keycode(df)
    #------------------------------------------------------------------------------------

    maper = {'IFRS(연결)':'연결', 'GAAP(연결)':'연결',
            'IFRS(개별)':'개별', 'GAAP(개별)':'개별', 'IFRS(별도)':'개별'}
    df['연결/별도'] = df['재무제표기준'].map(maper)
    df.drop(columns='재무제표기준', inplace=True)
    df['temp_key'] = df['KEY']+df['연결/별도'] # 일시 key컬럼
    #------------------------------------------------------------------------------------

    #주재무제표만 남기기 위해서 naver에서 가져온 종목별 주재무제표 df와 inner join
    temp_fs = conn_db.from_('DB_기업정보','종목별_주재무제표')
    maper = {'IFRS연결':'연결', 'GAAP연결':'연결',
            'IFRS개별':'개별', 'GAAP개별':'개별', 'IFRS별도':'개별'}
    temp_fs['연결/별도'] = temp_fs['재무제표기준'].map(maper)
    temp_fs['temp_key'] = temp_fs['KEY']+temp_fs['연결/별도']
    temp_fs.drop(columns=['재무제표기준','KEY','연결/별도'], inplace=True)
    df = df.merge(temp_fs, on='temp_key', how='inner').drop(columns=['temp_key'])
    del temp_fs
    #------------------------------------------------------------------------------------

    # DB_기업정보 FS_update_list에 있는 코드만 필터링
    filt = (df['값']!=0.0) & (df['종목코드'].isin(code_list))
    df = df[filt].copy()
    df = df.sort_values(by='날짜', ascending=False)
    df = df.dropna(axis=1, how='all') # 전체가 null인 경우는 삭제
    #--------------------------------------------------------------------------

    # 항목에 '(-1Y)'가 있으면 전년도 값임. 날짜부분을 실제 -1Y에 해당하는 날짜로 변경
    filt = df['항목'].str.contains('-1Y')
    df_all = df.loc[~filt, :].copy() # 전년도 값이 아닌, 본래 년도의 값만 들어 있는 df
    df_all.drop_duplicates(inplace=True)

    df_temp = df.loc[filt, :].copy() # 전년도 값만 들어 있는 df
    year_part = df_temp['날짜'].str[:4].tolist()
    year_part = [str(int(x)-1) for x in year_part] # '전년도' 구하기
    quarter_part = df_temp['날짜'].str[-3:].tolist() # '월부분' 발라내기
    df_temp['날짜'] = [x+y for x, y in zip(year_part, quarter_part)] # 새로 날짜 만들어 주기
    df_temp['항목'] = df_temp['항목'].str.replace('\(-1Y\)', '').str.strip()

    df = pd.concat([df_all, df_temp]).drop_duplicates()
    del df_temp, df_all
    #--------------------------------------------------------------------------

    df.drop(columns=['종목코드','종목명'], inplace=True)
    # 항목을 컬럼으로 옮기기
    dcols = ['KEY', '연결/별도', '주기', '날짜']
    df = df.pivot_table(index=dcols, columns='항목', values='값').reset_index()
    df.columns.name = None
    # 비율인 컬럼은 100으로 나누어 주어야 함. *100이 된 상태로 들어가 있음
    matcher = ['RO', '률', '율']
    all_cols = df.columns.tolist()
    prcnt_cols = [col for col in all_cols if any(prcnt in col for prcnt in matcher)]
    for col in prcnt_cols:
        df[col] = df[col]/100
    # 연율화는 나누기 100을 하면 안되기 때문에 다시 곱하기 100
    matcher = ['연율화']
    all_cols = df.columns.tolist()
    prcnt_cols = [col for col in all_cols if any(prcnt in col for prcnt in matcher)]
    for col in prcnt_cols:
        df[col] = df[col]*100
    #--------------------------------------------------------------------------

    df.to_pickle(folder_fn + "2_fsratio_from_fnguide_시계열.pkl")
    print('fnguide 재무비율 시계열용 pickle 저장완료')
    #-------------------------------- -------------------------------- --------------------------------

    # 분기/연간에서 가장 최근값만 있는 df만들기
    df = df.melt(id_vars=dcols, var_name='항목', value_name='값').dropna()
    df = df.sort_values(by=dcols, ascending=False).reset_index(drop=True)
    # df = df.groupby(['KEY','주기','연결/별도','항목'], as_index=False).head(1)
    # --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
    def filter_date(df):  # 회사별로 가장 최근 날짜에 해당하는 것만 남기기
        df_all = pd.DataFrame()
        for key in df['KEY'].unique().tolist():
            temp_df = df.loc[df['KEY'] == key]
            if temp_df['주기'].unique() == '분기':
                # 분기인 경우 0번째는 최근분기, 4번째는 전년분기
                date = temp_df['날짜'].unique().tolist()[0]
                filt = temp_df['날짜'] == date
                df_all = df_all.append(temp_df.loc[filt])
                try: # 신규상장된 경우 예전 값이 없을 수도 있음
                    date = temp_df['날짜'].unique().tolist()[4]
                    filt = temp_df['날짜'] == date
                    temp = temp_df.loc[filt].copy()
                    temp['주기'] = '전년분기'
                    df_all = df_all.append(temp)
                except:
                    pass
            #-----------------------------------------------------
            else:  # 연간인 경우 0번째는 분기누적, 1번째는 연간
                date = temp_df['날짜'].unique().tolist()[0]
                filt = temp_df['날짜'] == date            
                df_all = df_all.append(temp_df.loc[filt])
                try: # 신규상장된 경우 예전 값이 없을 수도 있음
                    date = temp_df['날짜'].unique().tolist()[1]
                    filt = temp_df['날짜'] == date
                    temp = temp_df.loc[filt].copy()
                    temp['주기'] = '분기누적'
                    df_all = df_all.append(temp_df.loc[filt])
                except:
                    pass
        return df_all.reset_index(drop=True)
    # --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
    filt = df['주기']=='분기'
    df_q = df[filt]
    df_y = df[~filt]
    # 연간 + 분기 합치기
    df = pd.concat([filter_date(df_q), filter_date(df_y)])
    del df_q, df_y
    df['항목'] = df['항목'] + " _" +  df['주기']
    df = df.drop(columns=['날짜','주기']).reset_index(drop=True)
    # 행/열 전환하고 저장하기
    df = df.pivot_table(index=['KEY', '연결/별도'], columns='항목', values='값').reset_index()
    df.columns.name = None
    #--------- --------- --------- --------- --------- --------- --------- --------- --------- --------- ---------
    # df.to_pickle(folder_fn + '2_fsratio_from_fnguide_최근값만.pkl')
    conn_db.to_(df, 'from_fnguide', 'fnguide_fsratio_최근값만')
    del df
    print('fnguide 재무비율 최근값만 업로드 완료')
    merge_df_all_numbers() # 전체 취합본 업데이트
    #--------------------------------------------------------------------------------------------------------------------
#FN GUIDE 투자지표
def get_invest_ratio_from_fnguide(code): # fnguide 투자지표 가져오기
    '''
    fnguide 투자지표 가져오기
    '''
    time.sleep(1)
    report_types = ['B', 'D']
    df_all = pd.DataFrame()  # 전체 재무제표 넣을 df
    for report_type in report_types:
        url = f'http://comp.fnguide.com/SVO2/ASP/SVD_Invest.asp?pGB=2&gicode=A{code}&cID=&MenuYn=Y&ReportGB={report_type}&NewMenuID=105&stkGb=701'
        r = requests.get(url, headers={'User-Agent': user_agent})
        try:
            dom = BeautifulSoup(r.text, "html.parser")
            all_datas = dom.select('#compBody > div.section.ul_de > div.ul_col2wrap.pd_t25 > div.um_table > table')[0]
            date_cols = [x.text.strip() for x in all_datas.select('tr')[0].select('th')]
            data_values = [x.text.strip() for x in all_datas.select('tr > td')]
            account_cols = []
            lap = len(all_datas.select('div'))
            for i in range(lap):
                try:
                    acc = all_datas.select('div')[i].select('dt')[0].text.strip()
                    account_cols.append(acc)
                except:
                    try:
                        account_cols.append(all_datas.select('div')[i].text.strip())
                    except:
                        pass
            df = pd.DataFrame(account_cols)
            lap = len(date_cols)-1
            for i in range(lap): # 컬럼의 갯수가 5개이기 때문에 매5번째마다 합쳐줌
                temp_df = pd.DataFrame(data_values[i::lap])
                df = pd.concat([df, temp_df], axis=1)
            df.columns = date_cols
            first_col = df.columns.tolist()[0]
            df.rename(columns={first_col: '항목'}, inplace=True)  # 첫 컬럼명을 '항목'으로 수정
            # acc_class = invest_class
            # df = df.merge(acc_class, on='항목', how='left').drop_duplicates()
            df = df.melt(id_vars='항목', var_name='날짜', value_name='값')
            df = df.dropna().drop_duplicates().reset_index(drop=True)
            df['재무제표기준'] = first_col  # 재무제표기준 컬럼으로 추가
            df_all = df_all.append(df)
        except:
            # print(f'{code} {report_type} 데이터가져오기 실패')
            pass
    try:
        df_all['종목코드'] = code
        return df_all.drop_duplicates().reset_index(drop=True)
    except:
        print(f'{code} 데이터가져오기 실패')
    #--------------------------------------------------------------------------------------------------------------------

@helper.timer
def update_fnguide_invest_ratio(param='all'):  # fnguide 투자지표 업데이트
    global code_list
    global max_workers
    start_time = helper.now_time()
    print('fnguide 투자비율 가져오기 시작 ' + start_time.strftime('%Y-%m-%d %H:%M:%S'))
    file = folder_fn_backup + "invest_ratio_from_fnguide_받은원본취합본.pkl"
    # file = conn_db.from_('from_fnguide','fnguide_invest_ratio_원본취합본')
    if param=='all':
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            result = executor.map(get_invest_ratio_from_fnguide, code_list)
            df = pd.concat([df for df in result], axis=0)
    else:
        df = pd.read_pickle(file)
        new_code = list(set(df['종목코드'].unique()) - set(conn_db.from_('DB_기업정보','취합본')['종목코드']))
        df = pd.concat([get_fsratio_from_fnguide(code) for code in new_code], axis=0)
    # 전처리 시작
    if len(df)>0:
        df = clean_numeric_value(df)
        # ------- ------- ------- ------- ------- ------- ------- -------
        # 백업 불러와서 취합본 업데이트
        old_df = pd.read_pickle(file)
        cols = ['항목', '날짜', '재무제표기준', '종목코드']
        df = drop_duplicate_rows(df, old_df,cols)
        df.to_pickle(file)
        # ------- ------- ------- ------- ------- ------- ------- -------
        df['항목'] = df['항목'].str.replace('\(', ' (').str.strip()
        df = df.pivot_table(index=['날짜','종목코드','재무제표기준'], columns='항목', values='값').reset_index()
        df.columns.name = None
        df['배당성향 (현금,%)'] = df['배당성향 (현금,%)']/100
        df.rename(columns={'배당금 (현금) (억원)':'배당금 (억원)'}, inplace=True)
        # DB_기업정보 FS_update_list에 있는 코드만 필터링, KEY컬럼 만들기
        df = helper.make_keycode(df)
        df = df[df['종목코드'].isin(code_list)].copy()
        df = df.dropna(axis=1, how='all') # 전체가 null인 경우는 삭제
        # ------- ------- ------- ------- ------- ------- ------- -------
        maper = {'IFRS 연결':'연결', 'GAAP 연결':'연결',
                'IFRS 개별':'개별', 'GAAP 개별':'개별', 'IFRS 별도':'개별'}
        df['연결/별도'] = df['재무제표기준'].map(maper)
        df.drop(columns='재무제표기준', inplace=True)
        df['temp_key'] = df['KEY']+df['연결/별도'] # 일시 key컬럼

        #주재무제표만 남기기 위해서 naver에서 가져온 종목별 주재무제표 df와 inner join
        temp_fs = conn_db.from_('DB_기업정보','종목별_주재무제표')
        maper = {'IFRS연결':'연결', 'GAAP연결':'연결',
                'IFRS개별':'개별', 'GAAP개별':'개별', 'IFRS별도':'개별'}
        temp_fs['연결/별도'] = temp_fs['재무제표기준'].map(maper)
        temp_fs['temp_key'] = temp_fs['KEY']+temp_fs['연결/별도']
        temp_fs.drop(columns=['재무제표기준','KEY','연결/별도'], inplace=True)
        df = df.merge(temp_fs, on='temp_key', how='inner').drop(columns=['temp_key'])
        del temp_fs
        #------------------------------------------------------------------------------------
        df.drop(columns=['종목코드','종목명'],inplace=True)
        df = df.merge(conn_db.from_("DB_기업정보", '총괄'), on='KEY', how='left')
        # 저장
        df.to_pickle(folder_fn + "1_invest_ratio_from_fnguide_시계열.pkl")
        print('fnguide 투자지표 저장완료')
        print('(소요시간: ' + str(helper.now_time() - start_time)+")")
        merge_df_all_numbers()
        del df, old_df, cols
    else:
        print('업데이트할 내역 없음')
    #--------------------------------------------------------------------------------------------------------------------
#FN GUIDE 기업정보
def get_fnguide_company_info(code):
    # fnguide 기업정보 결과물 넣을 dataframes---------------
    global company_info # 업종, business summary
    global financial_highlights # 재무제표
    global sales_mix # 제품별 매출비중 가장 최근
    global market_share # 시장점유율 가장 최근
    global cogs_n_oc # 판관비율추이, 매출원가율추이
    global export_n_domestic # 수출 및 내수 구성

    #업종, business summary, 재무제표 공통
    url = f"http://comp.fnguide.com/SVO2/asp/SVD_Main.asp?pGB=1&gicode=A{code}&cID=&MenuYn=Y&ReportGB=&NewMenuID=101&stkGb=701"
    time.sleep(2)
    r = requests.get(url, headers={'User-Agent': user_agent})
    dom = BeautifulSoup(r.text, "html.parser")
    #업종, business summary
    # 업종 가져오기
    temp = []
    for i in [1,2]: # 1=KSE 업종, 2=FICS 업종
        text = dom.select(f'#compBody > div.section.ul_corpinfo > div.corp_group1 > p > span.stxt.stxt{i}')[0]
        temp.append(text.text.replace('\xa0',' ').split(' ',1))
    industry = pd.DataFrame(temp).T
    # 첫행을 컬럼으로 변경하고 원래있던 첫행은 삭제
    industry.columns = industry.iloc[0]
    industry = industry.iloc[1:,].copy().reset_index(drop=True)
    #-------------------------------------

    # Business Summary 가져오기
    # update날짜 앞뒤로 []가 있어서 삭제
    try: # 일부 종목은 Business Summary 없음
        biz_summary_date = dom.select('#bizSummaryDate')[0].text.strip()[1:-1]
        # 제목
        biz_summary_title = dom.select('#bizSummaryHeader')[0].text.replace('\xa0',' ').strip()
        # 내용
        contents =[]
        for i in range(len(dom.select('#bizSummaryContent > li'))):
            contents.append(dom.select('#bizSummaryContent > li')[i].text.replace('\xa0',' ').strip())
        contents = [contents[0] + " " + contents[1]]
        # 합쳐서 하나로 만들기 + 업종도
        contents = pd.DataFrame([biz_summary_date, biz_summary_title, contents]).T
        contents[2] = contents[2][0][0] # 내용 컬럼의 값이 앞뒤로 list[]화 되어 있어서 문자열로 변경
        df = pd.concat([contents, industry], axis=1)
        df['종목코드'] = code
        # 결과물 추가
        company_info = company_info.append(df)
        del contents, biz_summary_date, biz_summary_title, industry, temp
    except:
        pass

    # 재무제표
    # highlight_D_A # 연결 전체, highlight_B_A # 별도 전체 - 사용x
    df_types = {'highlight_D_Y':'연결 연간',
                'highlight_D_Q':'연결 분기',
                'highlight_B_Y':'별도 연간',
                'highlight_B_Q':'별도 분기'}
    df_all = pd.DataFrame()
    for fs_type in df_types.keys():
        try:
            # 날짜 컬럼
            temp = dom.select(f'#{fs_type} > table > thead > tr.td_gapcolor2 > th > div')
            date_cols = []
            for x in [x.text.strip() for x in temp]:
                if len(x)>7:
                    date_cols.append(x.split('추정치')[-1].strip())
                else:
                    date_cols.append(x)
            # 계정명 컬럼
            temp = dom.select(f'#{fs_type} > table > tbody > tr > th')
            account_cols = []
            for acc in [x.text.strip() for x in temp]:
                if acc.count("(",)>1:
                    account_cols.append(acc.split(')',1)[0] + ')')
                else :
                    account_cols.append(acc)
            df = pd.DataFrame(account_cols)
            # data 값
            data_values = [x.text.strip() for x in dom.select(f'#{fs_type} > table > tbody > tr > td')]
            lap = len(date_cols)
            for i in range(lap):
                temp_df = pd.DataFrame(data_values[i::lap]) # 컬럼의 갯수(lap)만큼 매n번째마다 합쳐줌
                df = pd.concat([df, temp_df], axis=1)
            # 항목 컬럼명 추가해서 df 컬럼 만들기
            df.columns = ['항목'] + date_cols
            df['연결/별도'] = df_types[fs_type].split(' ')[0]
            df['연간/분기'] = df_types[fs_type].split(' ')[1]
            df = df.melt(id_vars=['항목', '연결/별도', '연간/분기'], var_name='날짜',value_name='값')
            df = clean_numeric_value(df)
            df_all = df_all.append(df)
        except:
            pass
    try:
        df_all['종목코드'] = code
        # 결과물 추가
        financial_highlights = financial_highlights.append(df_all)
        del df_all, df, temp, data_values
    except:
        pass

    #----- 제품별 매출비중, 시장점유율, 판관비율, 매출원가율, 수출 및 내수구성 공통 ------ ------ ------
    url = f"http://comp.fnguide.com/SVO2/asp/SVD_Corp.asp?pGB=1&gicode=A{code}&cID=&MenuYn=Y&ReportGB=&NewMenuID=102&stkGb=701"
    r = requests.get(url, headers={'User-Agent': user_agent})
    dom = BeautifulSoup(r.text, "html.parser")

    #----- 제품별 매출비중 가장 최근 ----- ------ ------ ------ ------ ------ ------ ------ -----
    # 날짜 컬럼
    date_cols = dom.select('#divProduct > div.ul_col2_l > div > div.um_table.pd_t1 > table > thead > tr > th')
    date_cols = [x.text.strip() for x in date_cols]

    # 제품명
    products = dom.select('#divProduct > div.ul_col2_l > div > div.um_table.pd_t1 > table > tbody > tr > th')
    products = [x.text.replace('\xa0',' ').strip() for x in products]
    df = pd.DataFrame(products)

    # 제품별 매출비중
    data_values = dom.select('#divProduct > div.ul_col2_l > div > div.um_table.pd_t1 > table > tbody > tr > td')
    data_values = [x.text.strip() for x in data_values]

    try:
        # 데이터 df
        lap = len(date_cols) -1
        for i in range(lap):
            temp_df = pd.DataFrame(data_values[i::lap]) # 컬럼의 갯수(lap)만큼 매n번째마다 합쳐줌
            df = pd.concat([df, temp_df], axis=1)
        df.columns=date_cols
        df = df[df.columns.tolist()[::lap]]
        # 마지막 정리
        # 본래 날짜 컬럼인것을 '구성비'로 수정하고 마지막 날짜만 남기기
        mix_date = df.columns.tolist()[-1] # 마지막 날짜 컬럼명
        df.rename(columns={mix_date:'구성비'}, inplace=True)
        df['기준날짜'] = mix_date # 마지막날짜를 기준날짜로 삽입
        # 길이가 0이 안되면 삭제
        filt = df['구성비'].apply(len) > 0
        df = df.loc[filt].copy()
        df['구성비'] = df['구성비'].astype('float')/100
        # 제품명이 기타(계)인 것 보다 아래에 있으면 삭제
        filt = df['제품명']=='기타(계)'
        try:
            df = df[df.index.tolist() < df[filt].index].copy().reset_index(drop=True)
        except:
            pass
        # 결과물 추가
        df['종목코드'] = code
        sales_mix = sales_mix.append(df)
        del df, filt, mix_date, data_values, products, date_cols
    except:
        pass

    #----- 시장점유율 가장 최근 ------ ------ ------ ------ ------ ------ ------ ------ ------
    # 표 컬럼
    ms_col = dom.select('#divProduct > div.ul_col2_r > div > div.um_table.pd_t1 > table > thead > th')
    ms_col = [x.text.strip() for x in ms_col]
    # 제품 컬럼
    products = dom.select('#divProduct > div.ul_col2_r > div > div.um_table.pd_t1 > table > tbody > tr > th')
    products = [x.text.replace('\xa0',' ').strip() for x in products]
    # 제품별 시장점유율
    data_values = dom.select('#divProduct > div.ul_col2_r > div > div.um_table.pd_t1 > table > tbody > tr > td')
    data_values = [x.text.strip() for x in data_values]
    # 한번에 df로 만들기
    df = pd.DataFrame([products, data_values]).T
    try:
        df.columns = ms_col
        # 마지막 정리
        filt1 = df['시장점유율'].apply(len) > 0
        filt2 = df['주요제품']!='전체'
        filt = filt1 & filt2
        df = df.loc[filt].copy()
        df['시장점유율'] = df['시장점유율'].astype('float')/100
        df['종목코드'] = code
    # 결과물 추가
        market_share = market_share.append(df)
        del filt1, filt2, df, data_values, products, ms_col
    except:
        pass

    #----- 판관비율추이, 매출원가율추이 ------ ------ ------ ------ ------ ------ ------ ------
    cost_types = {'panguanD_01':'연결,판관비율', 'panguanB_01':'별도,판관비율',
                    'panguanD_02':'연결,매출원가율', 'panguanB_02':'별도,매출원가율'}
    df = pd.DataFrame()
    for cost_type in cost_types.keys():
        try:
            date_cols = dom.select(f'#{cost_type} > div > div.um_table > table > thead > tr > th')
            date_cols = [x.text.strip() for x in date_cols]
            data_values = dom.select(f'#{cost_type} > div > div.um_table > table > tbody > tr > td')
            data_values = [cost_types[cost_type]] +  [x.text.strip() for x in data_values]
            temp = pd.DataFrame(data_values).T
            temp.columns = date_cols
            df = df.append(temp)
        except:
            pass
    try:
        df = df.melt(id_vars='항목',var_name='날짜',value_name='값')
        df = clean_numeric_value(df)
        df['값'] = df['값']/100
        df['연결/별도'] = df['항목'].str.split(',', expand=True)[0]
        df['항목'] = df['항목'].str.split(',', expand=True)[1]
        df['종목코드'] = code
        # 결과물 추가
        cogs_n_oc = cogs_n_oc.append(df)
        del df, date_cols, data_values, temp
    except:
        del df, date_cols, data_values, temp

    # 수출 및 내수구성 정리
    corp_types = {'corpExport_D':'연결',
                  'corpExport_B':'별도'}
    df = pd.DataFrame()
    for corp_type in corp_types.keys():
        try:
            # 날짜컬럼
            col = dom.select(f'#{corp_type} > table > thead > tr.th2row_f > th')
            col = [x.text.strip() for x in col]
            # ['매출유형', '제품명', '2017/12', '2018/12', '2019/12']
            #col의 결과에서 날짜만 선택 → ['2017/12', '2018/12', '2019/12']
            date_cols = col[2:]
            # 날짜와 내수/수출 combination된 df 만들기
            date_df = pd.DataFrame(['매출유형','제품명'] + [(x,y) for x in date_cols for y in ['내수','수출']])
            #------ ------ ------ ------ ------ ------ ------ ------
            # 매출유형 철럼
            sales_type = dom.select(f'#{corp_type} > table > tbody > tr > td.clf') # 매출유형
            sales_type = [x.text.strip() for x in sales_type]
            # ['제품', '기타', '기타', '']
            #------ ------ ------ ------ ------ ------ ------ ------
            # 제품명
            products = dom.select(f'#{corp_type} > table > tbody > tr > td.l')
            products =  [x.text.replace('\xa0',' ').strip()  for x in products]
            # ['Display 패널', 'LCD, OLED 기술 특허', '원재료,부품 등', '합계']
            # 매출유형+제품명 합친 df
            col_df = pd.DataFrame([sales_type, products]).T
            #------ ------ ------ ------ ------ ------ ------ ------
            # 데이터 정리
            data_values = dom.select(f'#{corp_type} > table > tbody > tr > td.r')
            data_values = [x.text.strip() for x in data_values]
            lap = len(date_cols)*2 # 다중index 컬럼이라서 *2 (날짜별 내수/수출)
            temp_df = pd.DataFrame()
            for i in range(lap):
                temp = pd.DataFrame(data_values[i::lap])
                temp_df = pd.concat([temp_df, temp], axis=1)
            #------ ------ ------ ------ ------ ------ ------ ------
            # 매출유형+제품명 합친 df + 데이터 df
            temp_df = pd.concat([col_df, temp_df], axis=1)
            # 매출유형+제품명 합친 df + 데이터 df + 날짜df
            temp_df = pd.concat([date_df, temp_df.T.reset_index(drop=True)], axis=1).T
            #------ ------ ------ ------ ------ ------ ------ ------
            # 합쳐진거 전처리
            # 첫행을 column으로 셋팅하고 첫행 삭제
            temp_df.columns = temp_df.iloc[0]
            temp_df = temp_df.iloc[1:,].copy()
            # wide to tidy
            temp_df = temp_df.melt(id_vars=['매출유형','제품명'], var_name='임시', value_name='값')
            temp_df['연결/별도'] = corp_types[corp_type]
            # 컬럼 정리
            df = df.append(temp_df)
        except:
            pass
    try:
        df['날짜'] = df['임시'].str[0]
        df['수출/내수'] = df['임시'].str[1]
        df.loc[df['매출유형'].apply(len)==0,'매출유형'] = df.loc[df['매출유형'].apply(len)==0,'제품명']
        col = ['매출유형','제품명','수출/내수','연결/별도','날짜','값']
        df = df[col].sort_values(by='날짜', ascending=False)
        '''
        값이 없는 경우에도 제품명은 가져오기 위해서
        df['값'].sum() ==''으로 test해서 각 경우 별도로 처리
        '''
        df = df.drop_duplicates(subset=['매출유형','제품명','연결/별도','값'])
        if df['값'].sum() =='':
            df = df.drop_duplicates(subset=['매출유형','연결/별도','제품명'])
            df['값'] = 0
        else:
            filt = df['값'].apply(len)>0
            df = df.loc[filt].copy()
            df['값'] = df['값'].astype('float')/100
        df['종목코드'] = code
        df.reset_index(drop=True, inplace=True)
        # 결과물 추가
        export_n_domestic = export_n_domestic.append(df)
        # del df, col, temp_df, data_values, products, date_cols
    except:
        pass 
    print(f'{code} 완료')
    #--------------------------------------------------------------------------------------------------------------------

@helper.timer
def update_fnguide_company_info(param='all'):
    # fnguide 기업정보 결과물 넣을 dataframes---------------
    global company_info  # 업종, business summary
    global financial_highlights  # 재무제표
    global sales_mix  # 제품별 매출비중 가장 최근
    global market_share  # 시장점유율 가장 최근
    global cogs_n_oc  # 판관비율추이, 매출원가율추이
    global export_n_domestic  # 수출 및 내수 구성
    global code_list 
    # with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
    #     executor.map(get_fnguide_company_info, code_list)
    if param !='all':
        new = conn_db.from_("DB_기업정보", 'FS_update_list')['종목코드']
        old = conn_db.from_("DB_기업정보", 'from_fnguide_기업정보')['종목코드']
        new_code_list = list(set(new) - set(old))
        if len(new_code_list)>0:
            for code in new_code_list:
                try:
                    get_fnguide_company_info(code)
                    time.sleep(2)
                except:
                    print(f"{code} 에러나서 pass")
                    pass
        else:
            print('업데이트할 내역 없음')
            del new, old, new_code_list
    else:
        for code in code_list:
            get_fnguide_company_info(code)
    
    # 업종, business summary------- ------- ------- ------- ------- ------- ------- -------
    if len(company_info)>0:
        company_info = helper.make_keycode(company_info.reset_index(drop=True))
        # 코스피(KSE)와 코스닥(KOSDAQ)이 별도 컬럼에 있는데 KRX하나로 통합
        all_cols = company_info.columns.tolist()
        for col in all_cols: # 컬럼별로 앞뒤 공백제거
            company_info[col] = company_info[col].str.strip()
        if 'KSE' in all_cols:
            company_info['KRX'] = (company_info['KSE'].astype(str) + company_info['KOSDAQ'].astype(str)).str.replace('nan','').str.strip()
        else:
            company_info['KRX'] = company_info['KOSDAQ'].astype(str).str.replace('nan','').str.strip()
        # 종목앞에 코스피/코스닥이 있어서 삭제
        company_info['KRX'] = company_info['KRX'].str.split(' ',1,expand=True)[1] 
        company_info['KRX'].fillna(company_info['FICS'], inplace=True)
        if 'KSE' in all_cols:
            company_info = company_info.rename(columns={0:'기준날짜',  1:'요약', 2:'내용'}).drop(columns=['KSE','KOSDAQ'])
        else:
            company_info = company_info.rename(columns={0:'기준날짜',  1:'요약', 2:'내용'}).drop(columns='KOSDAQ')

        # 가장 최근 파일이 위로 가도록 순서 정렬해서 취합하고 과거 df랑 중복 되는거 삭제
        old_df = conn_db.from_('DB_기업정보','from_fnguide_기업정보')
        cols = ['KEY']
        company_info = drop_duplicate_rows(company_info, old_df, cols)
        try:
            company_info.rename(columns={'et':'기준날짜'},inplace=True)
        except:
            pass

        conn_db.to_(company_info, 'DB_기업정보', 'from_fnguide_기업정보')
        # 취합본 수정해 놓기
        import co_info as co
        co.get_all_co_info()
        del company_info, old_df

    # 재무제표------- ------- ------- ------- ------- ------- ------- -------
    if len(financial_highlights)>0:
        financial_highlights = helper.make_keycode(financial_highlights.reset_index(drop=True))
        # 가장 최근 파일이 위로 가도록 순서 정렬해서 취합하고 과거 df랑 중복 되는거 삭제
        file = folder_fn_backup + "fnguide_financial_highlights_원본취합본.pkl"
        old_df = pd.read_pickle(file)
        # 컬럼 = 항목, 연결/별도, 연간/분기, 날짜, 값, 종목코드, 종목명, KEY
        cols = ['항목', '연결/별도', '날짜', '연간/분기', '종목코드', '종목명', 'KEY']
        df = drop_duplicate_rows(financial_highlights, old_df, cols)
        df.to_pickle(file)

        # 전처리------------------------------------------------------
        for item in [col for col in df['항목'].unique().tolist() if '%' in col]:
            temp = df.loc[df['항목'] == item, '값']
            df.loc[df['항목']==item, '값'] = temp/100

        try:
            dates = df.loc[df['날짜'].str.contains('(P)'), ['날짜']]
            dates['날짜'] = dates['날짜'].str.split('\n', expand=True).iloc[:,-1:]
            df.loc[df['날짜'].str.contains('(P)'), ['날짜']] = dates
        except:
            pass

        # 실적/전망 컬럼 생성
        for expect in ['E','P']:
            if df['날짜'].str.contains(expect).sum()>0:
                df['실적/전망'] = df['날짜'].apply(lambda x : '전망' if expect in x else '실적')
                df['날짜'] = df['날짜'].str.replace(f'\({expect}\)', '')

        # 항목에 있는 괄호 부분 삭제
        for item in ['\(원\)', '\(배\)','\(%\)']:
            df['항목'] = df['항목'].str.replace(item, '')

        # 주재무제표만 선택--------------------------------------------------
        df.drop(columns=['종목명','종목코드'], inplace=True)
        cols = df.columns.tolist()
        df['temp_key'] = df['KEY']+df['연결/별도']
        main_df = conn_db.from_('DB_기업정보','종목별_주재무제표')
        main_df['재무제표기준'] = main_df['재무제표기준'].str.replace('IFRS','')
        main_df['temp_key'] = main_df['KEY']+main_df['재무제표기준']
        filt = df['temp_key'].isin(main_df['temp_key'])
        df = df.loc[filt,cols].copy()
        df = df.drop(columns=['연결/별도']).reset_index(drop=True)

        # 저장하기---------------------------------------------------
        df.to_pickle(conn_db.get_path('fnguide_financial_highlights') + '.pkl')
        cols = ['KEY','연간/분기','날짜','실적/전망']
        df = df.pivot_table(index=cols, columns='항목', values='값').reset_index()
        df.columns.name=None
        conn_db.to_(df,'fnguide_fs_highlights','fs')

    # 제품별 매출비중 가장 최근------- ------- ------- ------- ------- ------- ------- -------
    if len(sales_mix)>0:
        sales_mix = helper.make_keycode(sales_mix.reset_index(drop=True))

        # 가장 최근 파일이 위로 가도록 순서 정렬해서 취합하고 과거 df랑 중복 되는거 삭제
        old_df = conn_db.from_('DB_기업정보', 'sales_mix_from_fnguide')
        cols = ['종목코드', '종목명', '제품명', 'KEY']
        sales_mix = drop_duplicate_rows(sales_mix, old_df, cols)

        # 컬럼 = 제품명,구성비,기준날짜,종목코드,종목명,KEY
        conn_db.to_(sales_mix, 'DB_기업정보', 'sales_mix_from_fnguide')
        del sales_mix, old_df

    # 시장점유율 가장 최근------- ------- ------- ------- ------- ------- ------- -------
    if len(market_share):
        market_share = helper.make_keycode(market_share.reset_index(drop=True))
        # 가장 최근 파일이 위로 가도록 순서 정렬해서 취합하고 과거 df랑 중복 되는거 삭제
        old_df = conn_db.from_('DB_기업정보', 'mkt_share_from_fnguide')
        # 컬럼 = 주요제품,시장점유율,종목코드,종목명,KEY
        cols = ['종목코드', '종목명', '주요제품', 'KEY']
        market_share = drop_duplicate_rows(market_share, old_df, cols)
        conn_db.to_(market_share, 'DB_기업정보', 'mkt_share_from_fnguide')
        del market_share, old_df

    # 판관비율추이, 매출원가율추이------- ------- ------- ------- ------- ------- ------- -------
    if len(cogs_n_oc)>0:
        cogs_n_oc = cogs_n_oc.pivot_table(index=['날짜','연결/별도','종목코드'],
                                        columns='항목',values='값').reset_index()
        cogs_n_oc.columns.name = None
        cogs_n_oc = helper.make_keycode(cogs_n_oc.reset_index(drop=True))
        # 가장 최근 파일이 위로 가도록 순서 정렬해서 취합하고 과거 df랑 중복 되는거 삭제
        file = folder_fn_backup + 'fnguide_판관비율매출원가율_원본취합본.pkl'
        old_df = pd.read_pickle(file)

        # 컬럼 = 날짜, 연결/별도, 종목코드, 매출원가율, 판관비율, 종목명, KEY
        cols = ['날짜', '연결/별도', 'KEY', '종목코드','종목명']
        cogs_n_oc = drop_duplicate_rows(cogs_n_oc, old_df, cols)
        cogs_n_oc.to_pickle(file)

        cogs_n_oc['날짜'] = cogs_n_oc['날짜'].apply(lambda x : x.replace('/','-') if '/' in x else x.replace('.','-') if '.'in x else x )
        cogs_n_oc['key'] = cogs_n_oc['연결/별도']+cogs_n_oc['KEY']
        main_fs = conn_db.from_('DB_기업정보','종목별_주재무제표')
        main_fs['재무제표기준'] = main_fs['재무제표기준'].str[-2:]
        main_fs['key'] = main_fs['재무제표기준']+main_fs['KEY']
        cogs_n_oc = main_fs.merge(cogs_n_oc, on=['key', 'KEY'], how='inner').drop(columns='key')
        cols = ['KEY','재무제표기준','날짜','매출원가율','판관비율']
        cogs_n_oc = cogs_n_oc[cols].reset_index(drop=True)
        conn_db.to_(cogs_n_oc, 'DB_기업정보', '매출원가율_판관비율_from_fnguide')
        del cogs_n_oc, old_df

    # 수출 및 내수 구성------- ------- ------- ------- ------- ------- ------- -------
    if len(export_n_domestic)>0:
        export_n_domestic = export_n_domestic.pivot_table(index=['날짜','연결/별도','종목코드','매출유형','제품명'],
                                                        columns='수출/내수',values='값').reset_index()
        export_n_domestic.columns.name=None
        export_n_domestic = export_n_domestic.sort_values(by=['종목코드','날짜',
                                                            '연결/별도','매출유형']).reset_index(drop=True)
        export_n_domestic = helper.make_keycode(export_n_domestic.reset_index(drop=True))
        export_n_domestic['날짜'] = export_n_domestic['날짜'].apply(lambda x: x.replace('/', '-') if '/' in x else x.replace('.', '-') if '.'in x else x)

        # 가장 최근 파일이 위로 가도록 순서 정렬해서 취합하고 과거 df랑 중복 되는거 삭제
        file = folder_fn_backup+'제품별_수출및내수_구성비_받은원본취합본.pkl'
        old_df = pd.read_pickle(file)

        # 컬럼 = 날짜,연결/별도,종목코드,매출유형,제품명,내수,수출,종목명,KEY
        cols = ['날짜', '연결/별도','매출유형','KEY', '제품명','종목코드','종목명']
        export_n_domestic = drop_duplicate_rows(export_n_domestic, old_df, cols)
        export_n_domestic.to_pickle(file) # 백업

        # 주재무제표만 필터링하고 저장
        export_n_domestic['key'] = export_n_domestic['연결/별도']+export_n_domestic['KEY']
        main_fs = conn_db.from_('DB_기업정보','종목별_주재무제표')
        main_fs['재무제표기준'] = main_fs['재무제표기준'].str[-2:]
        main_fs['key'] = main_fs['재무제표기준']+main_fs['KEY']
        export_n_domestic = main_fs.merge(export_n_domestic,
                                            on=['key', 'KEY'], how='inner').drop(columns='key')
        cols = ['KEY','재무제표기준','날짜','매출유형','제품명','내수','수출']
        export_n_domestic = export_n_domestic[cols].reset_index(drop=True)
        conn_db.to_(export_n_domestic,'DB_기업정보','export_n_domestic_from_fnguide') 
    merge_df_all_numbers()
