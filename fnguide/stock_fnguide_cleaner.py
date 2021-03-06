import pandas as pd
from glob import glob
import helper
import conn_db

code_list = conn_db.from_("DB_기업정보", 'FS_update_list')['종목코드']
# fsratio_class = conn_db.from_('from_fnguide_항목', '재무비율_항목정리').drop_duplicates()
# invest_class = conn_db.from_('from_fnguide_항목', '투자지표_항목정리').drop_duplicates()

folder_fn = conn_db.get_path('folder_fn')
folder_fn_backup = conn_db.get_path('folder_fn_backup')

# 회사별로 가장 최근 날짜에 해당하는 것만 남기기
def _filter_date(df):
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
# 주재무제표만 남기기 위해서 naver에서 가져온 종목별 주재무제표 df와 inner join
def _join_to_get_main_fs_type(df, mapper):

    df['연결/별도'] = df['재무제표기준'].map(mapper)
    df.drop(columns='재무제표기준', inplace=True)
    df['temp_key'] = df['KEY']+df['연결/별도'] # 일시 key컬럼


    temp_fs = conn_db.from_('DB_기업정보','종목별_주재무제표')
    temp_mapper = {'IFRS연결':'연결',
                    'GAAP연결':'연결',
                    'IFRS개별':'개별',
                    'GAAP개별':'개별',
                    'IFRS별도':'개별'}
    temp_fs['연결/별도'] = temp_fs['재무제표기준'].map(temp_mapper)
    temp_fs['temp_key'] = temp_fs['KEY']+temp_fs['연결/별도']
    temp_fs.drop(columns=['재무제표기준','KEY','연결/별도'], inplace=True)
    df = df.merge(temp_fs, on='temp_key', how='inner').drop(columns=['temp_key'])
    return df

# fnguide 재무비율 전처리
def clean_fsratio_from_fnguide():
    df = pd.read_pickle(conn_db.get_path('fsratio_from_fnguide_raw')+'.pkl')
    # KEY컬럼 만들기
    df = helper.make_keycode(df)
    df = _join_to_get_main_fs_type(df)

    # DB_기업정보 FS_update_list에 있는 코드만 필터링
    filt = (df['값']!=0.0) & (df['종목코드'].isin(code_list))
    df = df[filt].copy()
    df = df.sort_values(by='날짜', ascending=False)

    # 전체가 null인 경우는 삭제
    df = df.dropna(axis=1, how='all')

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

    df.to_pickle(folder_fn + "2_fsratio_from_fnguide_시계열.pkl")
    print('fnguide 재무비율 시계열용 pickle 저장완료')

    # 분기/연간에서 가장 최근값만 있는 df만들기
    df = df.melt(id_vars=dcols, var_name='항목', value_name='값').dropna()
    df = df.sort_values(by=dcols, ascending=False).reset_index(drop=True)
    # df = df.groupby(['KEY','주기','연결/별도','항목'], as_index=False).head(1)

    filt = df['주기']=='분기'
    df_q = df[filt]
    df_y = df[~filt]

    # 연간 + 분기 합치기
    df = pd.concat([_filter_date(df_q), _filter_date(df_y)])

    df['항목'] = df['항목'] + " _" +  df['주기']
    df = df.drop(columns=['날짜','주기']).reset_index(drop=True)

    # 행/열 전환하고 저장하기
    df = df.pivot_table(index=['KEY', '연결/별도'], columns='항목', values='값').reset_index()
    df.columns.name = None

    conn_db.to_(df, 'from_fnguide', 'fnguide_fsratio_최근값만')
    merge_df_all_numbers() # 전체 취합본 업데이트

def update_fnguide_invest_ratio(param='all'):  # fnguide 투자지표 업데이트
    global code_list
    global max_workers

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
        df = helper.clean_numeric_value(df)

        # 백업 불러와서 취합본 업데이트
        old_df = pd.read_pickle(file)
        cols = ['항목', '날짜', '재무제표기준', '종목코드']
        df = helper.add_df(df, old_df,cols)
        df.to_pickle(file)

        df['항목'] = df['항목'].str.replace('\(', ' (').str.strip()
        df = df.pivot_table(index=['날짜','종목코드','재무제표기준'], columns='항목', values='값').reset_index()
        df.columns.name = None
        df['배당성향 (현금,%)'] = df['배당성향 (현금,%)']/100
        df.rename(columns={'배당금 (현금) (억원)':'배당금 (억원)'}, inplace=True)

        # DB_기업정보 FS_update_list에 있는 코드만 필터링, KEY컬럼 만들기
        df = helper.make_keycode(df)
        df = df[df['종목코드'].isin(code_list)].copy()
        df = df.dropna(axis=1, how='all') # 전체가 null인 경우는 삭제

        df = _join_to_get_main_fs_type(df)

        df.drop(columns=['종목코드','종목명'],inplace=True)
        df = df.merge(conn_db.from_("DB_기업정보", '총괄'), on='KEY', how='left')
        # 저장
        df.to_pickle(folder_fn + "1_invest_ratio_from_fnguide_시계열.pkl")
        print('fnguide 투자지표 저장완료')
        print('(소요시간: ' + str(helper.now_time() - start_time)+")")
        merge_df_all_numbers()

    else:
        print('업데이트할 내역 없음')

# 업종, business summary
@helper.timer
def clean_fnguide_company_info(df):
    df = helper.make_keycode(df.reset_index(drop=True))
    # 코스피(KSE)와 코스닥(KOSDAQ)이 별도 컬럼에 있는데 KRX하나로 통합
    df['KRX'] = df['KOSDAQ'].fillna(df['KSE'])
    df.drop(columns=['KSE','KOSDAQ'], inplace=True)
    try:
        df['KRX'] = df['KRX'].fillna(df['KONEX'])
        df.drop(columns=['KONEX'], inplace=True)
    except:
        pass
    all_cols = df.columns.tolist()
    for col in all_cols:  # 컬럼별로 앞뒤 공백제거
        df[col] = df[col].str.strip()

    df['KRX'] = df['KRX'].str.split(' ', 1, expand=True)[1].str.strip()
    # 종목앞에 코스피/코스닥이 있어서 삭제
    names = {0:'기준날짜',
             1:'요약',
             2:'내용'}
    df.rename(columns=names, inplace=True)
    # 가장 최근 파일이 위로 가도록 순서 정렬해서 취합하고 과거 df랑 중복 되는거 삭제
    old_df = conn_db.from_('DB_기업정보','from_fnguide_기업정보')
    cols = ['KEY']
    df = helper.add_df(df, old_df, cols)
    try:
        df.rename(columns={'et':'기준날짜'},inplace=True)
    except:
        pass
    conn_db.to_(df, 'DB_기업정보', 'from_fnguide_기업정보')

# 재무제표 주요 항목
@helper.timer
def clean_financial_highlights(df):
    df = helper.make_keycode(df.reset_index(drop=True))
    # 가장 최근 파일이 위로 가도록 순서 정렬해서 취합하고 과거 df랑 중복 되는거 삭제
    file = folder_fn_backup + "fnguide_financial_highlights_원본취합본.pkl"
    old_df = pd.read_pickle(file)
    # 컬럼 = 항목, 연결/별도, 연간/분기, 날짜, 값, 종목코드, 종목명, KEY
    cols = ['항목', '연결/별도', '날짜', '연간/분기', '종목코드', '종목명', 'KEY']
    df = helper.add_df(df, old_df, cols)
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

# 제품별 매출비중
@helper.timer
def clean_salex_fix(df):
    df = helper.make_keycode(df.reset_index(drop=True))

    # 가장 최근 파일이 위로 가도록 순서 정렬해서 취합하고 과거 df랑 중복 되는거 삭제
    old_df = conn_db.from_('DB_기업정보', 'sales_mix_from_fnguide')
    cols = ['종목코드', '종목명', '제품명', 'KEY']
    df = helper.add_df(df, old_df, cols)

    # 컬럼 = 제품명,구성비,기준날짜,종목코드,종목명,KEY
    conn_db.to_(df, 'DB_기업정보', 'sales_mix_from_fnguide')

# 시장점유율 가장 최근
@helper.timer
def clean_market_share(df):
    df = helper.make_keycode(df.reset_index(drop=True))
    # 가장 최근 파일이 위로 가도록 순서 정렬해서 취합하고 과거 df랑 중복 되는거 삭제
    old_df = conn_db.from_('DB_기업정보', 'mkt_share_from_fnguide')
    # 컬럼 = 주요제품,시장점유율,종목코드,종목명,KEY
    cols = ['종목코드', '종목명', '주요제품', 'KEY']
    df = helper.add_df(df, old_df, cols)
    conn_db.to_(df, 'DB_기업정보', 'mkt_share_from_fnguide')

# 판관비율추이, 매출원가율추이
@helper.timer
def clean_cogs_and_expense(df):
    cols = ['날짜','연결/별도','종목코드']
    df = df.pivot_table(index=cols, columns='항목',values='값').reset_index()
    df.columns.name = None
    df = helper.make_keycode(df.reset_index(drop=True))
    # 가장 최근 파일이 위로 가도록 순서 정렬해서 취합하고 과거 df랑 중복 되는거 삭제
    file = folder_fn_backup + 'fnguide_판관비율매출원가율_원본취합본.pkl'
    old_df = pd.read_pickle(file)

    # 컬럼 = 날짜, 연결/별도, 종목코드, 매출원가율, 판관비율, 종목명, KEY
    cols = ['날짜', '연결/별도', 'KEY', '종목코드','종목명']
    df = helper.add_df(df, old_df, cols)
    df.to_pickle(file)

    df['날짜'] = df['날짜'].apply(lambda x : x.replace('/','-') if '/' in x else x.replace('.','-') if '.'in x else x )
    df['key'] = df['연결/별도']+df['KEY']
    main_fs = conn_db.from_('DB_기업정보','종목별_주재무제표')
    main_fs['재무제표기준'] = main_fs['재무제표기준'].str[-2:]
    main_fs['key'] = main_fs['재무제표기준']+main_fs['KEY']
    df = main_fs.merge(df, on=['key', 'KEY'], how='inner').drop(columns='key')
    cols = ['KEY','재무제표기준','날짜','매출원가율','판관비율']
    df = df[cols].reset_index(drop=True)
    conn_db.to_(df, 'DB_기업정보', '매출원가율_판관비율_from_fnguide')

# 수출 및 내수 구성
@helper.timer
def clean_export_n_domestic(df):
    cols = ['날짜','연결/별도','종목코드','매출유형','제품명']
    df = df.pivot_table(index=cols, columns='수출/내수',values='값').reset_index()
    df.columns.name=None

    cols = ['종목코드','날짜','연결/별도','매출유형']
    df = df.sort_values(by=cols).reset_index(drop=True)
    df = helper.make_keycode(df.reset_index(drop=True))

    df['날짜'] = df['날짜'].apply(lambda x: x.replace('/', '-') if '/' in x else x.replace('.', '-') if '.'in x else x)

    # 가장 최근 파일이 위로 가도록 순서 정렬해서 취합하고 과거 df랑 중복 되는거 삭제
    file = folder_fn_backup+'제품별_수출및내수_구성비_받은원본취합본.pkl'
    old_df = pd.read_pickle(file)

    # 컬럼 = 날짜,연결/별도,종목코드,매출유형,제품명,내수,수출,종목명,KEY
    cols = ['날짜', '연결/별도','매출유형','KEY', '제품명','종목코드','종목명']
    df = helper.add_df(df, old_df, cols)
    df.to_pickle(file) # 백업

    # 주재무제표만 필터링하고 저장
    df['key'] = df['연결/별도']+df['KEY']
    main_fs = conn_db.from_('DB_기업정보','종목별_주재무제표')
    main_fs['재무제표기준'] = main_fs['재무제표기준'].str[-2:]
    main_fs['key'] = main_fs['재무제표기준']+main_fs['KEY']

    cols = ['key', 'KEY']
    df = main_fs.merge(df, on=cols, how='inner').drop(columns='key')

    cols = ['KEY','재무제표기준','날짜','매출유형','제품명','내수','수출']
    df = df[cols].reset_index(drop=True)
    conn_db.to_(df,'DB_기업정보','export_n_domestic_from_fnguide')
