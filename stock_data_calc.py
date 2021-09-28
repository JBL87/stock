import pandas as pd
from glob import glob
import dart_fss as dart
import conn_db, helper

import warnings
warnings.filterwarnings(action='ignore')

# 총괄에 있는 코드 LIST로 가져오기
codes = conn_db.from_('DB_기업정보', '총괄')['KEY'].unique().tolist()

# 코스피 지수
kospi_df = conn_db.from_('코스피_코스닥지수', 'import_kospi_all')
kospi_df['Close'] = kospi_df['Close'].str.replace(',', '').astype(float)
names = {'Close': '코스피',
         'Date': '날짜'}
kospi_df.rename(columns=names, inplace=True)
df_index = conn_db.import_('PER_PBR_배당수익률_전체취합본')

# 주가, 거래량, 거래대금, 시가총액 데이터 불러와서 전처리
df_cap = conn_db.import_('시가총액_전체취합본')

row_filter = df_cap['DATE'] >= '2019-01-01'
col_filter = ['DATE', 'KEY', 'close',
              '거래량', '거래대금', '시가총액']
df_cap = df_cap.loc[row_filter, col_filter].reset_index(drop=True)
df_cap['시총대비 거래대금'] = df_cap['거래대금']/df_cap['시가총액']

cols = {'DATE': '날짜',
        'close': '주가'}
df_cap.rename(columns=cols, inplace=True)

def _make_dummy_df(days):
    # 가장 최근 n일 날짜
    last_n_days_df = df_cap[['날짜']].drop_duplicates().iloc[-days:]
    # 가장 최근 n일 날짜를 컬럼으로 하는 df
    df = pd.DataFrame(columns=['KEY'] +
                      last_n_days_df['날짜'].astype(str).tolist())
    return df
# 종목별 일별 상승률 계산
def _calc_daily_change(code, colname, days):
    '''
    종목별 일별 상승률 계산
    '''
    filt_date = df_cap['날짜'].isin(df_cap['날짜'].unique()[-(days+1):])
    filt_key = df_cap['KEY'] == code
    filt = filt_date & filt_key
    df = df_cap.loc[filt].reset_index(drop=True)
    try:
        if len(df) > 0:
            df[f'{colname} 전일비'] = df[colname].pct_change(periods=1)
            df['날짜'] = df['날짜'].astype(str)
            df = df.tail(days).pivot_table(
                index='KEY', columns='날짜', values=f'{colname} 전일비')
            df.reset_index(inplace=True)
            df.columns.name = None
            return df
        else:
            pass
    except:
        print(f'{code} 에서 오류')
# 종목별 일별 누적상승률 계산
def _calc_cum_change(code, colname):
    '''
    종목별 일별 누적상승률 계산
    '''
    cols = ['KEY','날짜',colname]
    df = df_cap.loc[df_cap['KEY'] == code, cols].iloc[-201:]

    if colname == '시가총액':
        df[colname] = df[colname]/1000000000000
    elif colname == '거래대금':
        df[colname] = df[colname]/100000000
    elif colname == '거래량':
        df[colname] = df[colname]/10000
    else:
        pass

    if len(df) > 0:
        for x in [1, 2, 3, 5, 10, 20, 45, 60, 120, 200]:
            df.loc[:, f'D-{x}'] = df[colname].pct_change(periods=x)
        return df.tail(1).reset_index(drop=True)
    else:
        pass
# N days 동안의 주가, 거래량, 거래대금, 시가총액 테이블 구하기
def _get_series_df(colname, days):
    '''
    N days 동안의 주가, 거래량, 거래대금, 시가총액 테이블 구하기
    '''
    cols = ['날짜', 'KEY', colname]
    if colname in ['주가', '거래량', '거래대금', '시가총액', '시총대비 거래대금']:
        df = df_cap[cols]
    else:
        df = df_index[cols]

    last_n_days_df = df[['날짜']].drop_duplicates().iloc[-days:]
    filt = df['날짜'].isin(last_n_days_df['날짜'].tolist())
    df = df.loc[filt].reset_index(drop=True)

    df['날짜'] = df['날짜'].astype(str)
    if colname == '시가총액':
        df[colname] = df[colname]/1000000000000
    elif colname == '거래대금':
        df[colname] = df[colname]/100000000
    elif colname == '거래량':
        df[colname] = df[colname]/10000
    elif colname == '배당수익률':
        df[colname] = df[colname]/100
    else:
        pass

    df = df.pivot_table(values=colname, columns='날짜',
                        index='KEY').reset_index()
    df.columns.name = None
    return df

# 최근 일별 추이
@helper.timer
def daily_values(days):
    '''
    최근 일별 추이
    '''
    upload_dict = {'주가':'최근주가',
                   '거래량':'최근거래량(만주)',
                   '거래대금':'최근거래대금(억원)',
                   '시가총액':'최근시가총액(조원)',
                   '시총대비 거래대금': '거래대금_시총대비'}
    # 신규상장인 경우 값 계산을 안해서 nan이 생겨서
    # loop 돌고나서 유효한 컬름으로 대체하기 위해서 미리 cols 생성
    cols = _make_dummy_df(days).columns.tolist()

    for colname in upload_dict.keys():
        df = _get_series_df(colname, days)[cols]
        conn_db.to_(df, 'data_from_krx', upload_dict[colname])

# 최근 일별 상승률
@helper.timer
def daily_change(days):
    '''
    최근 일별 상승률
    '''
    upload_dict = {'주가':'주가_전일비',
                    '시가총액':'시가총액_전일비'}
    # 신규상장인 경우 값 계산을 안해서 nan이 생겨서
    # loop 돌고나서 유효한 컬름으로 대체하기 위해서 미리 cols 생성
    cols = _make_dummy_df(days).columns.tolist()
    for colname in upload_dict.keys():
        df = pd.concat([_calc_daily_change(code, colname, days) for code in codes], ignore_index=True)
        conn_db.to_(df[cols], 'data_from_krx', upload_dict[colname])

# 종목별 일별 누적상승률
@helper.timer
def cum_change(days):
    '''
    누적상승률
    '''
    last_date = df_cap['날짜'].max()  # 가장 최근 날짜 필터링
    filt = df_cap['날짜']==last_date
    valid_codes = df_cap.loc[filt]['KEY'].tolist() # 가장 최근 날짜에 값이 있는 KEY

    upload_dict = {'주가':'주가_누적증감',
                    '시가총액':'시총_누적증감'}
    for colname in upload_dict.keys():
        df = pd.concat([_calc_cum_change(code, colname) for code in valid_codes], ignore_index=True)
        conn_db.to_(df, 'data_from_krx', upload_dict[colname])

# 코스피 일별 상승률
@helper.timer
def kospi_daily_chg(days):
    cols = ['날짜', '코스피']
    df = kospi_df[cols].tail(days+1)
    df['코스피 증감'] = df['코스피'].pct_change(periods=1)

    cols = ['날짜', '코스피 증감']
    df = df[cols].tail(days).reset_index(drop=True)
    conn_db.to_(df, '코스피_코스닥지수', 'kospi_전일비')

# 일별 시장대비 상승률
@helper.timer
def chg_over_market():
    # 코스피 전일비 불러오기
    df_kospi = conn_db.from_('코스피_코스닥지수', 'kospi_전일비')
    df_kospi['코스피 증감'] = df_kospi['코스피 증감'].astype(float)

    # 주가 전일비 불러오기
    df_price_chg = conn_db.from_('data_from_krx', '주가_전일비')
    df_price_chg = df_price_chg.melt(
        id_vars='KEY', var_name='날짜', value_name='주가 증감')
    df_price_chg['주가 증감'] = pd.to_numeric(df_price_chg['주가 증감'])

    # 합쳐서 시장대비 상승률 구하기
    df = df_price_chg.merge(df_kospi, how='left', on='날짜')
    df['시장대비'] = df['주가 증감'] - df['코스피 증감']

    # pivot 후 업로드
    cols = ['KEY', '날짜', '시장대비']
    df = df[cols]
    df = df.pivot_table(index='KEY', columns='날짜', values='시장대비').reset_index()
    df.columns.name = None
    conn_db.to_(df, 'data_from_krx', '시장대비수익률_일별')

# 코스피 일별 누적상승률
@helper.timer
def calc_kospi_cum_change(days):
    '''
    코스피 일별 누적상승률 계산
    '''
    cols = ['날짜','코스피']
    df = kospi_df[cols]
    for x in [1, 2, 3, 5, 10, 20, 45, 60, 120, 200]:
        df.loc[:,f'D-{x}'] = df['코스피'].pct_change(periods=x)

    df = df.tail(days).sort_values(by='날짜', ascending=False)
    df.reset_index(drop=True, inplace=True)
    conn_db.to_(df, '코스피_코스닥지수', '코스피증감')

# 종목별 시장대비 누적상승률
@helper.timer
def cum_chg_over_market():
    kospi_df = conn_db.from_('코스피_코스닥지수', '코스피증감').head(1)
    stock_df = conn_db.from_('data_from_krx', '시총_누적증감')

    if stock_df['날짜'].unique() == kospi_df['날짜'].unique():
        date = stock_df['날짜'].unique().tolist()[0]

        stock_df.drop(columns=['시가총액', '날짜'], inplace=True)
        kospi_df.drop(columns=['코스피', '날짜'], inplace=True)
    else:
        print('기준날짜가 다름')

    date_cum_cols = kospi_df.columns.tolist()
    # ['D-1', 'D-2', 'D-3', 'D-5', 'D-10', 'D-20', 'D-45', 'D-60', 'D-120', 'D-200']
    for col in date_cum_cols:
        kospi_df[col] = pd.to_numeric(kospi_df[col])
        stock_df[col] = pd.to_numeric(stock_df[col])

    stock_df = stock_df.melt(id_vars='KEY', var_name='날짜', value_name='시총 증감')
    kospi_df = kospi_df.melt(var_name='날짜', value_name='코스피 증감').reset_index()

    cols = ['KEY', 'index']
    df = stock_df.merge(kospi_df, on='날짜').sort_values(by=cols)

    df['시장대비수익률'] = df['시총 증감'] - df['코스피 증감']
    cols = ['KEY', '날짜', '시장대비수익률']
    df = df[cols].pivot_table(index='KEY', columns='날짜',
                            values='시장대비수익률').reset_index()
    df.columns.name = None

    cols = ['KEY'] + date_cum_cols
    df = df[cols]
    conn_db.to_(df, 'data_from_krx', '시장대비수익률_누적')

# PER_PBR_배당수익률 table 만들기
@helper.timer
def make_code_index_daily_df(days):
    '''
    PER_PBR_배당수익률 table 만들기
    '''
    # 신규상장인 경우 값 계산을 안해서 nan이 생겨서
    # loop 돌고나서 유효한 컬름으로 대체하기 위해서 미리 cols 생성
    cols = _make_dummy_df(days).columns.tolist()

    for col in ['EPS','PER','PBR','BPS','DPS','배당수익률']:
        df_index[col].fillna(0, inplace=True)
        df = _get_series_df(col,days)[cols]
        conn_db.to_(df, 'data_from_krx', col)

# 가장 최근날짜의 지표 모은 df 만들기
@helper.timer
def make_last_date_indexes():
    '''
    가장 최근날짜의 지표 모은 df 만들기
    '''
    max_date = df_cap['날짜'].unique().max()
    filt_cap = df_cap['날짜'] == max_date
    filt_per = df_index['날짜'] == max_date

    cols = ['날짜', 'KEY']
    df = df_cap[filt_cap].merge(df_index[filt_per], on=cols, how='left')

    cols = ['종목명', '종목코드']
    df.drop(columns=cols, inplace=True)

    df['거래량 (만주)'] = df['거래량']/10000
    df['거래대금 (억원)'] = df['거래대금']/100000000
    df['시가총액 (조원)'] = df['시가총액']/1000000000000
    df['배당수익률'] = df['배당수익률']/100

    cols = ['KEY',
            '날짜',
            '주가',
            '거래량 (만주)',
            '거래대금 (억원)',
            '시가총액 (조원)',
            'EPS',
            'PER',
            'BPS',
            'PBR',
            'DPS',
            '배당수익률']
    conn_db.to_(df[cols], 'data_from_krx', '전체취합본')

# 시가총액+PER+PBR 데이터셋 만들어놓기
@helper.timer
def union_stock_dataset():
    df_cap = conn_db.import_('시가총액_전체취합본')
    date_filt = df_cap['DATE'] >= '2019-01-01'
    df_cap = df_cap.loc[date_filt].reset_index(drop=True)
    df_cap.rename(columns={'DATE': '날짜'}, inplace=True)

    df_index = conn_db.import_('PER_PBR_배당수익률_전체취합본')
    date_filt = df_index['날짜'] >= '2019-01-01'
    df_index = df_index.loc[date_filt].reset_index(drop=True)

    cols = ['KEY', '날짜', 'close', 'open', 'high', 'low', '거래량',
            '거래대금', '시가총액', '상장주식수',
            'EPS', 'PER', 'BPS', 'PBR', 'DPS', '배당수익률']
    df = df_cap.merge(df_index, on=['KEY', '날짜'], how='left')[cols]
    conn_db.export_(df, '취합본_시가총액_PER')

# 전체 계산 실행
def calc_stock_market_data():
    days = 30
    daily_values(days)              # 최근 일별 추이
    daily_change(days)              # 최근 일별 상승률
    cum_change(days)                # 종목별 일별 누적상승률
    kospi_daily_chg(days)           # 코스피 일별 상승률
    chg_over_market()               # 일별 시장대비 상승률
    calc_kospi_cum_change(days)     # 코스피 일별 누적상승률
    cum_chg_over_market()           # 종목별 시장대비 누적상승률
    make_code_index_daily_df(days)  # PER_PBR_배당수익률 table 만들기
    make_last_date_indexes()        # 가장 최근날짜의 지표 모은 df 만들기
    union_stock_dataset()           # 시가총액+PER+PBR 데이터셋 만들어놓기
