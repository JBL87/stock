import pandas as pd
from glob import glob
import helper
import conn_db

folder_naver = conn_db.get_path('folder_naver_fs')

# 컬럼 뒤에 추가할 suffix 정의
expect_suffix = ' (E)' # 전망인 경우 컬럼 뒤에 추가
y_suffix = '_Y' # 연간인 경우 컬럼 뒤에 추가
ly_suffix = '_LY' # 전년 연간의 경우 컬럼 뒤에 추가
q_suffix = '_Q' # 분기인 경우 컬럼 뒤에 추가
lq_suffix = '_LQ' # 전분기인 경우 컬럼 뒤에 추가

# 주기, 실적/전망 별로 전처리
def _get_last_row(df):
    # 종목별로 가장 최근 날짜만 남기기
    df = df.groupby(['KEY'], as_index=False).head(1).copy()
    values_cols = df.columns.tolist()[4:-1] # 측정값만 있는 컬럼 list
    # 주기에 따라 컬럼명 뒤에 _Y / _Q 추가
    if df['주기'].unique().tolist()[0] == '연간':
        [df.rename(columns={col: col + y_suffix}, inplace=True) for col in values_cols]
    else:  # 분기의 경우 주당배당금이 없어서 삭제
        [df.drop(columns=[col], inplace=True) for col in df.columns.tolist() if "주당배당금"in col]
        [df.rename(columns={col: col + q_suffix}, inplace=True) for col in values_cols]

    # 위에서 컬럼명이 수정되었기 때문에 value_cols다시 정의
    values_cols = df.columns.tolist()[4:-1]
    # '전망'인 경우 컬럼명 뒤에 '(E)' 추가
    if df['실적/전망'].unique().tolist()[0] == '전망':
        [df.rename(columns={col: col + expect_suffix}, inplace=True) for col in values_cols]
    # 측정값 이외의 컬럼 삭제
    df.drop(columns=['실적/전망', '날짜', '재무제표기준', '주기'], inplace=True)
    return df.reset_index(drop=True)

# 전년도 연간/분기 실적 전처리 하는 함수
def _get_prior_row(df):
    # 올해 - KEY를 기준으로 가장 위에만 keep
    df_this = df.groupby(['KEY'], as_index=False).head(1)
    # 전년 - KEY를 기준으로 가장 위에2개만 keep한 다음에 다시 KEY를 기준으로 가장 마지막 keep
    df_last = df.groupby(['KEY'], as_index=False).head(2).groupby(['KEY'], as_index=False).tail(1)
    # 전년도 실적이 없는 경우가 있음.
    # 그래서 올해 실적 df를 concat할때 2번 넣어서 중복삭제로 모두 없앤다
    df = pd.concat([df_last, df_this, df_this]).drop_duplicates(keep=False)

    values_cols = df.columns.tolist()[4:-1]  # 측정값만 있는 컬럼 list
    if df['주기'].unique().tolist()[0] == '연간':
        [df.rename(columns={col: col + ly_suffix}, inplace=True) for col in values_cols]
    else: # 분기의 경우 주당배당금이 없어서 삭제
        [df.drop(columns=[col], inplace=True) for col in df.columns.tolist() if "주당배당금"in col]
        [df.rename(columns={col: col + lq_suffix}, inplace=True) for col in values_cols]
    # 측정값 이외의 컬럼 삭제
    df.drop(columns=['실적/전망', '날짜', '재무제표기준', '주기'], inplace=True)
    return df.reset_index(drop=True)

# 네이버증권 기업실적분석표 전처리
@helper.timer
def clean_naver_fs():
    code_list = conn_db.from_("DB_기업정보", 'FS_update_list')['종목코드']

    # 취합본 불러와서 공통부분 전처리
    df = conn_db.import_('fs_from_naver_raw')

    # DB_기업정보 FS_update_list에 있는 코드만 필터링하고 KEY컬럼 만들기
    df = df[df['종목코드'].isin(code_list)].copy()
    df = helper.make_keycode(df).drop(columns=['종목명','종목코드'])

    # 비율인 컬럼 100으로 나눠주기
    matcher = ['ROE', '률', '율', '배당성향']
    all_cols = df.columns.tolist()
    prcnt_cols = [col for col in all_cols if any(prcnt in col for prcnt in matcher)]
    for col in prcnt_cols:
        df[col] = df[col]/100

    # 정렬
    cols = ['KEY','날짜', '주기']
    df = df.sort_values(by=cols, ascending=False)

    # 기업정보랑 합쳐서 저장
    df_all = df.merge(conn_db.from_("DB_기업정보", '취합본'), on='KEY', how='left')

    # 새로 합쳐진 것 저장_시계열용
    conn_db.to_(df_all, 'from_naver증권', 'naver_final')
    df_all.to_pickle(folder_naver + "fs_from_naver_final.pkl")

    print('네이버증권 기업실적분석표+기업정보취합본 merge후 pickle 저장완료')

    # 회사별 주재무제표만 정리
    cols = ['재무제표기준','KEY']
    temp = df[cols].drop_duplicates(subset=['KEY']).reset_index(drop=True)
    conn_db.to_(temp,'DB_기업정보','종목별_주재무제표')

    # 최근 연간/분기 실적/전망만 정리
    # 컬럼명에 '(' 부분 삭제
    [df.rename(columns={col: col.split('(')[0]},
                inplace=True) for col in df.columns.tolist() if "("in col]

    filt_q = df['주기'] != '연간'
    filt_y = df['주기'] == '연간'
    filt_expect = df['실적/전망'] != '실적'
    filt_real = df['실적/전망'] == '실적'

    # 사용안하는 컬럼 삭제
    # for x in ['부채비율','당좌비율','유보율','시가배당률','배당성향']:
    for x in ['유보율','시가배당률','배당성향']:
        if x in df.columns.tolist():
            df.drop(columns=x, inplace=True)
        else:
            pass

    # 연간 df
    df_y_expect = _get_last_row(df.loc[filt_y & filt_expect].copy()) # 연간 전망
    df_y_real = _get_last_row(df.loc[filt_y & filt_real].copy()) # 연간 실적
    df_y_real_last = _get_prior_row(df.loc[filt_y & filt_real].copy()) # 전년 실적
    df_y = df_y_real.merge(df_y_real_last, on='KEY', how='left').merge(df_y_expect, on='KEY', how='left')

    # 분기 df
    df_q_expect = _get_last_row(df.loc[filt_q & filt_expect].copy()) # 분기 전망
    df_q_real = _get_last_row(df.loc[filt_q & filt_real].copy()) # 분기 실적
    df_q_real_last = _get_prior_row(df.loc[filt_q & filt_real].copy()) # 분기 실적
    df_q = df_q_real.merge(df_q_real_last, on='KEY', how='left').merge(df_q_expect, on='KEY', how='left')

    # 전체 합치기 + 'KEY'컬럼을 맨 앞으로 한 다음 저장
    df_all = df_y.merge(df_q, on='KEY', how='outer')
    cols = ['KEY'] + [col for col in df_all if col != 'KEY']
    df_all.dropna(axis=1, how='all', inplace=True)

    conn_db.to_(df_all, 'from_naver증권', 'naver_최근값만')
    df_all.to_pickle(folder_naver + "fs_from_naver_최근값만.pkl")