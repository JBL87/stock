import pandas as pd
import requests
from bs4 import BeautifulSoup
import concurrent.futures
import time
from glob import glob
import helper
import conn_db
import stock_itooza_cleaner, stock_info_cleaner

user_agent = helper.user_agent
max_workers = 3
code_list = conn_db.from_("DB_기업정보", 'FS_update_list')[['종목코드','종목명']]
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

folder_itooza = conn_db.get_path('folder_itooza')
folder_itooza_backup = conn_db.get_path('folder_itooza_backup')


# 아이투자 투자지표
def _get_table_from_itooza(param): # 5개년 주요 투자지표 및 최근것까지 반영된 투자지표 가져오기
    global df_tables
    global df_5yr
    global df_short

    time.sleep(1)
    code, co = param
    url = f'http://search.itooza.com/search.htm?seName={code}'
    r = requests.get(url, headers={'User-Agent': user_agent})
    # r.encoding = r.apparent_encoding # 원래 이거였는데 encoding방식이 변경됨?
    r.encoding ='euc-kr'
    dom = BeautifulSoup(r.text, "html.parser")

    #-------------------------------------------------------------------------------
    # 페이지 아래쪽에 있는 테이블3개
    report_types = ['indexTable1', 'indexTable2', 'indexTable3']
    report_type_name= {'indexTable1':'연환산',
                       'indexTable2':'연간',
                       'indexTable3' : '분기'}
    df_all = pd.DataFrame()
    for report_type in report_types:
        try:
            data_table = dom.select(f'#{report_type} > table')[0]
            # 지표 header
            item_col = data_table.select('tbody > tr > th')
            df = pd.DataFrame([x.text.strip() for x in item_col])
            # 날짜 header
            date_cols = data_table.select('thead > tr > th')
            date_cols = [x.text.strip() for x in date_cols]
            date_cols = [data.replace('.','년') for data in date_cols]
            # 데이터 값
            data_values = data_table.select('tbody > tr > td')
            data_values = [x.text.strip() for x in data_values]

            lap = len(date_cols)-1
            for i in range(lap):
                temp_df = pd.DataFrame(data_values[i::lap]) # 컬럼의 갯수(lap)만큼 매n번째마다 합쳐줌
                df = pd.concat([df, temp_df], axis=1)
            df.columns = date_cols
            df = df.melt(id_vars='투자지표', var_name='날짜', value_name='값')
            df = df.loc[~(df['값']=='N/A')]
            df.rename(columns={'투자지표':'항목'}, inplace=True)
            df.insert(loc=0, column='기준', value=report_type_name[report_type])
            df_all = df_all.append(df)
            time.sleep(1)
        except:
            pass

    if len(df_all)>0:
        df_all['종목코드'] = code
        df_tables = df_tables.append(df_all)
        # print(f'{param} 전체 테이블 가져오기 완료')
    else:
        print(f'{param} 전체 테이블 가져오기 실패')
    #-------------------------------------------------------------------------------
    # 5년평균
    try:
        all_data = dom.select('#stockItem > div.item-body > div.ar > div.item-data2')[0]
        df_fiveyr = pd.DataFrame([x.text.strip() for x in all_data.select('td')]).T
        headers = [x.text.strip() for x in all_data.select('th')]
        df_fiveyr.columns = headers
        df_fiveyr.insert(loc=0, column='종목명', value=co)
        df_fiveyr.insert(loc=0, column='종목코드', value=code)
        # memo = dom.select('#stockItem > div.item-body > div.ar > div.item-data2 > p')[0].text.split('* ')[1]
        # df_fiveyr['비고'] = memo
        for col in df_fiveyr.columns.tolist():
            df_fiveyr[col] = df_fiveyr[col].str.replace('\(-\)','')
            df_fiveyr[col] = df_fiveyr[col].str.replace('N/A','')
        df_5yr = df_5yr.append(df_fiveyr)
    except:
        print(f'{param} 5개년 지표 가져오기 실패')
    
    #-------------------------------------------------------------------------------
    # 최근 지표 가져오기
    try:
        all_data = dom.select('#stockItem > div.item-body > div.ar > div.item-data1')[0]
        df = pd.DataFrame([x.text.strip() for x in all_data.select('td')]).T
        headers = [x.text.strip() for x in all_data.select('th')]
        df.columns = headers
        fix_header = 'ROE = ROS * S/A * A/E'
        roe_all = df[fix_header].tolist()[0]
        #---------------------------
        # A/E
        AE = roe_all.split('=')[1].split('*')[2].strip('(')[:-1]
        df.insert(loc=2, column='A/E',value= AE)
        # S/A
        SA = roe_all.split('=')[1].split('*')[1].strip('(')[:-1]
        df.insert(loc=2, column='S/A',value= SA)
        # ROS
        ROS = roe_all.split('=')[1].split('*')[0].strip('(')[:-1]
        df.insert(loc=2, column='ROS',value= ROS)
        # ROE
        ROE = roe_all.split('=')[0]# ROE
        df.insert(loc=2, column='ROE', value=ROE)
        #---------------------------
        df.drop(columns=fix_header, inplace=True)
        df.insert(loc=0, column='종목명', value=co)
        df.insert(loc=0, column='종목코드', value=code)
        for col in df.columns.tolist():
            df[col] = df[col].str.replace('\(-\)','')
            df[col] = df[col].str.replace('N/A','')
        # memo = dom.select('#stockItem > div.item-body > div.ar > div.item-detail > p.table-desc')[0].text.split('* ')[1]
        # df['비고'] = memo
        df_short = df_short.append(df)
    except:
        print(f'{param} 최근 지표 가져오기 실패')
    print(f'{co} 가져오기 완료')

# 5개년 주요 투자지표 alc 전체 투자지표 업데이트
@helper.timer
def update_itooza_fsratio(param='all'): # 5개년 주요 투자지표 업데이트
    global code_list
    # global max_workers
    global df_tables
    global df_5yr
    global df_short

    # 아이투자 투자지표 업데이트
    if param !='all': # 추가분만 업데이트할 때, 업데이트할 내역이 있는지 확인
        new = code_list['종목코드'].unique().tolist()
        old = conn_db.from_("DB_기업정보", 'from_아이투자_기업정보')['종목코드'].unique().tolist()
        new_code = list(set(new) - set(old))

        if len(new_code)>0:
            code_list_temp = code_list[code_list['종목코드'].isin(new_code)]
            code_list_temp = code_list_temp.values.tolist() 

            for param in code_list_temp:
                _get_table_from_itooza(param) 
            del code_list_temp
        else:
            print('업데이트할 내역 없음') 

    else: # 전체 업데이트
        code_list_temp = code_list.values.tolist()
        
        for param in code_list_temp:
                _get_table_from_itooza(param)
        # with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        #     executor.map(_get_table_from_itooza, code_list_temp)
        
        print('(1차 가져오기 완료')
        print('누락된거 확인후 다시 시도')

        done_code = df_tables['종목코드'].unique().tolist()
        new = code_list['종목코드']
        new_code = list(set(new) - set(done_code))
        
        if len(new_code)>0: 
            code_list_temp = code_list[code_list['종목코드'].isin(new_code)]
            code_list_temp = code_list_temp.values.tolist()
            [_get_table_from_itooza(param) for param in code_list_temp]
            del code_list_temp, new, new_code, done_code
        else:
            print('누락된거 없음. 전처리 시작')
            pass

    # 테이블에 있는 지표 정리
    if len(df_tables)>0:
        stock_itooza_cleaner.clean_index_table(df_tables)
        del df_tables 

    # 5개년 주요 투자지표 정리
    if len(df_5yr)>0:
        stock_itooza_cleaner.clean_5yr_and_recent_index(df_5yr, '장기투자지표_5개년')
        del df_5yr

    # 최근 지표 요약본 정리
    if len(df_short)>0:
        stock_itooza_cleaner.clean_5yr_and_recent_index(df_short, '최근지표요약')
        del df_short
    stock_info_cleaner.merge_all_fs()

#아이투자 기업정보
def _get_itooza_company_description(code): 
    time.sleep(1)
    url = f"http://search.itooza.com/search.htm?seName={code}&jl=k&search_ck=&sm=&sd=&ed=&ds_de=&page=&cpv="
    try:
        r = requests.get(url, headers={'User-Agent': user_agent}, timeout=5)
        r.encoding ='euc-kr'

        # 전체 df 만들기
        dom = BeautifulSoup(r.text, "html.parser")
        title = dom.select('#content > div.box120903 > div.ainfo_com > div > table > tr > th')
        title = [x.text.replace('\r', '').replace('\t', '').replace('\n', '-') for x in title]
        content = dom.select('#content > div.box120903 > div.ainfo_com > div > table > tr > td')
        content = [x.text.strip() for x in content]

        df = pd.DataFrame([title, content]).T
        df[['구분', '기준날짜']] = df[0].str.split('-', expand=True)
        df['기준날짜'] = df['기준날짜'].astype(str).str.replace('.', '/')
        df = df.drop(columns=0, axis=1).rename(columns={1: '내용'})

        df['종목코드'] = code
        cols = ['종목코드', '구분', '내용', '기준날짜']

        print(f'{code} 완료')
        return df[cols]
    except requests.exceptions.Timeout:
        print(f'{code} 실패')
        pass

    # #제품 df ----------- ----------- ----------- ----------- ----------- -----------
    # products = []
    # for x in content[2].split(':'):
    #     products.append(x.split('-')[0])
    # products = ' '.join(products).replace(' 등 ', ', ').replace('* 괄호 안은 순매출액 비중', '').strip()
    # products = products.replace('* 괄호 안은 매출 비중','').strip()
    # products = products.replace('  ', ' ')
    # # 제품 가로형
    # product_1 = pd.DataFrame([products]).rename(columns={0: '제품'})
    # product_1['종목코드'] = code
    # product_1_df = product_1_df.append(product_1)
    # # 제품 세로형
    # product_2 = pd.DataFrame(products[:-1].split(', ')).rename(columns={0: '제품'})
    # product_2['종목코드'] = code
    # product_2_df = product_2_df.append(product_2)
    # #원재료 df ----------- ----------- ----------- ----------- ----------- -----------
    # raw_materials = []
    # for x in content[3].split('-'):
    #     try:
    #         raw_materials.append(x.split('(')[0].split(' ', 1)[1].strip())
    #     except:
    #         pass
    # # 원재료 가로형
    # raw_material_1 = pd.DataFrame([', '.join(raw_materials)]).rename(columns={0: '원재료'})
    # raw_material_1['종목코드'] = code
    # raw_material_1_df = raw_material_1_df.append(raw_material_1)
    # # 원재료 세로형
    # raw_material_2 = pd.DataFrame(raw_materials).rename(columns={0: '원재료'})
    # raw_material_2['종목코드'] = code
    # raw_material_2_df = raw_material_2_df.append(raw_material_2)
    #------------------------------------------------------------------------------------------------------------------------

@helper.timer
def update_itooza_company_description(param='all'): 
    df = pd.DataFrame()
    # global raw_material_1_df   # 원재료_가로형
    # global raw_material_2_df  # 원재료_세로형
    # global product_1_df  # 제품_가로형
    # global product_2_df  # 제품_세로형

    # with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
    #     executor.map(_get_itooza_company_description, code_list)
    if param !='all':
        new = code_list['종목코드']
        old = conn_db.from_("DB_기업정보", 'from_아이투자_기업정보')['종목코드']
        new_code_list = list(set(new) - set(old))

        if len(new_code_list)>0:
            df = pd.concat([_get_itooza_company_description(code) for code in new_code_list])
        else:
            print('업데이트할 내역 없음')

    else:
        for code in code_list['종목코드']:
            try:
                df = df.append(_get_itooza_company_description(code), ignore_index=True)
            except:
                try:
                    df = df.append(_get_itooza_company_description(
                        code), ignore_index=True)
                except:
                    pass
    if len(df)>0:
        df.reset_index(drop=True, inplace=True)
        conn_db.to_(df,'from_아이투자','기업정보_최근update')
        
        # cleaner 실행
        stock_itooza_cleaner.clean_itooza_company_description(df)
        stock_info_cleaner.make_master_co_info()
        stock_info_cleaner.co_info_to_dash()
    else:
        print('업데이트할 내역 없음')
''
