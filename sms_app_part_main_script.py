import math
# import scorecardpy as sc 
import pandas as pd
from sqlalchemy import create_engine
import time
import numpy as np
import datetime
from dateutil.relativedelta import relativedelta
import os, json 
from sqlalchemy import create_engine
from openpyxl import Workbook
from openpyxl.utils import get_column_letter
import pymysql
import pytz,re
import traceback
from applist_variable import applist_variable
from sms_variable import sms_variable
import warnings
warnings.filterwarnings('ignore')


conn = create_engine('postgresql://BASIC$root:WCjak3$RQnf3ST@hgprecn-cn-5yd39h015004-cn-hongkong-vpc-st.hologres.aliyuncs.com:80/test')
import pandas as pd
import json
import traceback
from sqlalchemy import create_engine
from tqdm import tqdm  

# 加载数据
conn_1 = create_engine('postgresql://BASIC$root:WCjak3$RQnf3ST@hgprecn-cn-5yd39h015004-cn-hongkong-vpc-st.hologres.aliyuncs.com:80/test')
sql = '''select order_id from bengal_test.t_rca_bd_part_feature_record where order_id is not null ;'''
bd_feature  = pd.read_sql_query(sql,conn_1)


conn = create_engine('postgresql://BASIC$root:WCjak3$RQnf3ST@hgprecn-cn-5yd39h015004-cn-hongkong-vpc-st.hologres.aliyuncs.com:80/test')
sql = '''
select 
    order_id,
    borrow_time,
    case when late_days > 0 then 1 else 0 end as y  
from bengal_test.bd_dwd_order  
where loan_completion_time >= '2023-01-01' 
  and loan_completion_time < '2023-10-01'  
  and status in (8, 10, 11) 
  and loan_completion_time != '';''' 


order = pd.read_sql_query(sql, conn)
order_ids = order['order_id'].tolist()
num_total = len(order_ids)
print(f"📦 总订单数：{num_total}")

# 初始化空表
sms_df = pd.DataFrame()
applist_df = pd.DataFrame()

# 设置分批
batch_size = 200
num_batches = (num_total + batch_size - 1) // batch_size

# 错误记录
error_orders_sms = []
error_orders_applist = []

 
for i in tqdm(range(num_batches), desc="📊 分批处理进度", unit="batch"):
    batch_order_ids = order_ids[i * batch_size: (i + 1) * batch_size]
    batch_sms_list = []
    batch_applist_list = []

    for j, oid in enumerate(batch_order_ids):
        index_in_total = i * batch_size + j + 1
        print(f"➡️ 正在处理 order_id: {oid}（第 {index_in_total} 条 / 共 {num_total} 条）")

        # SMS 特征
        try:
            df_sms = sms_variable(oid)
            batch_sms_list.append(df_sms)
        except Exception as e:
            print(f"❌ [SMS 特征失败] order_id: {oid}")
            traceback.print_exc()
            error_orders_sms.append(oid)

        # AppList 特征
        try:
            df_app = applist_variable(oid)
            batch_applist_list.append(df_app)
        except Exception as e:
            print(f"❌ [AppList 特征失败] order_id: {oid}")
            traceback.print_exc()
            error_orders_applist.append(oid)

    if batch_sms_list:
        sms_df = pd.concat([sms_df] + batch_sms_list, ignore_index=True)
    if batch_applist_list:
        applist_df = pd.concat([applist_df] + batch_applist_list, ignore_index=True)

 
if not sms_df.empty:
    json_cols = sms_df.columns.tolist()
    sms_df['sms_feature'] = sms_df[json_cols].apply(lambda row: json.dumps(row.to_dict(), ensure_ascii=False), axis=1)
    output_df_1 = sms_df[['order_id', 'sms_feature']]
else:
    output_df_1 = pd.DataFrame(columns=['order_id', 'sms_feature'])

if not applist_df.empty:
    json_cols = applist_df.columns.tolist()
    applist_df['applist_feature'] = applist_df[json_cols].apply(lambda row: json.dumps(row.to_dict(), ensure_ascii=False), axis=1)
    output_df_2 = applist_df[['order_id', 'applist_feature']]
else:
    output_df_2 = pd.DataFrame(columns=['order_id', 'applist_feature'])

 
output_df = pd.merge(output_df_1, output_df_2, on='order_id')
output_df['country'] = 'BD'
output_df[['create_time']] = order[['borrow_time']]
 

conn = create_engine('postgresql://BASIC$root:WCjak3$RQnf3ST@hgprecn-cn-5yd39h015004-cn-hongkong-vpc-st.hologres.aliyuncs.com:80/test')
output_df.to_sql('t_rca_bd_part_feature_record', conn, if_exists='append', index=False, schema='bengal_test')
conn.dispose()
 
