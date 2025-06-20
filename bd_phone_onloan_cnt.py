# phone_onloan_cnt 
# phone_first_disburse_date_gap 
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import pymysql
import warnings, json 
warnings.filterwarnings('ignore') 
import traceback 

conn = create_engine('postgresql://BASIC$root:WCjak3$RQnf3ST@hgprecn-cn-5yd39h015004-cn-hongkong-vpc-st.hologres.aliyuncs.com:80/test')

sql = '''select 
order_id,borrow_time, case when late_days >0 then 1 else 0 end as y 
from bengal_test.bd_dwd_order 
where 
loan_completion_time >= '2023-01-01' and loan_completion_time < '2023-07-01'

and status in (8, 10, 11) and loan_completion_time != ''
;''' 
order = pd.read_sql_query(sql,conn)
len(order) 

def bd_onloan_variable(order_id):
    conn = create_engine('postgresql://BASIC$root:WCjak3$RQnf3ST@hgprecn-cn-5yd39h015004-cn-hongkong-vpc-st.hologres.aliyuncs.com:80/test')

    sql = '''select 
    t1.order_id, 
    uniq(case when t2.order_id != t1.order_id then t2.order_id end ) as phone_onloan_cnt 

    from bengal_test.bd_dwd_order t1 left join bengal_test.bd_dwd_order t2 on t1.user_phone = t2.user_phone 
    where 
    t2.borrow_time <= t1.borrow_time  and 
    t2.repay_yes_time >= t1.borrow_time 

    and 
    t1.order_id = '{}' 
    and t2.status in (8,10, 11)  
    group by 1 
    ;'''.format(order_id) 
    phone_onloan_cnt_df = pd.read_sql_query(sql, conn) 
    if phone_onloan_cnt_df.empty:
        phone_onloan_cnt = -1 
    else:
        phone_onloan_cnt = phone_onloan_cnt_df['phone_onloan_cnt'].item()
    conn.dispose() 
 
    conn = create_engine('postgresql://BASIC$root:WCjak3$RQnf3ST@hgprecn-cn-5yd39h015004-cn-hongkong-vpc-st.hologres.aliyuncs.com:80/test')

    sql = '''
    select 
    t1.order_id,  
    date(t1.borrow_time) - date(t2.loan_completion_time) as phone_first_disburse_date_gap 

    from bengal_test.bd_dwd_order t1 left join(
    select 
    user_phone, row_number() over ( partition by user_phone order by loan_completion_time asc) as rn , loan_completion_time 
    from bengal_test.bd_dwd_order 
    )   t2 on t1.user_phone = t2.user_phone 

    where t2.rn = 1 
    and t1.order_id ='{}'
    ;'''.format(order.order_id)
    phone_first_disburse_date_gap_df = pd.read_sql_query(sql, conn) 
    if phone_first_disburse_date_gap_df.empty:
        phone_first_disburse_date_gap = -1 
    else:
        phone_first_disburse_date_gap = phone_first_disburse_date_gap_df['phone_first_disburse_date_gap'].item()
    conn.dispose() 

    data = { 'order_id': order_id, 'phone_onloan_cnt':phone_onloan_cnt, 'phone_first_disburse_date_gap':phone_first_disburse_date_gap
    }

    df= pd.DataFrame([data])
    df.fillna(-1, inplace= True)
    return df 



conn = create_engine('postgresql://BASIC$root:WCjak3$RQnf3ST@hgprecn-cn-5yd39h015004-cn-hongkong-vpc-st.hologres.aliyuncs.com:80/test')

sql = '''select 
order_id,borrow_time, case when late_days >0 then 1 else 0 end as y 
from bengal_test.bd_dwd_order 
where 
loan_completion_time >= '2023-01-01' and loan_completion_time < '2025-05-22'

and status in (8, 10, 11) and loan_completion_time != ''
order by loan_completion_time desc 
;''' 
order = pd.read_sql_query(sql,conn)
len(order) 
    

# 初始化结果DataFrame
output_df = pd.DataFrame()

if order.empty:
    pass
else:
    # 设置分批处理
    batch_size = 100
    order_ids = order['order_id'].tolist()
    num_batches = (len(order_ids) + batch_size - 1) // batch_size

    # 记录报错的订单号
    error_orders_onloan = []

    # 分批处理订单
    for i in range(num_batches):
        batch_order_ids = order_ids[i * batch_size: (i + 1) * batch_size]
        batch_results = []  # 存储当前批次的结果

        for oid in batch_order_ids:
            try:
                # 计算bd_onloan特征
                temp_df = bd_onloan_variable(oid)
                batch_results.append(temp_df)
            except Exception as e:
                print(f"\n❌ [bd_onloan特征计算失败] order_id: {oid}")
                print("错误详情如下：")
                traceback.print_exc()
                error_orders_onloan.append(oid)

        # 合并当前批次的结果
        if batch_results:
            batch_df = pd.concat(batch_results, ignore_index=True)
            output_df = pd.concat([output_df, batch_df], ignore_index=True)
            
conn = create_engine('postgresql://BASIC$root:WCjak3$RQnf3ST@hgprecn-cn-5yd39h015004-cn-hongkong-vpc-st.hologres.aliyuncs.com:80/test')
# conn = create_engine('postgresql://BASIC$root:WCjak3$RQnf3ST@8.210.75.205:13343/test')
output_df.to_sql('t_rca_bd_onloan_feature_record', conn, if_exists='append', index=False, schema='bengal_test')
conn.dispose()
 