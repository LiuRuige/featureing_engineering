import pandas as pd
sms_df = pd.read_csv('/Users/liuruige/Downloads/app_cate.txt' , sep='\t')
sms_df.head(1)
conn = create_engine('postgresql://BASIC$root:WCjak3$RQnf3ST@8.210.75.205:13343/test')
sql = '''select 
order_id,collect_app_name,app_type,collect_app_package,borrow_time, in_time,up_time
from    
bengal_test.bd_user_app_info 
where order_id = '{}';'''.format(order_id)

applist = pd.read_sql_query(sql,conn)
applist['borrow_time'] = pd.to_datetime(applist['borrow_time'], errors='coerce')
applist['in_time'] = pd.to_datetime(applist['in_time'], errors='coerce')
applist['up_time'] = pd.to_datetime(applist['up_time'], errors='coerce')
applist = applist.dropna(subset=['borrow_time', 'in_time','up_time'])

applist_new = pd.merge(applist,sms_df,how = 'left', on= 'app_name')

# # 提取分类对应的 app 列表和包名列表
# def extract_app_lists_by_category(df):
#     result = {}
#     for cat in sorted(df['category'].dropna().unique()):
#         group = df[df['category'] == cat]
#         result[cat] = {
#             'package_list': group['package_name'].tolist(),
#             'app_list': group['app_name'].tolist()
#         }
#     return result

# # 应用函数
# grouped_apps = extract_app_lists_by_category(sms_df)
# grouped_apps 
# # 例如访问 work 分类
# print("work_package_list:", grouped_apps.get('work', {}).get('package_list', []))
# print("work_app_list:", grouped_apps.get('work', {}).get('app_list', []))
# print("travel_package_list:", grouped_apps.get('travel', {}).get('package_list', []))
# print("game_app_list:", grouped_apps.get('game', {}).get('app_list', []))
