import pandas as pd
sms_df = pd.read_csv('/Users/liuruige/Downloads/app_cate.txt' , sep='\t')
sms_df.head(1)

# 提取分类对应的 app 列表和包名列表
def extract_app_lists_by_category(df):
    result = {}
    for cat in sorted(df['category'].dropna().unique()):
        group = df[df['category'] == cat]
        result[cat] = {
            'package_list': group['package_name'].tolist(),
            'app_list': group['app_name'].tolist()
        }
    return result

# 应用函数
grouped_apps = extract_app_lists_by_category(sms_df)
grouped_apps 
# 例如访问 work 分类
print("work_package_list:", grouped_apps.get('work', {}).get('package_list', []))
print("work_app_list:", grouped_apps.get('work', {}).get('app_list', []))
print("travel_package_list:", grouped_apps.get('travel', {}).get('package_list', []))
print("game_app_list:", grouped_apps.get('game', {}).get('app_list', []))
