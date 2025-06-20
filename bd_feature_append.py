import numpy as np
import pandas as pd
import statsmodels.api as sm
from scipy.stats import zscore
import pandas as pd
import numpy as np
import xgboost as xgb
import matplotlib.pyplot as plt
import seaborn as sns
# import plotly.graph_objects as go

from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score
from sklearn.metrics import confusion_matrix
from sklearn.metrics import f1_score
from sklearn.metrics import precision_score
from sklearn.metrics import recall_score
from sklearn.metrics import roc_curve
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import KBinsDiscretizer
import pandas as pd
from sqlalchemy import create_engine
import time
import numpy as np
import scipy.stats as stats
 
import json
from pandas import json_normalize
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine
from sklearn.metrics import roc_curve,roc_auc_score,RocCurveDisplay,make_scorer
from sklearn.model_selection import train_test_split
from sklearn.feature_selection import VarianceThreshold
import xgboost as xgb
from sklearn.preprocessing import OneHotEncoder
# import shap
import warnings
from datetime import datetime
warnings.filterwarnings('ignore')
pd.options.mode.chained_assignment=None
xgb.config_context(verbosity=0)

sns.set()
# shap.initjs()
pd.set_option('display.max_rows', 3000)
pd.set_option('display.max_columns', 500)
pd.set_option('display.max_colwidth', 500)

conn = create_engine('postgresql://BASIC$root:WCjak3$RQnf3ST@8.210.75.205:13343/test')
sql = '''select order_id,
        
        case when late_days > 0 then 1 else 0 end as late_day
        from bengal_test.bd_dwd_order
        where 
        user_type !=2 
        and status in (10,11) 
        and loan_completion_time >= '2025-04-01'
        -- and loan_completion_time  >=  '2023-01-01' and loan_completion_time < '2025-06-01'
        -- and  repay_time < current_date - 1 
          ;'''
df_y = pd.read_sql_query(sql, conn)
conn.dispose()
print(len(df_y))
order_id= 3004264069588582408
import re 
conn = create_engine('postgresql://BASIC$root:WCjak3$RQnf3ST@8.210.75.205:13343/test')
sql = '''select 
        id,order_id, borrow_time, created_time, content, type, phone,read,seen 
        from bengal_test.bd_user_sms    
        where order_id = '{}' and  seen != '2';'''.format(order_id)
sms = pd.read_sql_query(sql,conn)

sms['borrow_time'] = pd.to_datetime(sms['borrow_time'], errors='coerce')
sms['created_time'] = pd.to_datetime(sms['created_time'], errors='coerce')

sms_cnt= sms['content'].count()

bank_content= r'ebl|brac-bank|city bank|ucb.|pubali bank|ific bank|brac bank|trustbank|ucb|sonali bank|ibbl .|mtb.|ebl.|dmcb|islami.bank|dhaka bank|primebank|islami bank|ibbl|agrani bank|citybank|aibl|bank asia|islamibank|islamic|sebl cards|bank|bangladesh bank|sonali bank|agrani bank|rupali bank|janata bank|সোনালী ব্যাংক|অগ্রণী ব্যাংক|রূপালী ব্যাংক|জনতা ব্যাংক|dutch bangla bank|ডাচ-বাংলা ব্যাংক|brac bank|ব্র্যাক ব্যাংক|city bank|সিটি ব্যাংক|eastern bank ltd|ebl|ইস্টার্ন ব্যাংক|islami bank bangladesh|ব্যাংক|ইসলামী ব্যাংক বাংলাদেশ|standard chartered bangladesh|স্ট্যান্ডার্ড চার্টার্ড|hsbc bangladesh|এইচএসবিসি|grameen bank|গ্রামীণ ব্যাংক|brac microfinance|ব্র্যাক ক্ষুদ্রঋণ'
sms_in_bank_unique_phone = sms.loc[(sms.type==1) & (sms.content.str.contains(bank_content,case= False,na= False))]['content'].count()
trx_words = r'trxid|txnid'
trx_exclude_words  = r'reverse|fail|wait for|cancel|returned|processing'
credit_words = r'binimoy received fund|you have received|cash in received|money received|add money|cash in|b2b received|remittance received' 
bet_words = r'bet|casino|gambl|wager|slot|বাজি|ক্যাসিনো|জুয়া|বাজ'
debit_words = r'^payment tk|merchant payment|payment reversal|disbursement received|payment received|bkash to bank of|savings deposit|payment of|cash out received|payment to|send money|bill payment|b2b transfer|bill successfully paid|cash out|b2b transfer'
recharge_words  = r'recharge'


# sms['amount'] = 
# sms_in_d30_min_bkash_credit_amount = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(30,'D'))&(sms.content.str.contains(trx_words,case= False,na= False))& (~sms.content.str.contains(trx_exclude_words,case= False,na= False))&(sms.content.str.contains(credit_words,case= False,na= False))]['amount']
# sms_in_d3_max_credit_amount = 
# sms_in_d7_max_credit_amount = 
# sms_in_d30_max_bkash_debit_amount = 

# ((sms.content.str.contains(trx_words,case= False,na= False))& (~sms.content.str.contains(trx_exclude_words,case= False,na= False))&(sms['content'].str.extract(r'(?<=tk\s{0,5})[\d,]+(?:\.[\d]{2})?')[0].str.replace(',', '').astype(float))
sms_in_bkash_cnt= sms.loc[(sms.phone =='bKash')&(sms.type ==1)]['order_id'].count()
sms['balance']  = (sms['content'].str.extract(r'Balance Tk ([\d,]+(?:\.\d{2})?)', expand=False).str.replace(',', '', regex=False).astype(float))

sms_in_max_balance = np.max(sms.loc[(sms['type'] == 1) & (sms['balance'].notna())]['balance'])
sms_in_d14_max_balance = np.max(sms.loc[(sms['type'] == 1) & (sms['balance'].notna())]['balance'])
sms_in_d14_min_balance = np.min(sms.loc[(sms['type'] == 1) & (sms['balance'].notna())]['balance'])
# np.max(sms['content'].str.extract( r'balance[\s:tk\.]*([\d,]+(?:\.\d{2})?)', flags=re.IGNORECASE)[0].str.replace(',', '').astype(float))
# sms_in_d14_min_nagad_credit_amount = 

# sms_in_d7_max_bkash_credit_amount = 

sms_in_bet_cnt = sms.loc[(sms.type== 1)&(sms.content.str.contains(bet_words,case = False,na= False))&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(30,'D'))]['id'].count()
sms_in_d7_bkash_trx_cnt  = sms.loc[(sms.type ==1)&(sms.phone.str.contains('bKash',case = False, na = False))&(sms.content.str.contains(trx_words,case= False,na= False))& (~sms.content.str.contains(trx_exclude_words,case= False,na= False))]['id'].count()
sms_in_recharge_cnt = sms.loc[(sms.type == 1)&(sms.content.str.contains(recharge_words,case = False, na = False))]['id'].count()

sms_in_d30_recharge_cnt = sms.loc[(sms.type == 1)&(sms.content.str.contains(recharge_words,case = False, na = False))&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(30,'D'))]['id'].count()

sms_in_telcom_d7_unique_phone = sms.loc[(sms.type == 1)&((sms.content.str.contains('telecom',case =False, na= False))|(sms.phone.str.contains('telecom', case = False, na = False)))&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(7,'D'))]['phone'].nunique()

sms_in_d30_credit_cnt = sms.loc[(sms.type ==1)&(sms.content.str.contains(trx_words,case= False,na= False))&(~sms.content.str.contains(trx_exclude_words,case=False, na = False))&((sms.phone.str.contains('bKash', case = False, na = False))|(sms.phone.str.contains('nagad', case = False, na = False)))&(sms.content.str.contains(credit_words, case = False, na = False))&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(30,'D'))]['id'].count()
sms_in_d30_bkash_cnt = sms.loc[(sms.type == 1) &(sms.phone.str.contains('bkash', case= False, na = False))&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(30,'D'))]['id'].count()
# sms_in_bkash_debit_amount = 
sms_in_trx_cnt = sms.loc[(sms.type ==1)&((sms.phone.str.contains('bKash',case = False, na = False))|(sms.phone.str.contains('nagad',case= False, na= False)))&(sms.content.str.contains(trx_words,case= False,na= False))& (~sms.content.str.contains(trx_exclude_words,case= False,na= False))]['id'].count()
sms_in_d14_trx_cnt = sms.loc[(sms.type ==1)&((sms.phone.str.contains('bKash',case = False, na = False))|(sms.phone.str.contains('nagad',case= False, na= False)))&(sms.content.str.contains(trx_words,case= False,na= False))& (~sms.content.str.contains(trx_exclude_words,case= False,na= False))]['id'].count()


# sms_in_d14_max_credit_amount = 
# sms_in_min_bkash_credit_amount  =   
# sms_in_d30_bkash_credit_amount  = 
#  -1 nan 
sms_in_d0_min_balance = np.min(sms.loc[(sms.type ==1)&(sms['balance'].notna())&(sms.borrow_time.dt.date == sms.created_time.dt.date)]['balance'])
sms_in_telcom_d14_unique_phone = sms.loc[(sms.type == 1)&((sms.content.str.contains('telecom',case =False, na= False))|(sms.phone.str.contains('telecom', case = False, na = False)))&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(14,'D'))]['phone'].nunique()

sms_in_telcom_unique_phone = sms.loc[(sms.type == 1)&((sms.content.str.contains('telecom',case =False, na= False))|(sms.phone.str.contains('telecom', case = False, na = False)))]['phone'].nunique()



