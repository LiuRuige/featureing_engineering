# order_id= 3004264069588582408
 
# import re 
def sms_variable(order_id):
    
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
    trx_words = r'trxid|txnid'
    trx_exclude_words  = r'reverse|fail|wait for|cancel|returned|processing'
    credit_words = r'binimoy received fund|you have received|cash in received|money received|add money|cash in|b2b received|remittance received' 
    bet_words = r'bet|casino|gambl|wager|slot|বাজি|ক্যাসিনো|জুয়া|বাজcredit_wordscredit_Wordscreecrrcredit_words       '
    debit_words = r'^payment tk|merchant payment|payment reversal|disbursement received|payment received|bkash to bank of|savings deposit|payment of|cash out received|payment to|send money|bill payment|b2b transfer|bill successfully paid|cash out|b2b transfer'
    recharge_words  = r'recharge'
    otp_words = r'otp|verification|one time pin|one time code|one time password'
    loan_words = r'loan|ধার'
    sms['amount'] = sms['content'].str.extract(r'(?:tk|Tk)\s*([\d,]+(?:\.\d{1,2})?)', expand=False).str.replace(',', '', regex=False).replace('', pd.NA).astype(float).astype('Int64')

    sms_in_max_debit_amount = np.max(sms.loc[(sms.type==1)&((sms.phone.str.contains('bKash',case = False, na = False))|(sms.phone.str.contains('nagad',case = False, na = False)))&(sms.content.str.contains(debit_words,case= False,na= False))&(sms.content.str.contains(trx_words,case= False,na= False))& (~sms.content.str.contains(trx_exclude_words,case= False,na= False))]['amount'])
    sms_in_debit_amount= sms.loc[(sms.type==1)&((sms.phone.str.contains('bKash',case = False, na = False))|(sms.phone.str.contains('nagad',case = False, na = False)))&(sms.content.str.contains(debit_words,case= False,na= False))]['amount'].sum()
    sms_in_d0_debit_amount= sms.loc[(sms.type==1)&((sms.phone.str.contains('bKash',case = False, na = False))|(sms.phone.str.contains('nagad',case = False, na = False)))&(sms.content.str.contains(debit_words,case= False,na= False))&(sms.borrow_time.dt.date == sms.created_time.dt.date)]['amount'].sum()
    sms_in_d3_debit_amount= sms.loc[(sms.type==1)&((sms.phone.str.contains('bKash',case = False, na = False))|(sms.phone.str.contains('nagad',case = False, na = False)))&(sms.content.str.contains(debit_words,case= False,na= False))&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(3,'D'))]['amount'].sum()
    sms_in_d7_debit_amount= sms.loc[(sms.type==1)&((sms.phone.str.contains('bKash',case = False, na = False))|(sms.phone.str.contains('nagad',case = False, na = False)))&(sms.content.str.contains(debit_words,case= False,na= False))&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(7,'D'))]['amount'].sum()
    sms_in_max_d0_debit_amount = np.max(sms.loc[(sms.type==1)&((sms.phone.str.contains('bKash',case = False, na = False))|(sms.phone.str.contains('nagad',case = False, na = False)))&(sms.content.str.contains(debit_words,case= False,na= False))&(sms.borrow_time.dt.date == sms.created_time.dt.date)]['amount'].sum())
    sms_in_max_d7_debit_amount = np.max(sms.loc[(sms.type==1)&((sms.phone.str.contains('bKash',case = False, na = False))|(sms.phone.str.contains('nagad',case = False, na = False)))&(sms.content.str.contains(debit_words,case= False,na= False))&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(7,'D'))]['amount'].sum())

    sms_in_credit_amount = sms.loc[(sms.type ==1)&((sms.phone.str.contains('bKash',case = False, na = False))|(sms.phone.str.contains('nagad',case = False, na = False)))&(sms.content.str.contains(trx_words,case= False,na= False))& (~sms.content.str.contains(trx_exclude_words,case= False,na= False))&(sms.content.str.contains(credit_words,case= False,na= False))]['amount'].sum()
    sms_in_d14_credit_amount = sms.loc[(sms.type ==1)&((sms.phone.str.contains('bKash',case = False, na = False))|(sms.phone.str.contains('nagad',case = False, na = False)))&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(14,'D'))&(sms.content.str.contains(trx_words,case= False,na= False))& (~sms.content.str.contains(trx_exclude_words,case= False,na= False))&(sms.content.str.contains(credit_words,case= False,na= False))]['amount'].sum()
    sms_in_max_credit_amount = np.max(sms.loc[(sms.type ==1)&((sms.phone.str.contains('bKash',case = False, na = False))|(sms.phone.str.contains('nagad',case = False, na = False)))&(sms.content.str.contains(trx_words,case= False,na= False))& (~sms.content.str.contains(trx_exclude_words,case= False,na= False))&(sms.content.str.contains(credit_words,case= False,na= False))]['amount'].sum())
    sms_in_max_d3_credit_amount = np.max(sms.loc[(sms.type ==1)&((sms.phone.str.contains('bKash',case = False, na = False))|(sms.phone.str.contains('nagad',case = False, na = False)))&(sms.content.str.contains(trx_words,case= False,na= False))& (~sms.content.str.contains(trx_exclude_words,case= False,na= False))&(sms.content.str.contains(credit_words,case= False,na= False))&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(3,'D'))]['amount'].sum())
   
    sms_in_bkash_credit_cnt = sms.loc[(sms.type==1)&(sms.phone.str.contains('bKash', case = False, na = False))&(sms.content.str.contains(credit_words, case = False, na = False))]['id'].count()
    sms_in_d3_bkash_credit_cnt = sms.loc[(sms.type==1)&(sms.phone.str.contains('bKash', case = False, na = False))&(sms.content.str.contains(credit_words, case = False, na = False))&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(3,'D'))]['id'].count()
    sms_in_d14_bkash_credit_cnt = sms.loc[(sms.type==1)&(sms.phone.str.contains('bKash', case = False, na = False))&(sms.content.str.contains(credit_words, case = False, na = False))&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(14,'D'))]['id'].count()
    sms_in_d30_bkash_credit_cnt = sms.loc[(sms.type==1)&(sms.phone.str.contains('bKash', case = False, na = False))&(sms.content.str.contains(credit_words, case = False, na = False))&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(30,'D'))]['id'].count()
  
    sms_in_bkash_credit_amount = sms.loc[(sms.type ==1)&((sms.phone.str.contains('bKash',case = False, na = False)))&(sms.content.str.contains(trx_words,case= False,na= False))& (~sms.content.str.contains(trx_exclude_words,case= False,na= False))&(sms.content.str.contains(credit_words,case= False,na= False))]['amount'].sum()
    sms_in_min_bkash_credit_amount = np.min(sms.loc[(sms.type ==1)&((sms.phone.str.contains('bKash',case = False, na = False)))&(sms.content.str.contains(trx_words,case= False,na= False))& (~sms.content.str.contains(trx_exclude_words,case= False,na= False))&(sms.content.str.contains(credit_words,case= False,na= False))]['amount'].sum()) 
    sms_in_max_bkash_credit_amount = np.max(sms.loc[(sms.type ==1)&((sms.phone.str.contains('bKash',case = False, na = False)))&(sms.content.str.contains(trx_words,case= False,na= False))& (~sms.content.str.contains(trx_exclude_words,case= False,na= False))&(sms.content.str.contains(credit_words,case= False,na= False))]['amount'].sum()) 
    
    sms_in_d0_bkash_credit_amount = sms.loc[(sms.type ==1)&((sms.phone.str.contains('bKash',case = False, na = False)))&(sms.content.str.contains(trx_words,case= False,na= False))& (~sms.content.str.contains(trx_exclude_words,case= False,na= False))&(sms.content.str.contains(credit_words,case= False,na= False))&(sms.borrow_time.dt.date == sms.created_time.dt.date)]['amount'].sum()
    sms_in_d0_max_bkash_credit_amount = np.max(sms.loc[(sms.type ==1)&((sms.phone.str.contains('bKash',case = False, na = False)))&(sms.content.str.contains(trx_words,case= False,na= False))& (~sms.content.str.contains(trx_exclude_words,case= False,na= False))&(sms.content.str.contains(credit_words,case= False,na= False))&(sms.borrow_time.dt.date == sms.created_time.dt.date)]['amount'].sum())
    sms_in_d3_bkash_credit_amount = sms.loc[(sms.type ==1)&((sms.phone.str.contains('bKash',case = False, na = False)))&(sms.content.str.contains(trx_words,case= False,na= False))& (~sms.content.str.contains(trx_exclude_words,case= False,na= False))&(sms.content.str.contains(credit_words,case= False,na= False))&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(3,'D'))]['amount'].sum()
    sms_in_d3_max_bkash_credit_amount = np.max(sms.loc[(sms.type ==1)&((sms.phone.str.contains('bKash',case = False, na = False)))&(sms.content.str.contains(trx_words,case= False,na= False))& (~sms.content.str.contains(trx_exclude_words,case= False,na= False))&(sms.content.str.contains(credit_words,case= False,na= False))&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(3,'D'))]['amount'].sum())
    
    sms_in_nagad_credit_cnt = sms.loc[(sms.type==1)&(sms.phone.str.contains('nagad', case = False, na = False))&(sms.content.str.contains(credit_words, case = False, na = False))]['id'].count()
    sms_in_d3_nagad_credit_cnt = sms.loc[(sms.type==1)&(sms.phone.str.contains('nagad', case = False, na = False))&(sms.content.str.contains(credit_words, case = False, na = False))&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(3,'D'))]['id'].count()
    sms_in_d14_nagad_credit_cnt = sms.loc[(sms.type==1)&(sms.phone.str.contains('nagad', case = False, na = False))&(sms.content.str.contains(credit_words, case = False, na = False))&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(14,'D'))]['id'].count()
    sms_in_d30_nagad_credit_cnt = sms.loc[(sms.type==1)&(sms.phone.str.contains('nagad', case = False, na = False))&(sms.content.str.contains(credit_words, case = False, na = False))&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(30,'D'))]['id'].count()
  
#     sms['balance'] = sms['content'].str.extract(r'(?<=balance)[\s:tk\.]*([\d,]+(?:\.[\d]{2})?)', expand=False).str.replace(',', '', regex=False).replace('', pd.NA).astype(float).astype('Int64')
    sms_in_nagad_credit_amount = sms.loc[(sms.type ==1)&((sms.phone.str.contains('nagad',case = False, na = False)))&(sms.content.str.contains(trx_words,case= False,na= False))& (~sms.content.str.contains(trx_exclude_words,case= False,na= False))&(sms.content.str.contains(credit_words,case= False,na= False))]['amount'].sum()
    sms_in_min_nagad_credit_amount = np.min(sms.loc[(sms.type ==1)&((sms.phone.str.contains('nagad',case = False, na = False)))&(sms.content.str.contains(trx_words,case= False,na= False))& (~sms.content.str.contains(trx_exclude_words,case= False,na= False))&(sms.content.str.contains(credit_words,case= False,na= False))]['amount'].sum()) 
    sms_in_max_nagad_credit_amount = np.max(sms.loc[(sms.type ==1)&((sms.phone.str.contains('nagad',case = False, na = False)))&(sms.content.str.contains(trx_words,case= False,na= False))& (~sms.content.str.contains(trx_exclude_words,case= False,na= False))&(sms.content.str.contains(credit_words,case= False,na= False))]['amount'].sum()) 
    
    sms_in_d0_nagad_credit_amount = sms.loc[(sms.type ==1)&((sms.phone.str.contains('nagad',case = False, na = False)))&(sms.content.str.contains(trx_words,case= False,na= False))& (~sms.content.str.contains(trx_exclude_words,case= False,na= False))&(sms.content.str.contains(credit_words,case= False,na= False))&(sms.borrow_time.dt.date == sms.created_time.dt.date)]['amount'].sum()
    sms_in_d0_max_nagad_credit_amount = np.max(sms.loc[(sms.type ==1)&((sms.phone.str.contains('nagad',case = False, na = False)))&(sms.content.str.contains(trx_words,case= False,na= False))& (~sms.content.str.contains(trx_exclude_words,case= False,na= False))&(sms.content.str.contains(credit_words,case= False,na= False))&(sms.borrow_time.dt.date == sms.created_time.dt.date)]['amount'].sum())
    sms_in_d3_nagad_credit_amount = sms.loc[(sms.type ==1)&((sms.phone.str.contains('nagad',case = False, na = False)))&(sms.content.str.contains(trx_words,case= False,na= False))& (~sms.content.str.contains(trx_exclude_words,case= False,na= False))&(sms.content.str.contains(credit_words,case= False,na= False))&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(3,'D'))]['amount'].sum()
    sms_in_d3_max_nagad_credit_amount = np.max(sms.loc[(sms.type ==1)&((sms.phone.str.contains('nagad',case = False, na = False)))&(sms.content.str.contains(trx_words,case= False,na= False))& (~sms.content.str.contains(trx_exclude_words,case= False,na= False))&(sms.content.str.contains(credit_words,case= False,na= False))&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(3,'D'))]['amount'].sum())
    
#     sms['balance'] = sms['content'].str.extract(r'(?<=balance)[\s:tk\.]*([\d,]+(?:\.[\d]{2})?)', expand=False).str.replace(',', '', regex=False).replace('', pd.NA).astype(float).astype('Int64')
    sms['balance'] = (sms['content'].str.lower().str.extract(r'(?<=balance)[\s:tk\.]*([\d,]+(?:\.\d{2})?)')[0].str.replace(',', '', regex=False).replace('', pd.NA).astype(float))

# #     sms_in_min/max_d0/3/7/14/30_balance
    sms_in_min_balance = np.min(sms.loc[(sms.type==1)&((sms.phone.str.contains('bKash',case = False, na = False))|(sms.phone.str.contains('nagad',case = False, na = False)))]['balance'])
    sms_in_max_balance  = np.max(sms.loc[(sms.type==1)&((sms.phone.str.contains('bKash',case = False, na = False))|(sms.phone.str.contains('nagad',case = False, na = False)))]['balance'])
    sms_in_d3_max_balance = np.max(sms.loc[(sms.type==1)&((sms.phone.str.contains('bKash',case = False, na = False))|(sms.phone.str.contains('nagad',case = False, na = False)))&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(3,'D'))]['balance'])
    sms_in_d7_max_balance = np.max(sms.loc[(sms.type==1)&((sms.phone.str.contains('bKash',case = False, na = False))|(sms.phone.str.contains('nagad',case = False, na = False)))&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(7,'D'))]['balance'])
    sms_in_d14_max_balance = np.max(sms.loc[(sms.type==1)&((sms.phone.str.contains('bKash',case = False, na = False))|(sms.phone.str.contains('nagad',case = False, na = False)))&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(14,'D'))]['balance'])
    sms_in_d30_max_balance = np.max(sms.loc[(sms.type==1)&((sms.phone.str.contains('bKash',case = False, na = False))|(sms.phone.str.contains('nagad',case = False, na = False)))&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(30,'D'))]['balance'])
    
    
    

    


#     sms_in_d30_min_bkash_credit_amount = np.min(sms.loc[(sms.type ==1)&(sms.phone.str.contains('bKash',case = False, na = False))&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(30,'D'))&(sms.content.str.contains(trx_words,case= False,na= False))& (~sms.content.str.contains(trx_exclude_words,case= False,na= False))&(sms.content.str.contains(credit_words,case= False,na= False))]['amount'])
#     sms_in_d7_max_bkash_credit_amount = np.max(sms.loc[(sms.type ==1)&(sms.phone.str.contains('bKash',case = False, na = False))&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(7,'D'))&(sms.content.str.contains(trx_words,case= False,na= False))& (~sms.content.str.contains(trx_exclude_words,case= False,na= False))&(sms.content.str.contains(credit_words,case= False,na= False))]['amount'])
#     sms_in_d3_max_credit_amount = np.max(sms.loc[(sms.type ==1)&((sms.phone.str.contains('bKash',case = False, na = False))|(sms.phone.str.contains('nagad',case = False, na = False)))&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(3,'D'))&(sms.content.str.contains(trx_words,case= False,na= False))& (~sms.content.str.contains(trx_exclude_words,case= False,na= False))&(sms.content.str.contains(credit_words,case= False,na= False))]['amount'])
#     sms_in_d7_max_credit_amount = np.max(sms.loc[(sms.type ==1)&((sms.phone.str.contains('bKash',case = False, na = False))|(sms.phone.str.contains('nagad',case = False, na = False)))&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(7,'D'))&(sms.content.str.contains(trx_words,case= False,na= False))& (~sms.content.str.contains(trx_exclude_words,case= False,na= False))&(sms.content.str.contains(credit_words,case= False,na= False))]['amount'])

#     sms_in_d30_max_bkash_debit_amount = np.max(sms.loc[(sms.type ==1)&(sms.phone.str.contains('bKash',case = False, na = False))&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(30,'D'))&(sms.content.str.contains(trx_words,case= False,na= False))& (~sms.content.str.contains(trx_exclude_words,case= False,na= False))&(sms.content.str.contains(debit_words,case= False,na= False))]['amount'])
#     sms_in_d14_min_nagad_credit_amount = np.min(sms.loc[(sms.type ==1)&(sms.phone.str.contains('nagad',case = False, na = False))&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(14,'D'))&(sms.content.str.contains(trx_words,case= False,na= False))& (~sms.content.str.contains(trx_exclude_words,case= False,na= False))&(sms.content.str.contains(credit_words,case= False,na= False))]['amount'])

#     # sms.loc[(sms.type ==1)&((sms.phone.str.contains('bKash',case = False, na = False))|(sms.phone.str.contains('nagad',case = False, na = False)))&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(14,'D'))&(sms.content.str.contains(trx_words,case= False,na= False))& (~sms.content.str.contains(trx_exclude_words,case= False,na= False))&(sms.content.str.contains(credit_words,case= False,na= False))].notna().any()  
#     sms_in_d14_credit_amount = sms.loc[(sms.type ==1)&((sms.phone.str.contains('bKash',case = False, na = False))|(sms.phone.str.contains('nagad',case = False, na = False)))&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(14,'D'))&(sms.content.str.contains(trx_words,case= False,na= False))& (~sms.content.str.contains(trx_exclude_words,case= False,na= False))&(sms.content.str.contains(credit_words,case= False,na= False))]['amount'].sum()

#     sms_in_d14_max_credit_amount = np.max(sms.loc[(sms.type ==1)&((sms.phone.str.contains('bKash',case = False, na = False))|(sms.phone.str.contains('nagad',case = False, na = False)))&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(14,'D'))&(sms.content.str.contains(trx_words,case= False,na= False))& (~sms.content.str.contains(trx_exclude_words,case= False,na= False))&(sms.content.str.contains(credit_words,case= False,na= False))]['amount'])

#     sms_in_min_bkash_credit_amount  = np.min(sms.loc[(sms.type ==1)&(sms.phone.str.contains('bKash',case = False, na = False))&(sms.content.str.contains(trx_words,case= False,na= False))& (~sms.content.str.contains(trx_exclude_words,case= False,na= False))&(sms.content.str.contains(credit_words,case= False,na= False))]['amount'])

#     sms_in_d30_bkash_credit_amount  = sms.loc[(sms.type ==1)&((sms.phone.str.contains('bKash',case = False, na = False)))&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(30,'D'))&(sms.content.str.contains(trx_words,case= False,na= False))& (~sms.content.str.contains(trx_exclude_words,case= False,na= False))&(sms.content.str.contains(credit_words,case= False,na= False))]['amount'].sum()

#     sms_in_d14_max_bkash_credit_amount = np.max(sms.loc[(sms.type ==1)&(sms.phone.str.contains('bKash',case = False, na = False))&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(14,'D'))&(sms.content.str.contains(trx_words,case= False,na= False))& (~sms.content.str.contains(trx_exclude_words,case= False,na= False))&(sms.content.str.contains(credit_words,case= False,na= False))]['amount'])

#     sms_in_d7_bkash_credit_amount = sms.loc[(sms.type ==1)&((sms.phone.str.contains('bKash',case = False, na = False)))&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(7,'D'))&(sms.content.str.contains(trx_words,case= False,na= False))& (~sms.content.str.contains(trx_exclude_words,case= False,na= False))&(sms.content.str.contains(credit_words,case= False,na= False))]['amount'].sum()

#     sms_in_d14_max_nagad_debit_amount = np.max(sms.loc[(sms.type ==1)&(sms.phone.str.contains('nagad',case = False, na = False))&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(14,'D'))&(sms.content.str.contains(trx_words,case= False,na= False))& (~sms.content.str.contains(trx_exclude_words,case= False,na= False))&(sms.content.str.contains(debit_words,case= False,na= False))]['amount'])

#     sms_in_d14_max_nagad_credit_amount = np.max(sms.loc[(sms.type ==1)&(sms.phone.str.contains('nagad',case = False, na = False))&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(14,'D'))&(sms.content.str.contains(trx_words,case= False,na= False))& (~sms.content.str.contains(trx_exclude_words,case= False,na= False))&(sms.content.str.contains(credit_words,case= False,na= False))]['amount'])

#     sms_in_d14_bkash_credit_amount = sms.loc[(sms.type ==1)&((sms.phone.str.contains('bKash',case = False, na = False)))&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(14,'D'))&(sms.content.str.contains(trx_words,case= False,na= False))& (~sms.content.str.contains(trx_exclude_words,case= False,na= False))&(sms.content.str.contains(credit_words,case= False,na= False))]['amount'].sum()

#     sms_in_max_bkash_debit_amount = np.max(sms.loc[(sms.type ==1)&(sms.phone.str.contains('bKash',case = False, na = False))&(sms.content.str.contains(trx_words,case= False,na= False))& (~sms.content.str.contains(trx_exclude_words,case= False,na= False))&(sms.content.str.contains(debit_words,case= False,na= False))]['amount'])

#     sms_in_d30_max_bkash_credit_amount = np.max(sms.loc[(sms.type ==1)&(sms.phone.str.contains('bKash',case = False, na = False))&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(30,'D'))&(sms.content.str.contains(trx_words,case= False,na= False))& (~sms.content.str.contains(trx_exclude_words,case= False,na= False))&(sms.content.str.contains(credit_words,case= False,na= False))]['amount'])                               
#     sms_in_min_bkash_debit_amount = np.min(sms.loc[(sms.type ==1)&(sms.phone.str.contains('bKash',case = False, na = False))&(sms.content.str.contains(trx_words,case= False,na= False))& (~sms.content.str.contains(trx_exclude_words,case= False,na= False))&(sms.content.str.contains(debit_words,case= False,na= False))]['amount'])
# sms_in_credit_amount = 
# sms_in_d0_credit_amount 
# sms_in_bkash_credit_amount = 
# sms_in_d14_min_credit_amount = 
# sms_in_d14_min_nagad_credit_amount 
# sms_in_d30_bkash_debit_amount 
# sms_in_d30_debit_amount =
# sms_in_bkash_debit_amount =  




# # ((sms.content.str.contains(trx_words,case= False,na= False))& (~sms.content.str.contains(trx_exclude_words,case= False,na= False))&(sms['content'].str.extract(r'(?<=tk\s{0,5})[\d,]+(?:\.[\d]{2})?')[0].str.replace(',', '').astype(float))
# sms_in_bank_unique_phone = sms.loc[(sms.type==1) & (sms.content.str.contains(bank_content,case= False,na= False))]['phone'].nunique()
# sms_in_bank_cnt = sms.loc[(sms.type==1) & (sms.content.str.contains(bank_content,case= False,na= False))]['id'].count()
# sms_in_d3_bank_cnt =  sms.loc[(sms.type==1) & (sms.content.str.contains(bank_content,case= False,na= False))&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(3,'D'))]['id'].count()

# sms_in_bkash_cnt= sms.loc[(sms.phone.str.contains('bkash', case= False, na = False))&(sms.type ==1)]['id'].count()
# sms_in_d14_nagad_cnt = sms.loc[(sms.phone.str.contains('nagad', case= False, na = False))&(sms.type ==1)&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(14,'D'))]['id'].count()

# sms['balance']  = (sms['content'].str.extract(r'(?<=balance)[\s:tk\.]*([\d,]+(?:\.[\d]{2})?), expand=False).str.replace(',', '', regex=False).astype(float))

# sms_in_min_balance =np.min(sms.loc[(sms['type'] == 1) & (sms['balance'].notna())]['balance'])
# sms_in_max_balance = np.max(sms.loc[(sms['type'] == 1) & (sms['balance'].notna())]['balance'])
# sms_in_d14_max_balance = np.max(sms.loc[(sms['type'] == 1) & (sms['balance'].notna())&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(14,'D'))]['balance'])
# sms_in_d14_min_balance = np.min(sms.loc[(sms['type'] == 1) & (sms['balance'].notna())&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(14,'D'))]['balance'])
# sms_in_d7_max_balance = np.max(sms.loc[(sms['type'] == 1) & (sms['balance'].notna())&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(7,'D'))]['balance'])


# sms_in_bet_cnt = sms.loc[(sms.type== 1)&(sms.content.str.contains(bet_words,case = False,na= False))&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(30,'D'))]['id'].count()
# sms_in_d7_bkash_trx_cnt  = sms.loc[(sms.type ==1)&(sms.phone.str.contains('bKash',case = False, na = False))&(sms.content.str.contains(trx_words,case= False,na= False))& (~sms.content.str.contains(trx_exclude_words,case= False,na= False))]['id'].count()
# sms_in_recharge_cnt = sms.loc[(sms.type == 1)&(sms.content.str.contains(recharge_words,case = False, na = False))]['id'].count()
# sms_in_d7_recharge_cnt = sms.loc[(sms.type == 1)&(sms.content.str.contains(recharge_words,case = False, na = False))&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(7,'D'))]['id'].count()
# sms_in_d30_recharge_cnt = sms.loc[(sms.type == 1)&(sms.content.str.contains(recharge_words,case = False, na = False))&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(30,'D'))]['id'].count()

# sms_in_telcom_d7_unique_phone = sms.loc[(sms.type == 1)&((sms.content.str.contains('telecom',case =False, na= False))|(sms.phone.str.contains('telecom', case = False, na = False)))&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(7,'D'))]['phone'].nunique()

# sms_in_d30_credit_cnt = sms.loc[(sms.type ==1)&(sms.content.str.contains(trx_words,case= False,na= False))&(~sms.content.str.contains(trx_exclude_words,case=False, na = False))&((sms.phone.str.contains('bKash', case = False, na = False))|(sms.phone.str.contains('nagad', case = False, na = False)))&(sms.content.str.contains(credit_words, case = False, na = False))&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(30,'D'))]['id'].count()
# sms_in_d30_bkash_cnt = sms.loc[(sms.type == 1) &(sms.phone.str.contains('bkash', case= False, na = False))&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(30,'D'))]['id'].count()
# sms_in_d30_nagad_cnt = sms.loc[(sms.type == 1) &(sms.phone.str.contains('nagad', case= False, na = False))&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(30,'D'))]['id'].count()


# sms_in_nagad_cnt =  sms.loc[(sms.type == 1) &(sms.phone.str.contains('nagad', case= False, na = False))]['id'].count()

# sms_in_trx_cnt = sms.loc[(sms.type ==1)&((sms.phone.str.contains('bKash',case = False, na = False))|(sms.phone.str.contains('nagad',case= False, na= False)))&(sms.content.str.contains(trx_words,case= False,na= False))& (~sms.content.str.contains(trx_exclude_words,case= False,na= False))]['id'].count()
# sms_in_d3_trx_cnt = sms.loc[(sms.type ==1)&((sms.phone.str.contains('bKash',case = False, na = False))|(sms.phone.str.contains('nagad',case= False, na= False)))&(sms.content.str.contains(trx_words,case= False,na= False))& (~sms.content.str.contains(trx_exclude_words,case= False,na= False))&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(3,'D'))]['id'].count()
# sms_in_d14_trx_cnt = sms.loc[(sms.type ==1)&((sms.phone.str.contains('bKash',case = False, na = False))|(sms.phone.str.contains('nagad',case= False, na= False)))&(sms.content.str.contains(trx_words,case= False,na= False))& (~sms.content.str.contains(trx_exclude_words,case= False,na= False))&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(14,'D'))]['id'].count()

# sms_in_d3_bkash_credit_cnt = sms.loc[(sms.type==1)&(sms.phone.str.contains('bKash', case = False, na = False))&(sms.content.str.contains(credit_words, case = False, na = False))&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(3,'D'))]['id'].count()
# sms_in_d14_bkash_credit_cnt = sms.loc[(sms.type==1)&(sms.phone.str.contains('bKash', case = False, na = False))&(sms.content.str.contains(credit_words, case = False, na = False))&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(14,'D'))]['id'].count()
# sms_in_d30_bkash_credit_cnt = sms.loc[(sms.type==1)&(sms.phone.str.contains('bKash', case = False, na = False))&(sms.content.str.contains(credit_words, case = False, na = False))&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(30,'D'))]['id'].count()

# sms_in_d0_min_balance = np.min(sms.loc[(sms.type ==1)&(sms['balance'].notna())&(sms.borrow_time.dt.date == sms.created_time.dt.date)]['balance'])
# sms_in_d3_max_balance = np.max(sms.loc[(sms.type ==1)&(sms['balance'].notna())&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(3,'D'))]['balance'])
# sms_in_min_balance  = np.min(sms.loc[(sms.type ==1)&(sms['balance'].notna())]['balance'])

    
# sms_in_d14_telcom_cnt = sms.loc[(sms.type == 1)&((sms.content.str.contains('telecom',case =False, na= False)))&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(14,'D'))]['id'].count()
# sms_in_d30_telcom_cnt= sms.loc[(sms.type == 1)&((sms.content.str.contains('telecom',case =False, na= False)))&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(30,'D'))]['id'].count()

# sms_in_telcom_d30_unique_phone = sms.loc[(sms.type == 1)&((sms.content.str.contains('telecom',case =False, na= False))|(sms.phone.str.contains('telecom', case = False, na = False)))&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(30,'D'))]['phone'].nunique()
# sms_in_telcom_d14_unique_phone = sms.loc[(sms.type == 1)&((sms.content.str.contains('telecom',case =False, na= False))|(sms.phone.str.contains('telecom', case = False, na = False)))&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(14,'D'))]['phone'].nunique()

# sms_in_telcom_unique_phone = sms.loc[(sms.type == 1)&((sms.content.str.contains('telecom',case =False, na= False))|(sms.phone.str.contains('telecom', case = False, na = False)))]['phone'].nunique()
 
# sms_in_d7_bkash_credit_cnt= sms.loc[(sms.type ==1)&(sms.phone.str.contains('bKash',case = False, na = False))&(sms.content.str.contains(trx_words,case= False,na= False))& (~sms.content.str.contains(trx_exclude_words,case= False,na= False))&(sms.content.str.contains(credit_words, case = False, na = False))&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(7,'D'))]['id'].count()

# sms_in_d0_bkash_cnt = sms.loc[(sms.type ==1)&(sms.phone.str.contains('bKash', case = False, na = False))&(sms.borrow_time.dt.date == sms.created_time.dt.date)]['id'].count()

# sms_in_bkash_cnt = sms.loc[(sms.type ==1)&(sms.phone.str.contains('bKash', case = False, na = False))]['id'].count()

# sms_in_d14_bkash_cnt = sms.loc[(sms.type ==1)&(sms.phone.str.contains('bKash', case = False, na = False))&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(14,'D'))]['id'].count()
# sms_in_otp_unique_phone = sms.loc[(sms.type==1)&(sms.content.str.contains(otp_words, case= False,na= False))]['phone'].nunique()                         
# sms_in_otp_d14_unique_phone = sms.loc[(sms.type==1)&(sms.content.str.contains(otp_words, case= False,na= False))&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(14,'D'))]['phone'].nunique()                       
# sms_in_otp_d30_unique_phone = sms.loc[(sms.type==1)&(sms.content.str.contains(otp_words, case= False,na= False))&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(30,'D'))]['phone'].nunique()                       
# sms_in_d3_unique_phone = sms.loc[(sms.type==1)&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(3,'D'))]['phone'].nunique()

# sms_in_d7_trx_cnt = sms.loc[(sms.type==1)&((sms.phone.str.contains('bKash',case = False, na = False))|(sms.phone.str.contains('nagad',case= False, na= False)))&(sms.content.str.contains(trx_words,case= False,na= False))& (~sms.content.str.contains(trx_exclude_words,case= False,na= False))&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(7,'D'))]['id'].count() 
# sms_in_d30_trx_cnt = sms.loc[(sms.type==1)&((sms.phone.str.contains('bKash',case = False, na = False))|(sms.phone.str.contains('nagad',case= False, na= False)))&(sms.content.str.contains(trx_words,case= False,na= False))& (~sms.content.str.contains(trx_exclude_words,case= False,na= False))&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(30,'D'))]['id'].count()
# sms_in_d30_trx_unique_phone = sms.loc[(sms.type==1)&((sms.phone.str.contains('bKash',case = False, na = False))|(sms.phone.str.contains('nagad',case= False, na= False)))&(sms.content.str.contains(trx_words,case= False,na= False))& (~sms.content.str.contains(trx_exclude_words,case= False,na= False))&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(30,'D'))]['phone'].nunique()
 
# sms_in_due_d30_unique_phone = sms.loc[(sms.type==1)&((sms.content.str.contains('due',case = False, na = False)))&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(30,'D'))]['phone'].nunique()
# sms_in_d30_unique_phone = sms.loc[(sms.type==1)&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(30,'D'))]['phone'].nunique()

# sms_in_d7_bkash_debit_cnt = sms.loc[(sms.type==1)&((sms.phone.str.contains('bKash',case = False, na = False)))&(sms.content.str.contains(debit_words,case= False,na= False))&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(7,'D'))]['id'].count()
# sms_in_d14_bkash_debit_cnt = sms.loc[(sms.type==1)&((sms.phone.str.contains('bKash',case = False, na = False)))&(sms.content.str.contains(debit_words,case= False,na= False))&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(14,'D'))]['id'].count()
# sms_in_bkash_debit_cnt = sms.loc[(sms.type==1)&((sms.phone.str.contains('bKash',case = False, na = False)))&(sms.content.str.contains(debit_words,case= False,na= False))]['id'].count()
# sms_in_d7_personal_phone_cnt =sms.loc[(sms.type == 1)&(sms.content.str.contains('\+880',case =False, na= False))&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(7,'D'))]['phone'].nunique()
  
# sms_in_d7_bkash_debit_amount
# sms_in_debit_amount
#  sms_in_d7_min_debit_amount
# sms_in_otp_unique_phone = sms.loc[(sms.type== 1)&(sms.content.str.contains(otp_words,case = False,na= False))]['phone'].nunique()
# sms_in_loan_cnt = sms.loc[(sms.type ==1)&(sms.content.str.contains(loan_words, case = False, na = False))]['id'].count()
# sms_in_d14_recharge_telcom_cnt = sms.loc[(sms.type ==1)&(sms.content.str.contains(recharge_words, case = False, na = False))&((sms.phone.str.contains('telecom',case = False, na = False))|(sms.content.str.contains('telecom',case= False, na= False)))]['id'].count()
    df= {'order_id': order_id, 
                  'sms_in_max_debit_amount':sms_in_max_debit_amount,
                    'sms_in_debit_amount':sms_in_debit_amount,
                    'sms_in_d0_debit_amount':sms_in_d0_debit_amount,
                    'sms_in_d3_debit_amount':sms_in_d3_debit_amount,
                    'sms_in_d7_debit_amount':sms_in_d7_debit_amount,
                    'sms_in_max_d0_debit_amount':sms_in_max_d0_debit_amount,
                    'sms_in_max_d7_debit_amount':sms_in_max_d7_debit_amount,
                     'sms_in_credit_amount':sms_in_credit_amount,'sms_in_d14_credit_amount':sms_in_d14_credit_amount,
                    'sms_in_max_credit_amount':sms_in_max_credit_amount,'sms_in_max_d3_credit_amount':sms_in_max_d3_credit_amount,
                    'sms_in_bkash_credit_cnt':sms_in_bkash_credit_cnt 

         
        }
    
    df_data = pd.DataFrame([df])
    df_data.fillna(-1, inplace = True)
    return df_data 

