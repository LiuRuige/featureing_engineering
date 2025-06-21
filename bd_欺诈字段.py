def fraud_variable(order_id):
    
    debts_order = data[data['order_id'].astype(str)== order_id] 

    order_app_name = debts_order['app_name'].item()
    order_borrow_time = debts_order['borrow_time'].dt.date.item()
    order_user_phone = debts_order['user_phone'].item()
    order_user_ip =  debts_order['user_ip'].item()
    order_user_device_id = debts_order['user_device_id'].item()
    order_user_card_id = debts_order['user_card_id'].item()
    order_user_bank_account = debts_order['user_bank_account'].item() 
  
    debts  = data.loc[(data['user_phone'] == debts_order['user_phone'].item())&(data['borrow_time'].dt.date <= debts_order['borrow_time'].dt.date.item())]
    app_name = debts.app_name 
    loan_amount = debts.loan_amount 
    user_phone = debts.user_phone
    borrow_time = debts.borrow_time.dt.date
    user_card_id = debts.user_card_id 
    user_ip = debts.user_ip
    user_device_id = debts.user_device_id
    user_bank_account = debts.user_bank_account
    user_type = debts.user_type
    status =debts.status 
    repay_yes_time = debts.repay_yes_time.dt.date
    repay_time = debts.repay_time.dt.date
    loan_completion_time = debts.loan_completion_time.dt.date
    if debts.empty:
        same_phone_diff_device_apply_cnt =same_phone_diff_device_d0_apply_cnt = same_phone_diff_device_d3_apply_cnt = same_phone_diff_device_d7_apply_cnt = same_phone_diff_device_d14_apply_cnt = same_phone_diff_device_d30_apply_cnt = -1
        same_phone_diff_account_apply_cnt = same_phone_diff_account_d0_apply_cnt = same_phone_diff_account_d3_apply_cnt = same_phone_diff_account_d7_apply_cnt = same_phone_diff_account_d14_apply_cnt = same_phone_diff_account_d30_apply_cnt = -1
        same_phone_diff_cardid_apply_cnt = same_phone_diff_cardid_d0_apply_cnt = same_phone_diff_cardid_d3_apply_cnt = same_phone_diff_cardid_d7_apply_cnt = same_phone_diff_cardid_d14_apply_cnt = same_phone_diff_cardid_d30_apply_cnt = -1
        same_phone_diff_ip_apply_cnt = same_phone_diff_ip_d0_apply_cnt = same_phone_diff_ip_d3_apply_cnt = same_phone_diff_ip_d7_apply_cnt = same_phone_diff_ip_d14_apply_cnt = same_phone_diff_ip_d30_apply_cnt = -1 
    else:
        same_phone_diff_device_apply_cnt= debts.loc[(user_device_id  != order_user_device_id)&len(user_device_id)==36 & (user_device_id!='00000000-0000-0000-0000-000000000000') & (borrow_time<= order_borrow_time)]['user_device_id'].nunique()
        same_phone_diff_device_d0_apply_cnt= debts.loc[(user_device_id  != order_user_device_id)&len(user_device_id)==36 & (user_device_id!='00000000-0000-0000-0000-000000000000') & ((order_borrow_time - borrow_time).dt.days == 0)]['user_device_id'].nunique()
        same_phone_diff_device_d3_apply_cnt= debts.loc[(user_device_id  != order_user_device_id)&len(user_device_id)==36 & (user_device_id!='00000000-0000-0000-0000-000000000000') &((order_borrow_time - borrow_time).dt.days <= 3)]['user_device_id'].nunique()
        same_phone_diff_device_d7_apply_cnt= debts.loc[(user_device_id  != order_user_device_id)&len(user_device_id)==36 & (user_device_id!='00000000-0000-0000-0000-000000000000') &((order_borrow_time - borrow_time).dt.days <= 7)]['user_device_id'].nunique()
        same_phone_diff_device_d14_apply_cnt= debts.loc[(user_device_id  != order_user_device_id)&len(user_device_id)==36 & (user_device_id!='00000000-0000-0000-0000-000000000000') &((order_borrow_time - borrow_time).dt.days <= 14)]['user_device_id'].nunique()
        same_phone_diff_device_d30_apply_cnt= debts.loc[(user_device_id  != order_user_device_id)&len(user_device_id)==36 & (user_device_id!='00000000-0000-0000-0000-000000000000') &((order_borrow_time - borrow_time).dt.days <= 30)]['user_device_id'].nunique()


        same_phone_diff_account_apply_cnt= debts.loc[(user_bank_account  != order_user_bank_account)&(user_bank_account!='')]['user_bank_account'].nunique()
        same_phone_diff_account_d0_apply_cnt= debts.loc[(user_bank_account  != order_user_bank_account)&((order_borrow_time - borrow_time).dt.days == 0)&(user_bank_account !='')]['user_bank_account'].nunique()
        same_phone_diff_account_d3_apply_cnt= debts.loc[(user_bank_account  != order_user_bank_account)&((order_borrow_time - borrow_time).dt.days <= 3)&(user_bank_account != '')]['user_bank_account'].nunique()
        same_phone_diff_account_d7_apply_cnt= debts.loc[(user_bank_account  != order_user_bank_account)&((order_borrow_time - borrow_time).dt.days <= 7)&(user_bank_account != '')]['user_bank_account'].nunique()
        same_phone_diff_account_d14_apply_cnt= debts.loc[(user_bank_account  != order_user_bank_account)&((order_borrow_time - borrow_time).dt.days <= 14)&(user_bank_account != '')]['user_bank_account'].nunique()
        same_phone_diff_account_d30_apply_cnt= debts.loc[(user_bank_account  != order_user_bank_account)&((order_borrow_time - borrow_time).dt.days <= 30)&(user_bank_account !='')]['user_bank_account'].nunique()
        

        same_phone_diff_cardid_apply_cnt= debts.loc[(user_card_id != order_user_card_id)]['user_card_id'].nunique()
        same_phone_diff_cardid_d0_apply_cnt= debts.loc[(user_card_id != order_user_card_id)&((order_borrow_time - borrow_time).dt.days == 0)&(user_card_id !='')]['user_card_id'].nunique()
        same_phone_diff_cardid_d3_apply_cnt= debts.loc[(user_card_id != order_user_card_id)&((order_borrow_time - borrow_time).dt.days <= 3)&(user_card_id !='')]['user_card_id'].nunique()
        same_phone_diff_cardid_d7_apply_cnt= debts.loc[(user_card_id != order_user_card_id)&((order_borrow_time - borrow_time).dt.days <= 7)&(user_card_id !='')]['user_card_id'].nunique()
        same_phone_diff_cardid_d14_apply_cnt= debts.loc[(user_card_id != order_user_card_id)&((order_borrow_time - borrow_time).dt.days <= 14)&(user_card_id !='')]['user_card_id'].nunique()
        same_phone_diff_cardid_d30_apply_cnt= debts.loc[(user_card_id != order_user_card_id)&((order_borrow_time - borrow_time).dt.days <= 30)&(user_card_id !='')]['user_card_id'].nunique()
        
        same_phone_diff_ip_apply_cnt= debts.loc[(user_ip != order_user_ip)]['user_ip'].nunique()
        same_phone_diff_ip_d0_apply_cnt= debts.loc[(user_ip != order_user_ip)&((order_borrow_time - borrow_time).dt.days == 0)&(user_ip !='')]['user_ip'].nunique()
        same_phone_diff_ip_d3_apply_cnt= debts.loc[(user_ip != order_user_ip)&((order_borrow_time - borrow_time).dt.days <= 3)&(user_ip !='')]['user_ip'].nunique()
        same_phone_diff_ip_d7_apply_cnt= debts.loc[(user_ip != order_user_ip)&((order_borrow_time - borrow_time).dt.days <= 7)&(user_ip !='')]['user_ip'].nunique()
        same_phone_diff_ip_d14_apply_cnt= debts.loc[(user_ip != order_user_ip)&((order_borrow_time - borrow_time).dt.days <= 14)&(user_ip !='')]['user_ip'].nunique()
        same_phone_diff_ip_d30_apply_cnt= debts.loc[(user_ip != order_user_ip)&((order_borrow_time - borrow_time).dt.days <= 30)&(user_ip !='')]['user_ip'].nunique()

    # phone_first_disburse_gap=if_else(max(disburse_diff,na.rm = T)%>%is.infinite()|max(disburse_diff,na.rm = T)%>%is.na(),-1,max(disburse_diff,na.rm = T)),
    # phone_onloan_cnt=uniqueN(app_name[loan_completion_time<=tar_borrow_time & status>=8 & is_repay==0]),
    # phone_onloan_new_cnt=uniqueN(app_name[ loan_completion_time<=tar_borrow_time & status>=8 & is_repay==0 & user_type==0]),
    
    # phone_overdue_cnt=uniqueN(app_name[is_due==1 & is_repay==0]),
    # phone_repay_cnt=uniqueN(app_name[is_due==1 & is_repay==1]),
    # phone_repay_order_cnt=uniqueN(order_id[is_due==1 & is_repay==1])
     
    debts  = data.loc[(data['user_card_id'] == debts_order['user_card_id'].item())&(data['borrow_time'].dt.date <= debts_order['borrow_time'].dt.date.item())]
    app_name = debts.app_name 
    loan_amount = debts.loan_amount 
    user_phone = debts.user_phone
    borrow_time = debts.borrow_time.dt.date
    user_card_id = debts.user_card_id 
    user_ip = debts.user_ip
    user_device_id = debts.user_device_id
    user_type = debts.user_type
    status =debts.status 
    repay_yes_time = debts.repay_yes_time.dt.date
    repay_time = debts.repay_time.dt.date
    loan_completion_time = debts.loan_completion_time.dt.date
    user_bank_account = debts.user_bank_account 
    if debts.empty:
    # cardid_nex_plat_d0_overdue_cnt = debts.loc[(app_name !=  order_app_name) & (loan_amount > 0)&((repay_yes_time > pd.to_datetime(order_borrow_time))|(debts['repay_yes_time'].isna()))&((order_borrow_time - repay_time).dt.days == 0)&(repay_time< order_borrow_time)&(user_card_id == order_user_card_id)]['app_name'].nunique()
        same_cardid_diff_device_apply_cnt = same_cardid_diff_device_d0_apply_cnt = same_cardid_diff_device_d3_apply_cnt = same_cardid_diff_device_d7_apply_cnt = same_cardid_diff_device_d14_apply_cnt = same_cardid_diff_device_d30_apply_cnt = -1
        same_cardid_diff_account_apply_cnt = same_cardid_diff_account_d0_apply_cnt = same_cardid_diff_account_d3_apply_cnt = same_cardid_diff_account_d7_apply_cnt = same_cardid_diff_account_d14_apply_cnt = same_cardid_diff_account_d30_apply_cnt = -1
        same_cardid_diff_ip_apply_cnt = same_cardid_diff_ip_d0_apply_cnt = same_cardid_diff_ip_d3_apply_cnt = same_cardid_diff_ip_d7_apply_cnt = same_cardid_diff_ip_d14_apply_cnt = same_cardid_diff_ip_d30_apply_cnt = -1
        same_cardid_diff_phone_apply_cnt = same_cardid_diff_phone_d0_apply_cnt = same_cardid_diff_phone_d3_apply_cnt = same_cardid_diff_phone_d7_apply_cnt = same_cardid_diff_phone_d14_apply_cnt = same_cardid_diff_phone_d30_apply_cnt = -1
    else:
        same_cardid_diff_device_apply_cnt= debts.loc[(user_device_id != order_user_device_id)&(user_device_id!='00000000-0000-0000-0000-000000000000')]['user_device_id'].nunique()
        same_cardid_diff_device_d0_apply_cnt= debts.loc[(user_device_id  != order_user_device_id)&len(user_device_id)==36 & (user_device_id!='00000000-0000-0000-0000-000000000000') & ((order_borrow_time - borrow_time).dt.days == 0)]['user_device_id'].nunique()
        same_cardid_diff_device_d3_apply_cnt= debts.loc[(user_device_id  != order_user_device_id)&len(user_device_id)==36 & (user_device_id!='00000000-0000-0000-0000-000000000000') & ((order_borrow_time - borrow_time).dt.days <= 3)]['user_device_id'].nunique()
        same_cardid_diff_device_d7_apply_cnt= debts.loc[(user_device_id  != order_user_device_id)&len(user_device_id)==36 & (user_device_id!='00000000-0000-0000-0000-000000000000') & ((order_borrow_time - borrow_time).dt.days <= 7)]['user_device_id'].nunique()
        same_cardid_diff_device_d14_apply_cnt= debts.loc[(user_device_id  != order_user_device_id)&len(user_device_id)==36 & (user_device_id!='00000000-0000-0000-0000-000000000000') & ((order_borrow_time - borrow_time).dt.days <= 14)]['user_device_id'].nunique()
        same_cardid_diff_device_d30_apply_cnt= debts.loc[(user_device_id  != order_user_device_id)&len(user_device_id)==36 & (user_device_id!='00000000-0000-0000-0000-000000000000') & ((order_borrow_time - borrow_time).dt.days <= 30)]['user_device_id'].nunique()

        same_cardid_diff_account_apply_cnt= debts.loc[(user_bank_account  != order_user_bank_account)&(order_user_bank_account !='')]['user_bank_account'].nunique()
        same_cardid_diff_account_d0_apply_cnt= debts.loc[(user_bank_account  != order_user_bank_account)&((order_borrow_time - borrow_time).dt.days == 0)&(order_user_bank_account !='')]['user_bank_account'].nunique()
        same_cardid_diff_account_d3_apply_cnt= debts.loc[(user_bank_account  != order_user_bank_account)&((order_borrow_time - borrow_time).dt.days <= 3)&(order_user_bank_account !='')]['user_bank_account'].nunique()
        same_cardid_diff_account_d7_apply_cnt= debts.loc[(user_bank_account  != order_user_bank_account)&((order_borrow_time - borrow_time).dt.days <= 7)&(order_user_bank_account !='')]['user_bank_account'].nunique()
        same_cardid_diff_account_d14_apply_cnt= debts.loc[(user_bank_account  != order_user_bank_account)&((order_borrow_time - borrow_time).dt.days <= 14)&(order_user_bank_account !='')]['user_bank_account'].nunique()
        same_cardid_diff_account_d30_apply_cnt= debts.loc[(user_bank_account  != order_user_bank_account)&((order_borrow_time - borrow_time).dt.days <= 30)&(order_user_bank_account !='')]['user_bank_account'].nunique()

        same_cardid_diff_ip_apply_cnt=debts.loc[(user_ip != order_user_ip)]['user_ip'].nunique()
        same_cardid_diff_ip_d0_apply_cnt= debts.loc[(user_ip != order_user_ip)&((order_borrow_time - borrow_time).dt.days == 0)&(user_ip !='')]['user_ip'].nunique()
        same_cardid_diff_ip_d3_apply_cnt= debts.loc[(user_ip != order_user_ip)&((order_borrow_time - borrow_time).dt.days <= 3)&(user_ip !='')]['user_ip'].nunique()
        same_cardid_diff_ip_d7_apply_cnt= debts.loc[(user_ip != order_user_ip)&((order_borrow_time - borrow_time).dt.days <= 7)&(user_ip !='')]['user_ip'].nunique()
        same_cardid_diff_ip_d14_apply_cnt= debts.loc[(user_ip != order_user_ip)&((order_borrow_time - borrow_time).dt.days <= 14)&(user_ip !='')]['user_ip'].nunique()
        same_cardid_diff_ip_d30_apply_cnt= debts.loc[(user_ip != order_user_ip)&((order_borrow_time - borrow_time).dt.days <= 30)&(user_ip !='')]['user_ip'].nunique()
        
        same_cardid_diff_phone_apply_cnt= debts.loc[(user_phone != order_user_phone)]['user_phone'].nunique()
        same_cardid_diff_phone_d0_apply_cnt= debts.loc[(user_phone != order_user_phone)&((order_borrow_time - borrow_time).dt.days == 0)&(user_phone !='')]['user_phone'].nunique()
        same_cardid_diff_phone_d3_apply_cnt= debts.loc[(user_phone != order_user_phone)&((order_borrow_time - borrow_time).dt.days <= 3)&(user_phone !='')]['user_phone'].nunique()
        same_cardid_diff_phone_d7_apply_cnt= debts.loc[(user_phone != order_user_phone)&((order_borrow_time - borrow_time).dt.days <= 7)&(user_phone !='')]['user_phone'].nunique()    
        same_cardid_diff_phone_d14_apply_cnt= debts.loc[(user_phone != order_user_phone)&((order_borrow_time - borrow_time).dt.days <= 14)&(user_phone !='')]['user_phone'].nunique()
        same_cardid_diff_phone_d30_apply_cnt= debts.loc[(user_phone != order_user_phone)&((order_borrow_time - borrow_time).dt.days <= 30)&(user_phone !='')]['user_phone'].nunique()

    # cardid_first_disburse_gap=if_else(max(disburse_diff,na.rm = T)%>%is.infinite()|max(disburse_diff,na.rm = T)%>%is.na(),-1,max(disburse_diff,na.rm = T)),
    # cardid_onloan_cnt=uniqueN(app_name[loan_completion_time<=tar_borrow_time & status>=8 & is_repay==0]),
    # cardid_onloan_new_cnt=uniqueN(app_name[ loan_completion_time<=tar_borrow_time & status>=8 & is_repay==0 & user_type==0]),
    
    
    debts  = data.loc[(data['user_ip'] == debts_order['user_ip'].item())&(data['borrow_time'].dt.date <= debts_order['borrow_time'].dt.date.item())]
    app_name = debts.app_name 
    loan_amount = debts.loan_amount 
    user_phone = debts.user_phone
    borrow_time = debts.borrow_time.dt.date
    user_card_id = debts.user_card_id 
    user_ip = debts.user_ip
    user_device_id = debts.user_device_id
    user_type = debts.user_type
    status =debts.status 
    repay_yes_time = debts.repay_yes_time.dt.date
    repay_time = debts.repay_time.dt.date
    loan_completion_time = debts.loan_completion_time.dt.date
    user_bank_account = debts.user_bank_account 
    if debts.empty:
        same_ip_diff_account_apply_cnt = same_ip_diff_account_d0_apply_cnt = same_ip_diff_account_d3_apply_cnt = same_ip_diff_account_d7_apply_cnt = same_ip_diff_account_d14_apply_cnt = same_ip_diff_account_d30_apply_cnt = -1
        same_ip_diff_device_apply_cnt = same_ip_diff_device_d0_apply_cnt = same_ip_diff_device_d3_apply_cnt = same_ip_diff_device_d7_apply_cnt = same_ip_diff_device_d14_apply_cnt = same_ip_diff_device_d30_apply_cnt = -1
        same_ip_diff_cardid_apply_cnt = same_ip_diff_cardid_d0_apply_cnt = same_ip_diff_cardid_d3_apply_cnt = same_ip_diff_cardid_d7_apply_cnt = same_ip_diff_cardid_d14_apply_cnt = same_ip_diff_cardid_d30_apply_cnt = -1
        same_ip_diff_phone_apply_cnt = same_ip_diff_phone_d0_apply_cnt = same_ip_diff_phone_d3_apply_cnt = same_ip_diff_phone_d7_apply_cnt = same_ip_diff_phone_d14_apply_cnt = same_ip_diff_phone_d30_apply_cnt = -1
    else:
        same_ip_diff_device_apply_cnt= debts.loc[(user_device_id != order_user_device_id)&(user_device_id!='00000000-0000-0000-0000-000000000000')]['user_device_id'].nunique()
        same_ip_diff_device_d0_apply_cnt= debts.loc[(user_device_id  != order_user_device_id)&len(user_device_id)==36 & (user_device_id!='00000000-0000-0000-0000-000000000000') & ((order_borrow_time - borrow_time).dt.days == 0)]['user_device_id'].nunique()
        same_ip_diff_device_d3_apply_cnt= debts.loc[(user_device_id  != order_user_device_id)&len(user_device_id)==36 & (user_device_id!='00000000-0000-0000-0000-000000000000') & ((order_borrow_time - borrow_time).dt.days <= 3)]['user_device_id'].nunique()
        same_ip_diff_device_d7_apply_cnt= debts.loc[(user_device_id  != order_user_device_id)&len(user_device_id)==36 & (user_device_id!='00000000-0000-0000-0000-000000000000') & ((order_borrow_time - borrow_time).dt.days <= 7)]['user_device_id'].nunique()
        same_ip_diff_device_d14_apply_cnt= debts.loc[(user_device_id  != order_user_device_id)&len(user_device_id)==36 & (user_device_id!='00000000-0000-0000-0000-000000000000') & ((order_borrow_time - borrow_time).dt.days <= 14)]['user_device_id'].nunique()
        same_ip_diff_device_d30_apply_cnt= debts.loc[(user_device_id  != order_user_device_id)&len(user_device_id)==36 & (user_device_id!='00000000-0000-0000-0000-000000000000') & ((order_borrow_time - borrow_time).dt.days <= 30)]['user_device_id'].nunique()

        same_ip_diff_account_apply_cnt= debts.loc[(user_bank_account  != order_user_bank_account)&(order_user_bank_account !='')]['user_bank_account'].nunique()
        same_ip_diff_account_d0_apply_cnt= debts.loc[(user_bank_account  != order_user_bank_account)&((order_borrow_time - borrow_time).dt.days == 0)&(order_user_bank_account !='')]['user_bank_account'].nunique()
        same_ip_diff_account_d3_apply_cnt= debts.loc[(user_bank_account  != order_user_bank_account)&((order_borrow_time - borrow_time).dt.days <= 3)&(order_user_bank_account !='')]['user_bank_account'].nunique()
        same_ip_diff_account_d7_apply_cnt= debts.loc[(user_bank_account  != order_user_bank_account)&((order_borrow_time - borrow_time).dt.days <= 7)&(order_user_bank_account !='')]['user_bank_account'].nunique()
        same_ip_diff_account_d14_apply_cnt= debts.loc[(user_bank_account  != order_user_bank_account)&((order_borrow_time - borrow_time).dt.days <= 14)&(order_user_bank_account !='')]['user_bank_account'].nunique()
        same_ip_diff_account_d30_apply_cnt= debts.loc[(user_bank_account  != order_user_bank_account)&((order_borrow_time - borrow_time).dt.days <= 30)&(order_user_bank_account !='')]['user_bank_account'].nunique()
        
        same_ip_diff_cardid_apply_cnt= debts.loc[(user_card_id != order_user_card_id)]['user_card_id'].nunique()
        same_ip_diff_cardid_d0_apply_cnt= debts.loc[(user_card_id != order_user_card_id)&((order_borrow_time - borrow_time).dt.days == 0)&(user_card_id !='')]['user_card_id'].nunique()
        same_ip_diff_cardid_d3_apply_cnt= debts.loc[(user_card_id != order_user_card_id)&((order_borrow_time - borrow_time).dt.days <= 3)&(user_card_id !='')]['user_card_id'].nunique()
        same_ip_diff_cardid_d7_apply_cnt= debts.loc[(user_card_id != order_user_card_id)&((order_borrow_time - borrow_time).dt.days <= 7)&(user_card_id !='')]['user_card_id'].nunique() 
        same_ip_diff_cardid_d14_apply_cnt= debts.loc[(user_card_id != order_user_card_id)&((order_borrow_time - borrow_time).dt.days <= 14)&(user_card_id !='')]['user_card_id'].nunique()
        same_ip_diff_cardid_d30_apply_cnt= debts.loc[(user_card_id != order_user_card_id)&((order_borrow_time - borrow_time).dt.days <= 30)&(user_card_id !='')]['user_card_id'].nunique()
        
        same_ip_diff_phone_apply_cnt= debts.loc[(user_phone != order_user_phone)]['user_phone'].nunique()
        same_ip_diff_phone_d0_apply_cnt= debts.loc[(user_phone != order_user_phone)&((order_borrow_time - borrow_time).dt.days == 0)&(user_phone !='')]['user_phone'].nunique()  
        same_ip_diff_phone_d3_apply_cnt= debts.loc[(user_phone != order_user_phone)&((order_borrow_time - borrow_time).dt.days <= 3)&(user_phone !='')]['user_phone'].nunique()
        same_ip_diff_phone_d7_apply_cnt= debts.loc[(user_phone != order_user_phone)&((order_borrow_time - borrow_time).dt.days <= 7)&(user_phone !='')]['user_phone'].nunique()
        same_ip_diff_phone_d14_apply_cnt= debts.loc[(user_phone != order_user_phone)&((order_borrow_time - borrow_time).dt.days <= 14)&(user_phone !='')]['user_phone'].nunique()
        same_ip_diff_phone_d30_apply_cnt= debts.loc[(user_phone != order_user_phone)&((order_borrow_time - borrow_time).dt.days <= 30)&(user_phone !='')]['user_phone'].nunique()


    debts  = data.loc[(data['user_device_id'] == debts_order['user_device_id'].item())&(data['borrow_time'].dt.date <= debts_order['borrow_time'].dt.date.item())]
    app_name = debts.app_name 
    loan_amount = debts.loan_amount 
    user_phone = debts.user_phone
    borrow_time = debts.borrow_time.dt.date
    user_card_id = debts.user_card_id 
    user_ip = debts.user_ip
    user_device_id = debts.user_device_id
    user_type = debts.user_type
    status =debts.status 
    repay_yes_time = debts.repay_yes_time.dt.date
    repay_time = debts.repay_time.dt.date
    loan_completion_time = debts.loan_completion_time.dt.date
    user_bank_account = debts.user_bank_account

    if debts.empty:
        same_device_diff_account_apply_cnt = same_device_diff_account_d0_apply_cnt = same_device_diff_account_d3_apply_cnt = same_device_diff_account_d7_apply_cnt = same_device_diff_account_d14_apply_cnt = same_device_diff_account_d30_apply_cnt = -1
        same_device_diff_cardid_apply_cnt = same_device_diff_cardid_d0_apply_cnt = same_device_diff_cardid_d3_apply_cnt = same_device_diff_cardid_d7_apply_cnt = same_device_diff_cardid_d14_apply_cnt = same_device_diff_cardid_d30_apply_cnt = -1
        same_device_diff_phone_apply_cnt = same_device_diff_phone_d0_apply_cnt = same_device_diff_phone_d3_apply_cnt = same_device_diff_phone_d7_apply_cnt = same_device_diff_phone_d14_apply_cnt = same_device_diff_phone_d30_apply_cnt = -1
        same_device_diff_ip_apply_cnt = same_device_diff_ip_d0_apply_cnt = same_device_diff_ip_d3_apply_cnt = same_device_diff_ip_d7_apply_cnt = same_device_diff_ip_d14_apply_cnt = same_device_diff_ip_d30_apply_cnt = -1
    else:
        same_device_diff_account_apply_cnt= debts.loc[(user_bank_account  != order_user_bank_account)&(order_user_bank_account !='')]['user_bank_account'].nunique()
        same_device_diff_account_d0_apply_cnt= debts.loc[(user_bank_account  != order_user_bank_account)&((order_borrow_time - borrow_time).dt.days == 0)&(order_user_bank_account !='')]['user_bank_account'].nunique()
        same_device_diff_account_d3_apply_cnt= debts.loc[(user_bank_account  != order_user_bank_account)&((order_borrow_time - borrow_time).dt.days <= 3)&(order_user_bank_account !='')]['user_bank_account'].nunique()
        same_device_diff_account_d7_apply_cnt= debts.loc[(user_bank_account  != order_user_bank_account)&((order_borrow_time - borrow_time).dt.days <= 7)&(order_user_bank_account !='')]['user_bank_account'].nunique()
        same_device_diff_account_d14_apply_cnt= debts.loc[(user_bank_account  != order_user_bank_account)&((order_borrow_time - borrow_time).dt.days <= 14)&(order_user_bank_account !='')]['user_bank_account'].nunique()
        same_device_diff_account_d30_apply_cnt= debts.loc[(user_bank_account  != order_user_bank_account)&((order_borrow_time - borrow_time).dt.days <= 30)&(order_user_bank_account !='')]['user_bank_account'].nunique()
        
        same_device_diff_cardid_apply_cnt= debts.loc[(user_card_id != order_user_card_id)]['user_card_id'].nunique()
        same_device_diff_cardid_d0_apply_cnt= debts.loc[(user_card_id != order_user_card_id)&((order_borrow_time - borrow_time).dt.days == 0)&(user_card_id !='')]['user_card_id'].nunique()
        same_device_diff_cardid_d3_apply_cnt= debts.loc[(user_card_id != order_user_card_id)&((order_borrow_time - borrow_time).dt.days <= 3)&(user_card_id !='')]['user_card_id'].nunique()
        same_device_diff_cardid_d7_apply_cnt= debts.loc[(user_card_id != order_user_card_id)&((order_borrow_time - borrow_time).dt.days <= 7)&(user_card_id !='')]['user_card_id'].nunique() 
        same_device_diff_cardid_d14_apply_cnt= debts.loc[(user_card_id != order_user_card_id)&((order_borrow_time - borrow_time).dt.days <= 14)&(user_card_id !='')]['user_card_id'].nunique()
        same_device_diff_cardid_d30_apply_cnt= debts.loc[(user_card_id != order_user_card_id)&((order_borrow_time - borrow_time).dt.days <= 30)&(user_card_id !='')]['user_card_id'].nunique()
        
        same_device_diff_phone_apply_cnt= debts.loc[(user_phone != order_user_phone)]['user_phone'].nunique()
        same_device_diff_phone_d0_apply_cnt= debts.loc[(user_phone != order_user_phone)&((order_borrow_time - borrow_time).dt.days == 0)&(user_phone !='')]['user_phone'].nunique()  
        same_device_diff_phone_d3_apply_cnt= debts.loc[(user_phone != order_user_phone)&((order_borrow_time - borrow_time).dt.days <= 3)&(user_phone !='')]['user_phone'].nunique()
        same_device_diff_phone_d7_apply_cnt= debts.loc[(user_phone != order_user_phone)&((order_borrow_time - borrow_time).dt.days <= 7)&(user_phone !='')]['user_phone'].nunique()
        same_device_diff_phone_d14_apply_cnt= debts.loc[(user_phone != order_user_phone)&((order_borrow_time - borrow_time).dt.days <= 14)&(user_phone !='')]['user_phone'].nunique()
        same_device_diff_phone_d30_apply_cnt= debts.loc[(user_phone != order_user_phone)&((order_borrow_time - borrow_time).dt.days <= 30)&(user_phone !='')]['user_phone'].nunique()
        
        same_device_diff_ip_apply_cnt= debts.loc[(user_ip != order_user_ip)]['user_ip'].nunique()
        same_device_diff_ip_d0_apply_cnt= debts.loc[(user_ip != order_user_ip)&((order_borrow_time - borrow_time).dt.days == 0)&(user_ip !='')]['user_ip'].nunique()
        same_device_diff_ip_d3_apply_cnt= debts.loc[(user_ip != order_user_ip)&((order_borrow_time - borrow_time).dt.days <= 3)&(user_ip !='')]['user_ip'].nunique()
        same_device_diff_ip_d7_apply_cnt= debts.loc[(user_ip != order_user_ip)&((order_borrow_time - borrow_time).dt.days <= 7)&(user_ip !='')]['user_ip'].nunique()
        same_device_diff_ip_d14_apply_cnt= debts.loc[(user_ip != order_user_ip)&((order_borrow_time - borrow_time).dt.days <= 14)&(user_ip !='')]['user_ip'].nunique()
        same_device_diff_ip_d30_apply_cnt= debts.loc[(user_ip != order_user_ip)&((order_borrow_time - borrow_time).dt.days <= 30)&(user_ip !='')]['user_ip'].nunique()

    debts  = data.loc[(data['user_bank_account'] == debts_order['user_bank_account'].item())&(data['borrow_time'].dt.date <= debts_order['borrow_time'].dt.date.item())]
    app_name = debts.app_name 
    loan_amount = debts.loan_amount 
    user_phone = debts.user_phone
    borrow_time = debts.borrow_time.dt.date
    user_card_id = debts.user_card_id 
    user_ip = debts.user_ip
    user_device_id = debts.user_device_id
    user_type = debts.user_type
    status =debts.status 
    repay_yes_time = debts.repay_yes_time.dt.date
    repay_time = debts.repay_time.dt.date
    loan_completion_time = debts.loan_completion_time.dt.date
    user_bank_account = debts.user_bank_account
    if debts.empty:
        same_acount_diff_device_apply_cnt = same_acount_diff_device_d0_apply_cnt = same_acount_diff_device_d3_apply_cnt = same_acount_diff_device_d7_apply_cnt = same_acount_diff_device_d14_apply_cnt = same_acount_diff_device_d30_apply_cnt = -1
        same_acount_diff_cardid_apply_cnt = same_acount_diff_cardid_d0_apply_cnt = same_acount_diff_cardid_d3_apply_cnt = same_acount_diff_cardid_d7_apply_cnt = same_acount_diff_cardid_d14_apply_cnt = same_acount_diff_cardid_d30_apply_cnt = -1
        same_acount_diff_phone_apply_cnt = same_acount_diff_phone_d0_apply_cnt = same_acount_diff_phone_d3_apply_cnt = same_acount_diff_phone_d7_apply_cnt = same_acount_diff_phone_d14_apply_cnt = same_acount_diff_phone_d30_apply_cnt = -1
        same_acount_diff_ip_apply_cnt = same_acount_diff_ip_d0_apply_cnt = same_acount_diff_ip_d3_apply_cnt = same_acount_diff_ip_d7_apply_cnt = same_acount_diff_ip_d14_apply_cnt = same_acount_diff_ip_d30_apply_cnt = -1
    else:
        same_acount_diff_cardid_apply_cnt= debts.loc[(user_card_id != order_user_card_id)]['user_card_id'].nunique()
        same_acount_diff_cardid_d0_apply_cnt= debts.loc[(user_card_id != order_user_card_id)&((order_borrow_time - borrow_time).dt.days == 0)&(user_card_id !='')]['user_card_id'].nunique()
        same_acount_diff_cardid_d3_apply_cnt= debts.loc[(user_card_id != order_user_card_id)&((order_borrow_time - borrow_time).dt.days <= 3)&(user_card_id !='')]['user_card_id'].nunique()
        same_acount_diff_cardid_d7_apply_cnt= debts.loc[(user_card_id != order_user_card_id)&((order_borrow_time - borrow_time).dt.days <= 7)&(user_card_id !='')]['user_card_id'].nunique() 
        same_acount_diff_cardid_d14_apply_cnt= debts.loc[(user_card_id != order_user_card_id)&((order_borrow_time - borrow_time).dt.days <= 14)&(user_card_id !='')]['user_card_id'].nunique()
        same_acount_diff_cardid_d30_apply_cnt= debts.loc[(user_card_id != order_user_card_id)&((order_borrow_time - borrow_time).dt.days <= 30)&(user_card_id !='')]['user_card_id'].nunique()
        
        same_acount_diff_phone_apply_cnt= debts.loc[(user_phone != order_user_phone)]['user_phone'].nunique()
        same_acount_diff_phone_d0_apply_cnt= debts.loc[(user_phone != order_user_phone)&((order_borrow_time - borrow_time).dt.days == 0)&(user_phone !='')]['user_phone'].nunique()  
        same_acount_diff_phone_d3_apply_cnt= debts.loc[(user_phone != order_user_phone)&((order_borrow_time - borrow_time).dt.days <= 3)&(user_phone !='')]['user_phone'].nunique()
        same_acount_diff_phone_d7_apply_cnt= debts.loc[(user_phone != order_user_phone)&((order_borrow_time - borrow_time).dt.days <= 7)&(user_phone !='')]['user_phone'].nunique()
        same_acount_diff_phone_d14_apply_cnt= debts.loc[(user_phone != order_user_phone)&((order_borrow_time - borrow_time).dt.days <= 14)&(user_phone !='')]['user_phone'].nunique()
        same_acount_diff_phone_d30_apply_cnt= debts.loc[(user_phone != order_user_phone)&((order_borrow_time - borrow_time).dt.days <= 30)&(user_phone !='')]['user_phone'].nunique()
        
        same_acount_diff_ip_apply_cnt= debts.loc[(user_ip != order_user_ip)]['user_ip'].nunique()
        same_acount_diff_ip_d0_apply_cnt= debts.loc[(user_ip != order_user_ip)&((order_borrow_time - borrow_time).dt.days == 0)&(user_ip !='')]['user_ip'].nunique()
        same_acount_diff_ip_d3_apply_cnt= debts.loc[(user_ip != order_user_ip)&((order_borrow_time - borrow_time).dt.days <= 3)&(user_ip !='')]['user_ip'].nunique()
        same_acount_diff_ip_d7_apply_cnt= debts.loc[(user_ip != order_user_ip)&((order_borrow_time - borrow_time).dt.days <= 7)&(user_ip !='')]['user_ip'].nunique()
        same_acount_diff_ip_d14_apply_cnt= debts.loc[(user_ip != order_user_ip)&((order_borrow_time - borrow_time).dt.days <= 14)&(user_ip !='')]['user_ip'].nunique()
        same_acount_diff_ip_d30_apply_cnt= debts.loc[(user_ip != order_user_ip)&((order_borrow_time - borrow_time).dt.days <= 30)&(user_ip !='')]['user_ip'].nunique()
        
        same_acount_diff_device_apply_cnt= debts.loc[(user_device_id != order_user_device_id)&(user_device_id!='00000000-0000-0000-0000-000000000000')]['user_device_id'].nunique()
        same_acount_diff_device_d0_apply_cnt= debts.loc[(user_device_id  != order_user_device_id)&len(user_device_id)==36 & (user_device_id!='00000000-0000-0000-0000-000000000000') & ((order_borrow_time - borrow_time).dt.days == 0)]['user_device_id'].nunique()
        same_acount_diff_device_d3_apply_cnt= debts.loc[(user_device_id  != order_user_device_id)&len(user_device_id)==36 & (user_device_id!='00000000-0000-0000-0000-000000000000') & ((order_borrow_time - borrow_time).dt.days <= 3)]['user_device_id'].nunique()    
        same_acount_diff_device_d7_apply_cnt= debts.loc[(user_device_id  != order_user_device_id)&len(user_device_id)==36 & (user_device_id!='00000000-0000-0000-0000-000000000000') & ((order_borrow_time - borrow_time).dt.days <= 7)]['user_device_id'].nunique()
        same_acount_diff_device_d14_apply_cnt= debts.loc[(user_device_id  != order_user_device_id)&len(user_device_id)==36 & (user_device_id!='00000000-0000-0000-0000-000000000000') & ((order_borrow_time - borrow_time).dt.days <= 14)]['user_device_id'].nunique()
        same_acount_diff_device_d30_apply_cnt= debts.loc[(user_device_id  != order_user_device_id)&len(user_device_id)==36 & (user_device_id!='00000000-0000-0000-0000-000000000000') & ((order_borrow_time - borrow_time).dt.days <= 30)]['user_device_id'].nunique() 


     
    df= {'order_id': order_id, 
                  'same_phone_diff_device_apply_cnt':same_phone_diff_device_apply_cnt,
                    'same_phone_diff_device_d0_apply_cnt':same_phone_diff_device_d0_apply_cnt,
                    'same_phone_diff_device_d3_apply_cnt':same_phone_diff_device_d3_apply_cnt,
                    'same_phone_diff_device_d7_apply_cnt':same_phone_diff_device_d7_apply_cnt,
                    'same_phone_diff_device_d14_apply_cnt':same_phone_diff_device_d14_apply_cnt,
                    'same_phone_diff_device_d30_apply_cnt':same_phone_diff_device_d30_apply_cnt,
                    'same_phone_diff_account_apply_cnt':same_phone_diff_account_apply_cnt,
                    'same_phone_diff_account_d0_apply_cnt':same_phone_diff_account_d0_apply_cnt,
                    'same_phone_diff_account_d3_apply_cnt':same_phone_diff_account_d3_apply_cnt,
                    'same_phone_diff_account_d7_apply_cnt':same_phone_diff_account_d7_apply_cnt,
                    'same_phone_diff_account_d14_apply_cnt':same_phone_diff_account_d14_apply_cnt,
                    'same_phone_diff_account_d30_apply_cnt':same_phone_diff_account_d30_apply_cnt,
                    'same_phone_diff_cardid_apply_cnt':same_phone_diff_cardid_apply_cnt,
                    'same_phone_diff_cardid_d0_apply_cnt':same_phone_diff_cardid_d0_apply_cnt,
                    'same_phone_diff_cardid_d3_apply_cnt':same_phone_diff_cardid_d3_apply_cnt,
                    'same_phone_diff_cardid_d7_apply_cnt':same_phone_diff_cardid_d7_apply_cnt,  
                    'same_phone_diff_cardid_d14_apply_cnt':same_phone_diff_cardid_d14_apply_cnt,
                    'same_phone_diff_cardid_d30_apply_cnt':same_phone_diff_cardid_d30_apply_cnt,
                    'same_phone_diff_ip_apply_cnt':same_phone_diff_ip_apply_cnt,
                    'same_phone_diff_ip_d0_apply_cnt':same_phone_diff_ip_d0_apply_cnt,
                    'same_phone_diff_ip_d3_apply_cnt':same_phone_diff_ip_d3_apply_cnt,
                    'same_phone_diff_ip_d7_apply_cnt':same_phone_diff_ip_d7_apply_cnt,
                    'same_phone_diff_ip_d14_apply_cnt':same_phone_diff_ip_d14_apply_cnt,
                    'same_phone_diff_ip_d30_apply_cnt':same_phone_diff_ip_d30_apply_cnt,
                   
                    'same_cardid_diff_device_apply_cnt':same_cardid_diff_device_apply_cnt,
                    'same_cardid_diff_device_d0_apply_cnt':same_cardid_diff_device_d0_apply_cnt,
                    'same_cardid_diff_device_d3_apply_cnt':same_cardid_diff_device_d3_apply_cnt,
                    'same_cardid_diff_device_d7_apply_cnt':same_cardid_diff_device_d7_apply_cnt,
                    'same_cardid_diff_device_d14_apply_cnt':same_cardid_diff_device_d14_apply_cnt,
                    'same_cardid_diff_device_d30_apply_cnt':same_cardid_diff_device_d30_apply_cnt,
                    'same_cardid_diff_account_apply_cnt':same_cardid_diff_account_apply_cnt,
                    'same_cardid_diff_account_d0_apply_cnt':same_cardid_diff_account_d0_apply_cnt,
                    'same_cardid_diff_account_d3_apply_cnt':same_cardid_diff_account_d3_apply_cnt,
                    'same_cardid_diff_account_d7_apply_cnt':same_cardid_diff_account_d7_apply_cnt,
                    'same_cardid_diff_account_d14_apply_cnt':same_cardid_diff_account_d14_apply_cnt,
                    'same_cardid_diff_account_d30_apply_cnt':same_cardid_diff_account_d30_apply_cnt,
                    'same_cardid_diff_ip_apply_cnt':same_cardid_diff_ip_apply_cnt,
                    'same_cardid_diff_ip_d0_apply_cnt':same_cardid_diff_ip_d0_apply_cnt,    
                    'same_cardid_diff_ip_d3_apply_cnt':same_cardid_diff_ip_d3_apply_cnt,
                    'same_cardid_diff_ip_d7_apply_cnt':same_cardid_diff_ip_d7_apply_cnt,
                    'same_cardid_diff_ip_d14_apply_cnt':same_cardid_diff_ip_d14_apply_cnt,
                    'same_cardid_diff_ip_d30_apply_cnt':same_cardid_diff_ip_d30_apply_cnt,
                    'same_cardid_diff_phone_apply_cnt':same_cardid_diff_phone_apply_cnt,
                    'same_cardid_diff_phone_d0_apply_cnt':same_cardid_diff_phone_d0_apply_cnt,
                    'same_cardid_diff_phone_d3_apply_cnt':same_cardid_diff_phone_d3_apply_cnt,
                    'same_cardid_diff_phone_d7_apply_cnt':same_cardid_diff_phone_d7_apply_cnt,
                    'same_cardid_diff_phone_d14_apply_cnt':same_cardid_diff_phone_d14_apply_cnt,
                    'same_cardid_diff_phone_d30_apply_cnt':same_cardid_diff_phone_d30_apply_cnt,
                    'same_ip_diff_device_apply_cnt':same_ip_diff_device_apply_cnt,
                    'same_ip_diff_device_d0_apply_cnt':same_ip_diff_device_d0_apply_cnt,
                    'same_ip_diff_device_d3_apply_cnt':same_ip_diff_device_d3_apply_cnt,
                    'same_ip_diff_device_d7_apply_cnt':same_ip_diff_device_d7_apply_cnt,
                    'same_ip_diff_device_d14_apply_cnt':same_ip_diff_device_d14_apply_cnt,
                    'same_ip_diff_device_d30_apply_cnt':same_ip_diff_device_d30_apply_cnt,
                    'same_ip_diff_account_apply_cnt':same_ip_diff_account_apply_cnt,
                    'same_ip_diff_account_d0_apply_cnt':same_ip_diff_account_d0_apply_cnt,
                    'same_ip_diff_account_d3_apply_cnt':same_ip_diff_account_d3_apply_cnt,
                    'same_ip_diff_account_d7_apply_cnt':same_ip_diff_account_d7_apply_cnt,
                    'same_ip_diff_account_d14_apply_cnt':same_ip_diff_account_d14_apply_cnt,
                    'same_ip_diff_account_d30_apply_cnt':same_ip_diff_account_d30_apply_cnt,
                    'same_ip_diff_cardid_apply_cnt':same_ip_diff_cardid_apply_cnt,
                    'same_ip_diff_cardid_d0_apply_cnt':same_ip_diff_cardid_d0_apply_cnt,
                    'same_ip_diff_cardid_d3_apply_cnt':same_ip_diff_cardid_d3_apply_cnt,
                    'same_ip_diff_cardid_d7_apply_cnt':same_ip_diff_cardid_d7_apply_cnt,
                    'same_ip_diff_cardid_d14_apply_cnt':same_ip_diff_cardid_d14_apply_cnt,
                    'same_ip_diff_cardid_d30_apply_cnt':same_ip_diff_cardid_d30_apply_cnt,
                    'same_ip_diff_phone_apply_cnt':same_ip_diff_phone_apply_cnt,    
                    'same_ip_diff_phone_d0_apply_cnt':same_ip_diff_phone_d0_apply_cnt,
                    'same_ip_diff_phone_d3_apply_cnt':same_ip_diff_phone_d3_apply_cnt,
                    'same_ip_diff_phone_d7_apply_cnt':same_ip_diff_phone_d7_apply_cnt,
                    'same_ip_diff_phone_d14_apply_cnt':same_ip_diff_phone_d14_apply_cnt,
                    'same_ip_diff_phone_d30_apply_cnt':same_ip_diff_phone_d30_apply_cnt,
                    'same_device_diff_account_apply_cnt':same_device_diff_account_apply_cnt,
                    'same_device_diff_account_d0_apply_cnt':same_device_diff_account_d0_apply_cnt,
                    'same_device_diff_account_d3_apply_cnt':same_device_diff_account_d3_apply_cnt,
                    'same_device_diff_account_d7_apply_cnt':same_device_diff_account_d7_apply_cnt,
                    'same_device_diff_account_d14_apply_cnt':same_device_diff_account_d14_apply_cnt,
                    'same_device_diff_account_d30_apply_cnt':same_device_diff_account_d30_apply_cnt,
                    'same_device_diff_cardid_apply_cnt':same_device_diff_cardid_apply_cnt,
                    'same_device_diff_cardid_d0_apply_cnt':same_device_diff_cardid_d0_apply_cnt,
                    'same_device_diff_cardid_d3_apply_cnt':same_device_diff_cardid_d3_apply_cnt,    
                    'same_device_diff_cardid_d7_apply_cnt':same_device_diff_cardid_d7_apply_cnt,
                    'same_device_diff_cardid_d14_apply_cnt':same_device_diff_cardid_d14_apply_cnt,
                    'same_device_diff_cardid_d30_apply_cnt':same_device_diff_cardid_d30_apply_cnt,
                    'same_device_diff_phone_apply_cnt':same_device_diff_phone_apply_cnt,
                    'same_device_diff_phone_d0_apply_cnt':same_device_diff_phone_d0_apply_cnt,
                    'same_device_diff_phone_d3_apply_cnt':same_device_diff_phone_d3_apply_cnt,
                    'same_device_diff_phone_d7_apply_cnt':same_device_diff_phone_d7_apply_cnt,
                    'same_device_diff_phone_d14_apply_cnt':same_device_diff_phone_d14_apply_cnt,
                    'same_device_diff_phone_d30_apply_cnt':same_device_diff_phone_d30_apply_cnt,
                    'same_device_diff_ip_apply_cnt':same_device_diff_ip_apply_cnt,
                    'same_device_diff_ip_d0_apply_cnt':same_device_diff_ip_d0_apply_cnt,    
                    'same_device_diff_ip_d3_apply_cnt':same_device_diff_ip_d3_apply_cnt,
                    'same_device_diff_ip_d7_apply_cnt':same_device_diff_ip_d7_apply_cnt,
                    'same_device_diff_ip_d14_apply_cnt':same_device_diff_ip_d14_apply_cnt,
                    'same_device_diff_ip_d30_apply_cnt':same_device_diff_ip_d30_apply_cnt,
                    'same_acount_diff_cardid_apply_cnt':same_acount_diff_cardid_apply_cnt,
                    'same_acount_diff_cardid_d0_apply_cnt':same_acount_diff_cardid_d0_apply_cnt,
                    'same_acount_diff_cardid_d3_apply_cnt':same_acount_diff_cardid_d3_apply_cnt,
                    'same_acount_diff_cardid_d7_apply_cnt':same_acount_diff_cardid_d7_apply_cnt,
                    'same_acount_diff_cardid_d14_apply_cnt':same_acount_diff_cardid_d14_apply_cnt,
                    'same_acount_diff_cardid_d30_apply_cnt':same_acount_diff_cardid_d30_apply_cnt,
                    'same_acount_diff_phone_apply_cnt':same_acount_diff_phone_apply_cnt,
                    'same_acount_diff_phone_d0_apply_cnt':same_acount_diff_phone_d0_apply_cnt,
                    'same_acount_diff_phone_d3_apply_cnt':same_acount_diff_phone_d3_apply_cnt,
                    'same_acount_diff_phone_d7_apply_cnt':same_acount_diff_phone_d7_apply_cnt,
                    'same_acount_diff_phone_d14_apply_cnt':same_acount_diff_phone_d14_apply_cnt,
                    'same_acount_diff_phone_d30_apply_cnt':same_acount_diff_phone_d30_apply_cnt,
                    'same_acount_diff_ip_apply_cnt':same_acount_diff_ip_apply_cnt,
                    'same_acount_diff_ip_d0_apply_cnt':same_acount_diff_ip_d0_apply_cnt,
                    'same_acount_diff_ip_d3_apply_cnt':same_acount_diff_ip_d3_apply_cnt,
                    'same_acount_diff_ip_d7_apply_cnt':same_acount_diff_ip_d7_apply_cnt,
                    'same_acount_diff_ip_d14_apply_cnt':same_acount_diff_ip_d14_apply_cnt,
                    'same_acount_diff_ip_d30_apply_cnt':same_acount_diff_ip_d30_apply_cnt,
                    'same_acount_diff_device_apply_cnt':same_acount_diff_device_apply_cnt,
                    'same_acount_diff_device_d0_apply_cnt':same_acount_diff_device_d0_apply_cnt,
                    'same_acount_diff_device_d3_apply_cnt':same_acount_diff_device_d3_apply_cnt,    
                    'same_acount_diff_device_d7_apply_cnt':same_acount_diff_device_d7_apply_cnt,
                    'same_acount_diff_device_d14_apply_cnt':same_acount_diff_device_d14_apply_cnt,
                    'same_acount_diff_device_d30_apply_cnt':same_acount_diff_device_d30_apply_cnt
         
             }
    df_data = pd.DataFrame([df])
    df_data.fillna(-1, inplace = True)
    return df_data 

