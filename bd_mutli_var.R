dwd_order<-read.csv(file='bd_dwd_order.csv',sep=',',he=T,colClasses = c(order_id = "character",user_card_id='character',user_phone='character'))


# cash_bd_order_rename_reg%>%
#   select(order_id,user_id,app_name,status,user_name,user_type,user_phone,user_card_id,user_device_id,user_ip,user_bank_account,contract_amount,loan_amount,
#          borrow_time,loan_completion_time,repay_time,repay_yes_time,payed_amount,late_days)->dwd_order



library(tidyverse)
library(data.table)
library(RPostgreSQL)
# library(pool)
##同手机号 申请，拒绝，逾期
##同身份证号 申请，拒绝，逾期
##同设备号 申请，拒绝，逾期
##同ip 申请，拒绝，逾期
##同银行卡 申请，拒绝，逾期
## 欺诈类：同手机号不同证件号 同手机号 不同设备号  同手机号不同银行卡；同手机号不同姓名
## 欺诈类：同证件号不同手机号 ；同证件号不同设备号；同证件号不同银行卡号；同证件号不同姓名
## 欺诈类：同设备号不同手机号；同设备号不同证件号；同设备号不同银行卡号；同设备号不同姓名
## 欺诈类：同姓名不同手机号；同姓名不同证件号；同姓名不同银行卡号；同姓名不同设备号

##customer_type 同手机号首次放款天数 在贷笔数


##样本表
dwd_order%>%
  as_tibble()%>%
  filter(status>=8)->due_order


##取出订单号 2779124987605811210

# pool <- dbPool(
#   drv = PostgreSQL(),  # 或 Postgres()
#   dbname = "test",
#   host = "8.210.75.205",
#   port = 13343,
#   user = "BASIC$root",
#   password = "WCjak3$RQnf3ST",
#   options="-c search_path=bengal_test",
#   minSize = 1,
#   maxSize = 5,
#   idleTimeout = 3600000  # 1小时空闲超时（毫秒）
# )
# poolClose(pool)


# due_order[80000:85000,]->sample_order



card_multi_var_frame<-data.frame()
phone_multi_var_frame<-data.frame()
device_multi_var_frame<-data.frame()
ip_multi_var_frame<-data.frame()
account_multi_var_frame<-data.frame()

## 筛选出来正好的join字段，然后 用到期表join 订单总表查询对应的数据，然后再做条件统计

# due_order[80000:85000,]->sample_order

i=1
# num=0

while(i<dim(due_order)[1]){
   num=min((i+2000),dim(due_order)[1])
  due_order[i:min((i+2000),dim(due_order)[1]),]->sample_order
##手机号特征
sample_order%>%
  as_tibble()%>%
  rename_with(~ paste0("tar_", .), everything())%>%
  # rename(user_phone=tar_user_phone)%>%
  select(tar_order_id,tar_user_id,tar_app_name,tar_user_name,tar_user_ip,tar_user_card_id,tar_user_phone,
         tar_user_device_id,tar_user_bank_account,tar_borrow_time)%>%
    filter(tar_user_phone!='')%>%
  left_join(dwd_order%>%select(user_id,order_id,user_name,user_phone,user_card_id,user_ip,user_device_id,user_bank_account,
                               borrow_time,user_type,repay_yes_time,app_name,loan_completion_time,repay_time,borrow_time,status),
            by=c('tar_user_phone'='user_phone') )->sample_phone_order_join


sample_phone_order_join%>%
  filter(borrow_time<=tar_borrow_time)%>% ##剔除申请时间在计算订单之后的记录
  # filter(order_id!=tar_order_id)%>% ## 剔除订单号相同的记录  ## 回溯订单状态 添加时间切片
  mutate(is_repay=if_else(repay_yes_time>tar_borrow_time | repay_yes_time=='',0,1),
         is_due=if_else(as.Date(repay_time)<=as.Date(tar_borrow_time),1,0),
         apply_diff=difftime(as.Date(tar_borrow_time),as.Date(borrow_time),units='day')%>%as.numeric(),
         due_diff=difftime(as.Date(tar_borrow_time),as.Date(repay_time),units='day')%>%as.numeric(),
         disburse_diff=difftime(as.Date(tar_borrow_time),as.Date(loan_completion_time),units='day')%>%as.numeric(),
         
  )->sample_phone_dwd_order_tib


###手机号多头
sample_phone_dwd_order_tib%>%
  as_tibble()%>%
group_by(tar_order_id)%>%
  summarise(
    
    ##同手机号平台多头
    phone_nex_plat_apply_cnt=if_else(first(tar_user_phone)=='',-1,uniqueN(app_name[user_id!=tar_user_id & app_name!=tar_app_name]) ),
    phone_nex_plat_d0_apply_cnt=if_else(first(tar_user_phone)=='',-1,uniqueN(app_name[user_id!=tar_user_id & app_name!=tar_app_name & apply_diff==0]) ),
    phone_nex_plat_d3_apply_cnt=if_else(first(tar_user_phone)=='',-1,uniqueN(app_name[user_id!=tar_user_id & app_name!=tar_app_name & apply_diff<=3]) ),
    phone_nex_plat_d7_apply_cnt=if_else(first(tar_user_phone)=='',-1,uniqueN(app_name[user_id!=tar_user_id & app_name!=tar_app_name & apply_diff<=7]) ),
    phone_nex_plat_d14_apply_cnt=if_else(first(tar_user_phone)=='',-1,uniqueN(app_name[user_id!=tar_user_id & app_name!=tar_app_name & apply_diff<=14]) ),
    phone_nex_plat_d30_apply_cnt=if_else(first(tar_user_phone)=='',-1,uniqueN(app_name[user_id!=tar_user_id & app_name!=tar_app_name & apply_diff<=30]) ),
    
    phone_nex_plat_reject_cnt=if_else(first(tar_user_phone)=='',-1,uniqueN(app_name[user_id!=tar_user_id & app_name!=tar_app_name & status==3]) ),
    phone_nex_plat_d0_reject_cnt=if_else(first(tar_user_phone)=='',-1,uniqueN(app_name[user_id!=tar_user_id & app_name!=tar_app_name & status==3 & apply_diff==0]) ),    
    phone_nex_plat_d3_reject_cnt=if_else(first(tar_user_phone)=='',-1,uniqueN(app_name[user_id!=tar_user_id & app_name!=tar_app_name & status==3 & apply_diff<=3]) ),    
    phone_nex_plat_d7_reject_cnt=if_else(first(tar_user_phone)=='',-1,uniqueN(app_name[user_id!=tar_user_id & app_name!=tar_app_name & status==3 & apply_diff<=7]) ),     
    phone_nex_plat_d14_reject_cnt=if_else(first(tar_user_phone)=='',-1,uniqueN(app_name[user_id!=tar_user_id & app_name!=tar_app_name & status==3 & apply_diff<=14]) ),    
    phone_nex_plat_d30_reject_cnt=if_else(first(tar_user_phone)=='',-1,uniqueN(app_name[user_id!=tar_user_id & app_name!=tar_app_name & status==3 & apply_diff<=30]) ),   
    
    phone_nex_plat_disburse_cnt= if_else(first(tar_user_phone)=='',-1,uniqueN(app_name[user_id!=tar_user_id & app_name!=tar_app_name & status>=8]) ),
    phone_nex_plat_d0_disburse_cnt= if_else(first(tar_user_phone)=='',-1,uniqueN(app_name[user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff==0]) ),
    phone_nex_plat_d3_disburse_cnt= if_else(first(tar_user_phone)=='',-1,uniqueN(app_name[user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff<=3]) ),
    phone_nex_plat_d7_disburse_cnt= if_else(first(tar_user_phone)=='',-1,uniqueN(app_name[user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff<=7]) ),
    phone_nex_plat_d14_disburse_cnt= if_else(first(tar_user_phone)=='',-1,uniqueN(app_name[user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff<=14]) ),
    phone_nex_plat_d30_disburse_cnt= if_else(first(tar_user_phone)=='',-1,uniqueN(app_name[user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff<=30]) ),    
    
    phone_nex_plat_overdue_cnt= if_else(first(tar_user_phone)=='',-1,uniqueN(app_name[user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & is_due==1 & is_repay==0]) ),
    phone_nex_plat_d0_overdue_cnt= if_else(first(tar_user_phone)=='',-1,uniqueN(app_name[user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff==0]) ),
    phone_nex_plat_d3_overdue_cnt= if_else(first(tar_user_phone)=='',-1,uniqueN(app_name[user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff<=3]) ),
    phone_nex_plat_d7_overdue_cnt= if_else(first(tar_user_phone)=='',-1,uniqueN(app_name[user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff<=7]) ),
    phone_nex_plat_d14_overdue_cnt= if_else(first(tar_user_phone)=='',-1,uniqueN(app_name[user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff<=14]) ),
    phone_nex_plat_d30_overdue_cnt= if_else(first(tar_user_phone)=='',-1,uniqueN(app_name[user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff<=30]) ),    
    
    ##同手机号平台多头
    phone_nex_plat_apply_order_cnt=if_else(first(tar_user_phone)=='',-1,uniqueN(order_id[user_id!=tar_user_id & app_name!=tar_app_name]) ),
    phone_nex_plat_d0_apply_order_cnt=if_else(first(tar_user_phone)=='',-1,uniqueN(order_id[user_id!=tar_user_id & app_name!=tar_app_name & apply_diff==0]) ),
    phone_nex_plat_d3_apply_order_cnt=if_else(first(tar_user_phone)=='',-1,uniqueN(order_id[user_id!=tar_user_id & app_name!=tar_app_name & apply_diff<=3]) ),
    phone_nex_plat_d7_apply_order_cnt=if_else(first(tar_user_phone)=='',-1,uniqueN(order_id[user_id!=tar_user_id & app_name!=tar_app_name & apply_diff<=7]) ),
    phone_nex_plat_d14_apply_order_cnt=if_else(first(tar_user_phone)=='',-1,uniqueN(order_id[user_id!=tar_user_id & app_name!=tar_app_name & apply_diff<=14]) ),
    phone_nex_plat_d30_apply_order_cnt=if_else(first(tar_user_phone)=='',-1,uniqueN(order_id[user_id!=tar_user_id & app_name!=tar_app_name & apply_diff<=30]) ),
    
    phone_nex_plat_reject_order_cnt=if_else(first(tar_user_phone)=='',-1,uniqueN(order_id[user_id!=tar_user_id & app_name!=tar_app_name & status==3]) ),
    phone_nex_plat_d0_reject_order_cnt=if_else(first(tar_user_phone)=='',-1,uniqueN(order_id[user_id!=tar_user_id & app_name!=tar_app_name & status==3 & apply_diff==0]) ),    
    phone_nex_plat_d3_reject_order_cnt=if_else(first(tar_user_phone)=='',-1,uniqueN(order_id[user_id!=tar_user_id & app_name!=tar_app_name & status==3 & apply_diff<=3]) ),    
    phone_nex_plat_d7_reject_order_cnt=if_else(first(tar_user_phone)=='',-1,uniqueN(order_id[user_id!=tar_user_id & app_name!=tar_app_name & status==3 & apply_diff<=7]) ),     
    phone_nex_plat_d14_reject_order_cnt=if_else(first(tar_user_phone)=='',-1,uniqueN(order_id[user_id!=tar_user_id & app_name!=tar_app_name & status==3 & apply_diff<=14]) ),    
    phone_nex_plat_d30_reject_order_cnt=if_else(first(tar_user_phone)=='',-1,uniqueN(order_id[user_id!=tar_user_id & app_name!=tar_app_name & status==3 & apply_diff<=30]) ),   
    
    phone_nex_plat_disburse_order_cnt= if_else(first(tar_user_phone)=='',-1,uniqueN(order_id[user_id!=tar_user_id & app_name!=tar_app_name & status>=8]) ),
    phone_nex_plat_d0_disburse_order_cnt= if_else(first(tar_user_phone)=='',-1,uniqueN(order_id[user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff==0]) ),
    phone_nex_plat_d3_disburse_order_cnt= if_else(first(tar_user_phone)=='',-1,uniqueN(order_id[user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff<=3]) ),
    phone_nex_plat_d7_disburse_order_cnt= if_else(first(tar_user_phone)=='',-1,uniqueN(order_id[user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff<=7]) ),
    phone_nex_plat_d14_disburse_order_cnt= if_else(first(tar_user_phone)=='',-1,uniqueN(order_id[user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff<=14]) ),
    phone_nex_plat_d30_disburse_order_cnt= if_else(first(tar_user_phone)=='',-1,uniqueN(order_id[user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff<=30]) ),    
    
    phone_nex_plat_overdue_order_cnt= if_else(first(tar_user_phone)=='',-1,uniqueN(order_id[user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & is_due==1 & is_repay==0]) ),
    phone_nex_plat_d0_overdue_order_cnt= if_else(first(tar_user_phone)=='',-1,uniqueN(order_id[user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff==0]) ),
    phone_nex_plat_d3_overdue_order_cnt= if_else(first(tar_user_phone)=='',-1,uniqueN(order_id[user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff<=3]) ),
    phone_nex_plat_d7_overdue_order_cnt= if_else(first(tar_user_phone)=='',-1,uniqueN(order_id[user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff<=7]) ),
    phone_nex_plat_d14_overdue_order_cnt= if_else(first(tar_user_phone)=='',-1,uniqueN(order_id[user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff<=14]) ),
    phone_nex_plat_d30_overdue_order_cnt= if_else(first(tar_user_phone)=='',-1,uniqueN(order_id[user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff<=30]) ),    
    
    same_phone_diff_device_apply_cnt=if_else(first(tar_user_phone)=='',-1, uniqueN(user_device_id[  tar_user_device_id!=user_device_id & (str_length(user_device_id)==36 & user_device_id!='00000000-0000-0000-0000-000000000000')])),
    same_phone_diff_device_d0_apply_cnt=if_else(first(tar_user_phone)=='',-1, uniqueN(user_device_id[ apply_diff==0 &  tar_user_device_id!=user_device_id & (str_length(user_device_id)==36 & user_device_id!='00000000-0000-0000-0000-000000000000')])),
    same_phone_diff_device_d3_apply_cnt=if_else(first(tar_user_phone)=='',-1, uniqueN(user_device_id[ apply_diff<=3 &  tar_user_device_id!=user_device_id & (str_length(user_device_id)==36 & user_device_id!='00000000-0000-0000-0000-000000000000')])),
    same_phone_diff_device_d7_apply_cnt=if_else(first(tar_user_phone)=='',-1, uniqueN(user_device_id[ apply_diff<=7 &  tar_user_device_id!=user_device_id & (str_length(user_device_id)==36 & user_device_id!='00000000-0000-0000-0000-000000000000')])),
    same_phone_diff_device_d14_apply_cnt=if_else(first(tar_user_phone)=='',-1, uniqueN(user_device_id[ apply_diff<=14 &  tar_user_device_id!=user_device_id & (str_length(user_device_id)==36 & user_device_id!='00000000-0000-0000-0000-000000000000')])),
    same_phone_diff_device_d30_apply_cnt=if_else(first(tar_user_phone)=='',-1, uniqueN(user_device_id[ apply_diff<=30 &  tar_user_device_id!=user_device_id & (str_length(user_device_id)==36 & user_device_id!='00000000-0000-0000-0000-000000000000')])),
    
    same_phone_diff_account_apply_cnt=if_else(first(tar_user_phone)=='',-1, uniqueN(user_bank_account[  tar_user_bank_account!=user_bank_account & user_bank_account!=''])),
    same_phone_diff_account_d0_apply_cnt=if_else(first(tar_user_phone)=='',-1, uniqueN(user_bank_account[ apply_diff==0 &  tar_user_bank_account!=user_bank_account & user_bank_account!=''])),    
    same_phone_diff_account_d3_apply_cnt=if_else(first(tar_user_phone)=='',-1, uniqueN(user_bank_account[ apply_diff<=3 &  tar_user_bank_account!=user_bank_account & user_bank_account!=''])),    
    same_phone_diff_account_d7_apply_cnt=if_else(first(tar_user_phone)=='',-1, uniqueN(user_bank_account[ apply_diff<=7 &  tar_user_bank_account!=user_bank_account & user_bank_account!=''])),    
    same_phone_diff_account_d14_apply_cnt=if_else(first(tar_user_phone)=='',-1, uniqueN(user_bank_account[ apply_diff<=14 &  tar_user_bank_account!=user_bank_account & user_bank_account!=''])),    
    same_phone_diff_account_d30_apply_cnt=if_else(first(tar_user_phone)=='',-1, uniqueN(user_bank_account[ apply_diff<=30 &  tar_user_bank_account!=user_bank_account & user_bank_account!=''])),    
    
    same_phone_diff_cardid_apply_cnt=if_else(first(tar_user_phone)=='',-1, uniqueN(user_card_id[  tar_user_card_id!=user_card_id & user_card_id!=''])),
    same_phone_diff_cardid_d0_apply_cnt=if_else(first(tar_user_phone)=='',-1, uniqueN(user_card_id[ apply_diff==0 &  tar_user_card_id!=user_card_id & user_card_id!=''])),    
    same_phone_diff_cardid_d3_apply_cnt=if_else(first(tar_user_phone)=='',-1, uniqueN(user_card_id[ apply_diff<=3 &  tar_user_card_id!=user_card_id & user_card_id!=''])),    
    same_phone_diff_cardid_d7_apply_cnt=if_else(first(tar_user_phone)=='',-1, uniqueN(user_card_id[ apply_diff<=7 &  tar_user_card_id!=user_card_id & user_card_id!=''])),    
    same_phone_diff_cardid_d14_apply_cnt=if_else(first(tar_user_phone)=='',-1, uniqueN(user_card_id[ apply_diff<=14 &  tar_user_card_id!=user_card_id & user_card_id!=''])),    
    same_phone_diff_cardid_d30_apply_cnt=if_else(first(tar_user_phone)=='',-1, uniqueN(user_card_id[ apply_diff<=30 &  tar_user_card_id!=user_card_id & user_card_id!=''])),
    
    same_phone_diff_ip_apply_cnt=if_else(first(tar_user_phone)=='',-1, uniqueN(user_ip[  tar_user_ip!=user_ip & user_ip!=''])),
    same_phone_diff_ip_d0_apply_cnt=if_else(first(tar_user_phone)=='',-1, uniqueN(user_ip[ apply_diff==0 &  tar_user_ip!=user_ip & user_ip!=''])),    
    same_phone_diff_ip_d3_apply_cnt=if_else(first(tar_user_phone)=='',-1, uniqueN(user_ip[ apply_diff<=3 &  tar_user_ip!=user_ip & user_ip!=''])),    
    same_phone_diff_ip_d7_apply_cnt=if_else(first(tar_user_phone)=='',-1, uniqueN(user_ip[ apply_diff<=7 &  tar_user_ip!=user_ip & user_ip!=''])),    
    same_phone_diff_ip_d14_apply_cnt=if_else(first(tar_user_phone)=='',-1, uniqueN(user_ip[ apply_diff<=14 &  tar_user_ip!=user_ip & user_ip!=''])),    
    same_phone_diff_ip_d30_apply_cnt=if_else(first(tar_user_phone)=='',-1, uniqueN(user_ip[ apply_diff<=30 &  tar_user_ip!=user_ip & user_ip!=''])),  
    
    phone_first_disburse_gap=if_else(max(disburse_diff,na.rm = T)%>%is.infinite()|max(disburse_diff,na.rm = T)%>%is.na(),-1,max(disburse_diff,na.rm = T)),
    phone_onloan_cnt=uniqueN(app_name[loan_completion_time<=tar_borrow_time & status>=8 & is_repay==0]),
    phone_onloan_new_cnt=uniqueN(app_name[ loan_completion_time<=tar_borrow_time & status>=8 & is_repay==0 & user_type==0]),
    
    phone_overdue_cnt=uniqueN(app_name[is_due==1 & is_repay==0]),
    phone_repay_cnt=uniqueN(app_name[is_due==1 & is_repay==1]),
    phone_repay_order_cnt=uniqueN(order_id[is_due==1 & is_repay==1])
    
      )%>%
  mutate(time=Sys.time())->phone_multi_var

phone_multi_var_frame<<-bind_rows(phone_multi_var_frame,phone_multi_var)
##证件号多头

##证件号特征
sample_order%>%
  rename_with(~ paste0("tar_", .), everything())%>%
  # rename(user_phone=tar_user_phone)%>%
  select(tar_order_id,tar_user_id,tar_app_name,tar_user_name,tar_user_ip,tar_user_card_id,tar_user_phone,
         tar_user_device_id,tar_user_bank_account,tar_borrow_time)%>%
  filter(tar_user_card_id!='')%>%
  left_join(dwd_order%>%select(user_id,order_id,user_name,user_phone,user_card_id,user_ip,user_device_id,user_bank_account,
                               borrow_time,user_type,repay_yes_time,app_name,loan_completion_time,repay_time,borrow_time,status),
            by=c('tar_user_card_id'='user_card_id') )->sample_cardid_order_join


sample_cardid_order_join%>%
  filter(borrow_time<=tar_borrow_time)%>% ##剔除申请时间在计算订单之后的记录
  # filter(order_id!=tar_order_id)%>% ## 剔除订单号相同的记录  ## 回溯订单状态 添加时间切片
  mutate(is_repay=if_else(repay_yes_time>tar_borrow_time | repay_yes_time=='',0,1),
         is_due=if_else(as.Date(repay_time)<=as.Date(tar_borrow_time),1,0),
         apply_diff=difftime(as.Date(tar_borrow_time),as.Date(borrow_time),units='day')%>%as.numeric(),
         due_diff=difftime(as.Date(tar_borrow_time),as.Date(repay_time),units='day')%>%as.numeric(),
         disburse_diff=difftime(as.Date(tar_borrow_time),as.Date(loan_completion_time),units='day')%>%as.numeric(),
         
  )->sample_cardid_dwd_order_tib

###手机号多头
sample_cardid_dwd_order_tib%>%
  as_tibble()%>%
  group_by(tar_order_id)%>%
  summarise(
    ##同证件号平台多头
    cardid_nex_plat_apply_cnt=if_else(first(tar_user_card_id)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name!=tar_app_name]) ),
    cardid_nex_plat_d0_apply_cnt=if_else(first(tar_user_card_id)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name!=tar_app_name & apply_diff==0]) ),
    cardid_nex_plat_d3_apply_cnt=if_else(first(tar_user_card_id)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name!=tar_app_name & apply_diff<=3]) ),
    cardid_nex_plat_d7_apply_cnt=if_else(first(tar_user_card_id)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name!=tar_app_name & apply_diff<=7]) ),
    cardid_nex_plat_d14_apply_cnt=if_else(first(tar_user_card_id)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name!=tar_app_name & apply_diff<=14]) ),
    cardid_nex_plat_d30_apply_cnt=if_else(first(tar_user_card_id)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name!=tar_app_name & apply_diff<=30]) ),
    
    cardid_nex_plat_reject_cnt=if_else(first(tar_user_card_id)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name!=tar_app_name & status==3]) ),
    cardid_nex_plat_d0_reject_cnt=if_else(first(tar_user_card_id)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name!=tar_app_name & status==3 & apply_diff==0]) ),    
    cardid_nex_plat_d3_reject_cnt=if_else(first(tar_user_card_id)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name!=tar_app_name & status==3 & apply_diff<=3]) ),    
    cardid_nex_plat_d7_reject_cnt=if_else(first(tar_user_card_id)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name!=tar_app_name & status==3 & apply_diff<=7]) ),     
    cardid_nex_plat_d14_reject_cnt=if_else(first(tar_user_card_id)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name!=tar_app_name & status==3 & apply_diff<=14]) ),    
    cardid_nex_plat_d30_reject_cnt=if_else(first(tar_user_card_id)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name!=tar_app_name & status==3 & apply_diff<=30]) ),   
    
    cardid_nex_plat_disburse_cnt= if_else(first(tar_user_card_id)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8]) ),
    cardid_nex_plat_d0_disburse_cnt= if_else(first(tar_user_card_id)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff==0]) ),
    cardid_nex_plat_d3_disburse_cnt= if_else(first(tar_user_card_id)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff<=3]) ),
    cardid_nex_plat_d7_disburse_cnt= if_else(first(tar_user_card_id)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff<=7]) ),
    cardid_nex_plat_d14_disburse_cnt= if_else(first(tar_user_card_id)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff<=14]) ),
    cardid_nex_plat_d30_disburse_cnt= if_else(first(tar_user_card_id)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff<=30]) ),    
    
    cardid_nex_plat_overdue_cnt= if_else(first(tar_user_card_id)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & is_due==1 & is_repay==0]) ),
    cardid_nex_plat_d0_overdue_cnt= if_else(first(tar_user_card_id)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff==0]) ),
    cardid_nex_plat_d3_overdue_cnt= if_else(first(tar_user_card_id)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff<=3]) ),
    cardid_nex_plat_d7_overdue_cnt= if_else(first(tar_user_card_id)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff<=7]) ),
    cardid_nex_plat_d14_overdue_cnt= if_else(first(tar_user_card_id)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff<=14]) ),
    cardid_nex_plat_d30_overdue_cnt= if_else(first(tar_user_card_id)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff<=30]) ),    
    
    ##同证件号app内多头  
    cardid_nex_app_apply_cnt=if_else(first(tar_user_card_id)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name==tar_app_name]) ),
    cardid_nex_app_d0_apply_cnt=if_else(first(tar_user_card_id)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name==tar_app_name & apply_diff==0]) ),
    cardid_nex_app_d3_apply_cnt=if_else(first(tar_user_card_id)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name==tar_app_name & apply_diff<=3]) ),
    cardid_nex_app_d7_apply_cnt=if_else(first(tar_user_card_id)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name==tar_app_name & apply_diff<=7]) ),
    cardid_nex_app_d14_apply_cnt=if_else(first(tar_user_card_id)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name==tar_app_name & apply_diff<=14]) ),
    cardid_nex_app_d30_apply_cnt=if_else(first(tar_user_card_id)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name==tar_app_name & apply_diff<=30]) ),
    
    cardid_nex_app_reject_cnt=if_else(first(tar_user_card_id)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name==tar_app_name & status==3]) ),
    cardid_nex_app_d0_reject_cnt=if_else(first(tar_user_card_id)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name==tar_app_name & status==3 & apply_diff==0]) ),    
    cardid_nex_app_d3_reject_cnt=if_else(first(tar_user_card_id)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name==tar_app_name & status==3 & apply_diff<=3]) ),    
    cardid_nex_app_d7_reject_cnt=if_else(first(tar_user_card_id)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name==tar_app_name & status==3 & apply_diff<=7]) ),     
    cardid_nex_app_d14_reject_cnt=if_else(first(tar_user_card_id)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name==tar_app_name & status==3 & apply_diff<=14]) ),    
    cardid_nex_app_d30_reject_cnt=if_else(first(tar_user_card_id)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name==tar_app_name & status==3 & apply_diff<=30]) ),   
    
    cardid_nex_app_disburse_cnt= if_else(first(tar_user_card_id)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name==tar_app_name & status>=8]) ),
    cardid_nex_app_d0_disburse_cnt= if_else(first(tar_user_card_id)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff==0]) ),
    cardid_nex_app_d3_disburse_cnt= if_else(first(tar_user_card_id)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff<=3]) ),
    cardid_nex_app_d7_disburse_cnt= if_else(first(tar_user_card_id)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff<=7]) ),
    cardid_nex_app_d14_disburse_cnt= if_else(first(tar_user_card_id)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff<=14]) ),
    cardid_nex_app_d30_disburse_cnt= if_else(first(tar_user_card_id)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff<=30]) ),    
    
    cardid_nex_app_overdue_cnt= if_else(first(tar_user_card_id)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & is_due==1 & is_repay==0]) ),
    cardid_nex_app_d0_overdue_cnt= if_else(first(tar_user_card_id)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff==0]) ),
    cardid_nex_app_d3_overdue_cnt= if_else(first(tar_user_card_id)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff<=3]) ),
    cardid_nex_app_d7_overdue_cnt= if_else(first(tar_user_card_id)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff<=7]) ),
    cardid_nex_app_d14_overdue_cnt= if_else(first(tar_user_card_id)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff<=14]) ),
    cardid_nex_app_d30_overdue_cnt= if_else(first(tar_user_card_id)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff<=30]) ),    
    
    ##同证件号平台多头
    cardid_nex_plat_apply_order_cnt=if_else(first(tar_user_card_id)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name]) ),
    cardid_nex_plat_d0_apply_order_cnt=if_else(first(tar_user_card_id)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & apply_diff==0]) ),
    cardid_nex_plat_d3_apply_order_cnt=if_else(first(tar_user_card_id)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & apply_diff<=3]) ),
    cardid_nex_plat_d7_apply_order_cnt=if_else(first(tar_user_card_id)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & apply_diff<=7]) ),
    cardid_nex_plat_d14_apply_order_cnt=if_else(first(tar_user_card_id)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & apply_diff<=14]) ),
    cardid_nex_plat_d30_apply_order_cnt=if_else(first(tar_user_card_id)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & apply_diff<=30]) ),
    
    cardid_nex_plat_reject_order_cnt=if_else(first(tar_user_card_id)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & status==3]) ),
    cardid_nex_plat_d0_reject_order_cnt=if_else(first(tar_user_card_id)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & status==3 & apply_diff==0]) ),    
    cardid_nex_plat_d3_reject_order_cnt=if_else(first(tar_user_card_id)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & status==3 & apply_diff<=3]) ),    
    cardid_nex_plat_d7_reject_order_cnt=if_else(first(tar_user_card_id)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & status==3 & apply_diff<=7]) ),     
    cardid_nex_plat_d14_reject_order_cnt=if_else(first(tar_user_card_id)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & status==3 & apply_diff<=14]) ),    
    cardid_nex_plat_d30_reject_order_cnt=if_else(first(tar_user_card_id)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & status==3 & apply_diff<=30]) ),   
    
    cardid_nex_plat_disburse_order_cnt= if_else(first(tar_user_card_id)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8]) ),
    cardid_nex_plat_d0_disburse_order_cnt= if_else(first(tar_user_card_id)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff==0]) ),
    cardid_nex_plat_d3_disburse_order_cnt= if_else(first(tar_user_card_id)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff<=3]) ),
    cardid_nex_plat_d7_disburse_order_cnt= if_else(first(tar_user_card_id)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff<=7]) ),
    cardid_nex_plat_d14_disburse_order_cnt= if_else(first(tar_user_card_id)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff<=14]) ),
    cardid_nex_plat_d30_disburse_order_cnt= if_else(first(tar_user_card_id)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff<=30]) ),    
    
    cardid_nex_plat_overdue_order_cnt= if_else(first(tar_user_card_id)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & is_due==1 & is_repay==0]) ),
    cardid_nex_plat_d0_overdue_order_cnt= if_else(first(tar_user_card_id)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff==0]) ),
    cardid_nex_plat_d3_overdue_order_cnt= if_else(first(tar_user_card_id)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff<=3]) ),
    cardid_nex_plat_d7_overdue_order_cnt= if_else(first(tar_user_card_id)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff<=7]) ),
    cardid_nex_plat_d14_overdue_order_cnt= if_else(first(tar_user_card_id)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff<=14]) ),
    cardid_nex_plat_d30_overdue_order_cnt= if_else(first(tar_user_card_id)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff<=30]) ),    
    
    ##同证件号app内多头  
    cardid_nex_app_apply_order_cnt=if_else(first(tar_user_card_id)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name]) ),
    cardid_nex_app_d0_apply_order_cnt=if_else(first(tar_user_card_id)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & apply_diff==0]) ),
    cardid_nex_app_d3_apply_order_cnt=if_else(first(tar_user_card_id)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & apply_diff<=3]) ),
    cardid_nex_app_d7_apply_order_cnt=if_else(first(tar_user_card_id)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & apply_diff<=7]) ),
    cardid_nex_app_d14_apply_order_cnt=if_else(first(tar_user_card_id)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & apply_diff<=14]) ),
    cardid_nex_app_d30_apply_order_cnt=if_else(first(tar_user_card_id)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & apply_diff<=30]) ),
    
    cardid_nex_app_reject_order_cnt=if_else(first(tar_user_card_id)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & status==3]) ),
    cardid_nex_app_d0_reject_order_cnt=if_else(first(tar_user_card_id)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & status==3 & apply_diff==0]) ),    
    cardid_nex_app_d3_reject_order_cnt=if_else(first(tar_user_card_id)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & status==3 & apply_diff<=3]) ),    
    cardid_nex_app_d7_reject_order_cnt=if_else(first(tar_user_card_id)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & status==3 & apply_diff<=7]) ),     
    cardid_nex_app_d14_reject_order_cnt=if_else(first(tar_user_card_id)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & status==3 & apply_diff<=14]) ),    
    cardid_nex_app_d30_reject_order_cnt=if_else(first(tar_user_card_id)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & status==3 & apply_diff<=30]) ),   
    
    cardid_nex_app_disburse_order_cnt= if_else(first(tar_user_card_id)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & status>=8]) ),
    cardid_nex_app_d0_disburse_order_cnt= if_else(first(tar_user_card_id)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff==0]) ),
    cardid_nex_app_d3_disburse_order_cnt= if_else(first(tar_user_card_id)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff<=3]) ),
    cardid_nex_app_d7_disburse_order_cnt= if_else(first(tar_user_card_id)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff<=7]) ),
    cardid_nex_app_d14_disburse_order_cnt= if_else(first(tar_user_card_id)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff<=14]) ),
    cardid_nex_app_d30_disburse_order_cnt= if_else(first(tar_user_card_id)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff<=30]) ),    
    
    cardid_nex_app_overdue_order_cnt= if_else(first(tar_user_card_id)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & is_due==1 & is_repay==0]) ),
    cardid_nex_app_d0_overdue_order_cnt= if_else(first(tar_user_card_id)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff==0]) ),
    cardid_nex_app_d3_overdue_order_cnt= if_else(first(tar_user_card_id)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff<=3]) ),
    cardid_nex_app_d7_overdue_order_cnt= if_else(first(tar_user_card_id)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff<=7]) ),
    cardid_nex_app_d14_overdue_order_cnt= if_else(first(tar_user_card_id)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff<=14]) ),
    cardid_nex_app_d30_overdue_order_cnt= if_else(first(tar_user_card_id)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff<=30]) ),    
    
    same_cardid_diff_device_apply_cnt=if_else(first(tar_user_card_id)=='',-1, uniqueN(user_device_id[ tar_user_device_id!=user_device_id & (str_length(user_device_id)==36 & user_device_id!='00000000-0000-0000-0000-000000000000')])),
    same_cardid_diff_device_d0_apply_cnt=if_else(first(tar_user_card_id)=='',-1, uniqueN(user_device_id[apply_diff==0 &  tar_user_device_id!=user_device_id & (str_length(user_device_id)==36 & user_device_id!='00000000-0000-0000-0000-000000000000')])),
    same_cardid_diff_device_d3_apply_cnt=if_else(first(tar_user_card_id)=='',-1, uniqueN(user_device_id[apply_diff<=3 &  tar_user_device_id!=user_device_id & (str_length(user_device_id)==36 & user_device_id!='00000000-0000-0000-0000-000000000000')])),
    same_cardid_diff_device_d7_apply_cnt=if_else(first(tar_user_card_id)=='',-1, uniqueN(user_device_id[apply_diff<=7 &  tar_user_device_id!=user_device_id & (str_length(user_device_id)==36 & user_device_id!='00000000-0000-0000-0000-000000000000')])),
    same_cardid_diff_device_d14_apply_cnt=if_else(first(tar_user_card_id)=='',-1, uniqueN(user_device_id[apply_diff<=14 &  tar_user_device_id!=user_device_id & (str_length(user_device_id)==36 & user_device_id!='00000000-0000-0000-0000-000000000000')])),
    same_cardid_diff_device_d30_apply_cnt=if_else(first(tar_user_card_id)=='',-1, uniqueN(user_device_id[apply_diff<=30 &  tar_user_device_id!=user_device_id & (str_length(user_device_id)==36 & user_device_id!='00000000-0000-0000-0000-000000000000')])),
    
    same_cardid_diff_account_apply_cnt=if_else(first(tar_user_card_id)=='',-1, uniqueN(user_bank_account[ tar_user_bank_account!=user_bank_account & user_bank_account!=''])),
    same_cardid_diff_account_d0_apply_cnt=if_else(first(tar_user_card_id)=='',-1, uniqueN(user_bank_account[apply_diff==0 &  tar_user_bank_account!=user_bank_account & user_bank_account!=''])),    
    same_cardid_diff_account_d3_apply_cnt=if_else(first(tar_user_card_id)=='',-1, uniqueN(user_bank_account[apply_diff<=3 &  tar_user_bank_account!=user_bank_account & user_bank_account!=''])),    
    same_cardid_diff_account_d7_apply_cnt=if_else(first(tar_user_card_id)=='',-1, uniqueN(user_bank_account[apply_diff<=7 &  tar_user_bank_account!=user_bank_account & user_bank_account!=''])),    
    same_cardid_diff_account_d14_apply_cnt=if_else(first(tar_user_card_id)=='',-1, uniqueN(user_bank_account[apply_diff<=14 &  tar_user_bank_account!=user_bank_account & user_bank_account!=''])),    
    same_cardid_diff_account_d30_apply_cnt=if_else(first(tar_user_card_id)=='',-1, uniqueN(user_bank_account[apply_diff<=30 &  tar_user_bank_account!=user_bank_account & user_bank_account!=''])),    
    
    same_cardid_diff_ip_apply_cnt=if_else(first(tar_user_card_id)=='',-1, uniqueN(user_ip[ tar_user_ip!=user_ip & user_ip!=''])),
    same_cardid_diff_ip_d0_apply_cnt=if_else(first(tar_user_card_id)=='',-1, uniqueN(user_ip[apply_diff==0 &  tar_user_ip!=user_ip & user_ip!=''])),    
    same_cardid_diff_ip_d3_apply_cnt=if_else(first(tar_user_card_id)=='',-1, uniqueN(user_ip[apply_diff<=3 &  tar_user_ip!=user_ip & user_ip!=''])),    
    same_cardid_diff_ip_d7_apply_cnt=if_else(first(tar_user_card_id)=='',-1, uniqueN(user_ip[apply_diff<=7 &  tar_user_ip!=user_ip & user_ip!=''])),    
    same_cardid_diff_ip_d14_apply_cnt=if_else(first(tar_user_card_id)=='',-1, uniqueN(user_ip[apply_diff<=14 &  tar_user_ip!=user_ip & user_ip!=''])),    
    same_cardid_diff_ip_d30_apply_cnt=if_else(first(tar_user_card_id)=='',-1, uniqueN(user_ip[apply_diff<=30 &  tar_user_ip!=user_ip & user_ip!=''])),  
    
    same_cardid_diff_phone_apply_cnt=if_else(first(tar_user_card_id)=='',-1, uniqueN(user_phone[ tar_user_phone!=user_phone & user_phone!=''])),
    same_cardid_diff_phone_d0_apply_cnt=if_else(first(tar_user_card_id)=='',-1, uniqueN(user_phone[apply_diff==0 &  tar_user_phone!=user_phone & user_phone!=''])),    
    same_cardid_diff_phone_d3_apply_cnt=if_else(first(tar_user_card_id)=='',-1, uniqueN(user_phone[apply_diff<=3 &  tar_user_phone!=user_phone & user_phone!=''])),    
    same_cardid_diff_phone_d7_apply_cnt=if_else(first(tar_user_card_id)=='',-1, uniqueN(user_phone[apply_diff<=7 &  tar_user_phone!=user_phone & user_phone!=''])),    
    same_cardid_diff_phone_d14_apply_cnt=if_else(first(tar_user_card_id)=='',-1, uniqueN(user_phone[apply_diff<=14 &  tar_user_phone!=user_phone & user_phone!=''])),    
    same_cardid_diff_phone_d30_apply_cnt=if_else(first(tar_user_card_id)=='',-1, uniqueN(user_phone[apply_diff<=30 &  tar_user_phone!=user_phone & user_phone!=''])),

    cardid_first_disburse_gap=if_else(max(disburse_diff,na.rm = T)%>%is.infinite()|max(disburse_diff,na.rm = T)%>%is.na(),-1,max(disburse_diff,na.rm = T)),
    cardid_onloan_cnt=uniqueN(app_name[loan_completion_time<=tar_borrow_time & status>=8 & is_repay==0]),
    cardid_onloan_new_cnt=uniqueN(app_name[ loan_completion_time<=tar_borrow_time & status>=8 & is_repay==0 & user_type==0]),
    
  )%>%
  mutate(time=Sys.time())->card_multi_var

card_multi_var_frame<<-bind_rows(card_multi_var_frame,card_multi_var)

# ##姓名特征
# sample_order%>%
#   as_tibble()%>%
#   rename_with(~ paste0("tar_", .), everything())%>%
#   # rename(user_phone=tar_user_phone)%>%
#   select(tar_order_id,tar_user_id,tar_app_name,tar_user_name,tar_user_ip,tar_user_card_id,tar_user_phone,
#          tar_user_device_id,tar_user_bank_account,tar_borrow_time)%>%
#   filter(tar_user_name!='')%>%
#   left_join(dwd_order%>%select(user_id,order_id,user_name,user_phone,user_card_id,user_ip,user_device_id,user_bank_account,
#                                borrow_time,user_type,repay_yes_time,app_name,loan_completion_time,repay_time,borrow_time,status),
#             by=c('tar_user_name'='user_name') )->sample_name_order_join
# 
# 
# sample_name_order_join%>%
#   filter(borrow_time<=tar_borrow_time)%>% ##剔除申请时间在计算订单之后的记录
#   # filter(order_id!=tar_order_id)%>% ## 剔除订单号相同的记录  ## 回溯订单状态 添加时间切片
#   mutate(is_repay=if_else(repay_yes_time>tar_borrow_time | repay_yes_time=='',0,1),
#          is_due=if_else(as.Date(repay_time)<=as.Date(tar_borrow_time),1,0),
#          apply_diff=difftime(as.Date(tar_borrow_time),as.Date(borrow_time),units='day')%>%as.numeric(),
#          due_diff=difftime(as.Date(tar_borrow_time),as.Date(repay_time),units='day')%>%as.numeric(),
#          disburse_diff=difftime(as.Date(tar_borrow_time),as.Date(loan_completion_time),units='day')%>%as.numeric(),
#          
#   )->sample_name_dwd_order_tib


##设备id

##设备号特征
sample_order%>%
  rename_with(~ paste0("tar_", .), everything())%>%
  # rename(user_phone=tar_user_phone)%>%
  select(tar_order_id,tar_user_id,tar_app_name,tar_user_name,tar_user_ip,tar_user_card_id,tar_user_phone,
         tar_user_device_id,tar_user_bank_account,tar_borrow_time)%>%
  filter(tar_user_device_id!='' & str_length(tar_user_device_id)==36 & tar_user_device_id!='00000000-0000-0000-0000-000000000000')%>%
  left_join(dwd_order%>%select(user_id,order_id,user_name,user_phone,user_card_id,user_ip,user_device_id,user_bank_account,
                               borrow_time,user_type,repay_yes_time,app_name,loan_completion_time,repay_time,borrow_time,status),
            by=c('tar_user_device_id'='user_device_id') )->sample_device_order_join


sample_device_order_join%>%
  filter(borrow_time<=tar_borrow_time)%>% ##剔除申请时间在计算订单之后的记录
  # filter(order_id!=tar_order_id)%>% ## 剔除订单号相同的记录  ## 回溯订单状态 添加时间切片
  mutate(is_repay=if_else(repay_yes_time>tar_borrow_time | repay_yes_time=='',0,1),
         is_due=if_else(as.Date(repay_time)<=as.Date(tar_borrow_time),1,0),
         apply_diff=difftime(as.Date(tar_borrow_time),as.Date(borrow_time),units='day')%>%as.numeric(),
         due_diff=difftime(as.Date(tar_borrow_time),as.Date(repay_time),units='day')%>%as.numeric(),
         disburse_diff=difftime(as.Date(tar_borrow_time),as.Date(loan_completion_time),units='day')%>%as.numeric(),
         
  )->sample_device_dwd_order_tib



sample_device_dwd_order_tib%>%
  as_tibble()%>%
  group_by(tar_order_id)%>%
  summarise(
    deviceid_nex_plat_apply_cnt=if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(app_name[ user_id!=tar_user_id & app_name!=tar_app_name]) ),
    deviceid_nex_plat_d0_apply_cnt=if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(app_name[ user_id!=tar_user_id & app_name!=tar_app_name & apply_diff==0]) ),
    deviceid_nex_plat_d3_apply_cnt=if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(app_name[ user_id!=tar_user_id & app_name!=tar_app_name & apply_diff<=3]) ),
    deviceid_nex_plat_d7_apply_cnt=if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(app_name[ user_id!=tar_user_id & app_name!=tar_app_name & apply_diff<=7]) ),
    deviceid_nex_plat_d14_apply_cnt=if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(app_name[ user_id!=tar_user_id & app_name!=tar_app_name & apply_diff<=14]) ),
    deviceid_nex_plat_d30_apply_cnt=if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(app_name[ user_id!=tar_user_id & app_name!=tar_app_name & apply_diff<=30]) ),
    
    deviceid_nex_plat_reject_cnt=if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(app_name[ user_id!=tar_user_id & app_name!=tar_app_name & status==3]) ),
    deviceid_nex_plat_d0_reject_cnt=if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(app_name[ user_id!=tar_user_id & app_name!=tar_app_name & status==3 & apply_diff==0]) ),    
    deviceid_nex_plat_d3_reject_cnt=if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(app_name[ user_id!=tar_user_id & app_name!=tar_app_name & status==3 & apply_diff<=3]) ),    
    deviceid_nex_plat_d7_reject_cnt=if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(app_name[ user_id!=tar_user_id & app_name!=tar_app_name & status==3 & apply_diff<=7]) ),     
    deviceid_nex_plat_d14_reject_cnt=if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(app_name[ user_id!=tar_user_id & app_name!=tar_app_name & status==3 & apply_diff<=14]) ),    
    deviceid_nex_plat_d30_reject_cnt=if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(app_name[ user_id!=tar_user_id & app_name!=tar_app_name & status==3 & apply_diff<=30]) ),   
    
    deviceid_nex_plat_disburse_cnt= if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(app_name[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8]) ),
    deviceid_nex_plat_d0_disburse_cnt= if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(app_name[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff==0]) ),
    deviceid_nex_plat_d3_disburse_cnt= if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(app_name[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff<=3]) ),
    deviceid_nex_plat_d7_disburse_cnt= if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(app_name[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff<=7]) ),
    deviceid_nex_plat_d14_disburse_cnt= if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(app_name[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff<=14]) ),
    deviceid_nex_plat_d30_disburse_cnt= if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(app_name[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff<=30]) ),    
    
    deviceid_nex_plat_overdue_cnt= if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(app_name[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & is_due==1 & is_repay==0]) ),
    deviceid_nex_plat_d0_overdue_cnt= if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(app_name[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff==0]) ),
    deviceid_nex_plat_d3_overdue_cnt= if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(app_name[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff<=3]) ),
    deviceid_nex_plat_d7_overdue_cnt= if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(app_name[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff<=7]) ),
    deviceid_nex_plat_d14_overdue_cnt= if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(app_name[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff<=14]) ),
    deviceid_nex_plat_d30_overdue_cnt= if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(app_name[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff<=30]) ),    
    
    ##同设备号app内多头  
    deviceid_nex_app_apply_cnt=if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(app_name[ user_id!=tar_user_id & app_name==tar_app_name]) ),
    deviceid_nex_app_d0_apply_cnt=if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(app_name[ user_id!=tar_user_id & app_name==tar_app_name & apply_diff==0]) ),
    deviceid_nex_app_d3_apply_cnt=if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(app_name[ user_id!=tar_user_id & app_name==tar_app_name & apply_diff<=3]) ),
    deviceid_nex_app_d7_apply_cnt=if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(app_name[ user_id!=tar_user_id & app_name==tar_app_name & apply_diff<=7]) ),
    deviceid_nex_app_d14_apply_cnt=if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(app_name[ user_id!=tar_user_id & app_name==tar_app_name & apply_diff<=14]) ),
    deviceid_nex_app_d30_apply_cnt=if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(app_name[ user_id!=tar_user_id & app_name==tar_app_name & apply_diff<=30]) ),
    
    deviceid_nex_app_reject_cnt=if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(app_name[ user_id!=tar_user_id & app_name==tar_app_name & status==3]) ),
    deviceid_nex_app_d0_reject_cnt=if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(app_name[ user_id!=tar_user_id & app_name==tar_app_name & status==3 & apply_diff==0]) ),    
    deviceid_nex_app_d3_reject_cnt=if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(app_name[ user_id!=tar_user_id & app_name==tar_app_name & status==3 & apply_diff<=3]) ),    
    deviceid_nex_app_d7_reject_cnt=if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(app_name[ user_id!=tar_user_id & app_name==tar_app_name & status==3 & apply_diff<=7]) ),     
    deviceid_nex_app_d14_reject_cnt=if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(app_name[ user_id!=tar_user_id & app_name==tar_app_name & status==3 & apply_diff<=14]) ),    
    deviceid_nex_app_d30_reject_cnt=if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(app_name[ user_id!=tar_user_id & app_name==tar_app_name & status==3 & apply_diff<=30]) ),   
    
    deviceid_nex_app_disburse_cnt= if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(app_name[ user_id!=tar_user_id & app_name==tar_app_name & status>=8]) ),
    deviceid_nex_app_d0_disburse_cnt= if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(app_name[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff==0]) ),
    deviceid_nex_app_d3_disburse_cnt= if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(app_name[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff<=3]) ),
    deviceid_nex_app_d7_disburse_cnt= if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(app_name[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff<=7]) ),
    deviceid_nex_app_d14_disburse_cnt= if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(app_name[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff<=14]) ),
    deviceid_nex_app_d30_disburse_cnt= if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(app_name[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff<=30]) ),    
    
    deviceid_nex_app_overdue_cnt= if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(app_name[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & is_due==1 & is_repay==0]) ),
    deviceid_nex_app_d0_overdue_cnt= if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(app_name[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff==0]) ),
    deviceid_nex_app_d3_overdue_cnt= if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(app_name[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff<=3]) ),
    deviceid_nex_app_d7_overdue_cnt= if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(app_name[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff<=7]) ),
    deviceid_nex_app_d14_overdue_cnt= if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(app_name[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff<=14]) ),
    deviceid_nex_app_d30_overdue_cnt= if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(app_name[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff<=30]) ),    
    
    ##同设备号平台多头
    deviceid_nex_plat_apply_order_cnt=if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name]) ),
    deviceid_nex_plat_d0_apply_order_cnt=if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & apply_diff==0]) ),
    deviceid_nex_plat_d3_apply_order_cnt=if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & apply_diff<=3]) ),
    deviceid_nex_plat_d7_apply_order_cnt=if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & apply_diff<=7]) ),
    deviceid_nex_plat_d14_apply_order_cnt=if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & apply_diff<=14]) ),
    deviceid_nex_plat_d30_apply_order_cnt=if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & apply_diff<=30]) ),
    
    deviceid_nex_plat_reject_order_cnt=if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & status==3]) ),
    deviceid_nex_plat_d0_reject_order_cnt=if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & status==3 & apply_diff==0]) ),    
    deviceid_nex_plat_d3_reject_order_cnt=if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & status==3 & apply_diff<=3]) ),    
    deviceid_nex_plat_d7_reject_order_cnt=if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & status==3 & apply_diff<=7]) ),     
    deviceid_nex_plat_d14_reject_order_cnt=if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & status==3 & apply_diff<=14]) ),    
    deviceid_nex_plat_d30_reject_order_cnt=if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & status==3 & apply_diff<=30]) ),   
    
    deviceid_nex_plat_disburse_order_cnt= if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8]) ),
    deviceid_nex_plat_d0_disburse_order_cnt= if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff==0]) ),
    deviceid_nex_plat_d3_disburse_order_cnt= if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff<=3]) ),
    deviceid_nex_plat_d7_disburse_order_cnt= if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff<=7]) ),
    deviceid_nex_plat_d14_disburse_order_cnt= if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff<=14]) ),
    deviceid_nex_plat_d30_disburse_order_cnt= if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff<=30]) ),    
    
    deviceid_nex_plat_overdue_order_cnt= if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & is_due==1 & is_repay==0]) ),
    deviceid_nex_plat_d0_overdue_order_cnt= if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff==0]) ),
    deviceid_nex_plat_d3_overdue_order_cnt= if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff<=3]) ),
    deviceid_nex_plat_d7_overdue_order_cnt= if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff<=7]) ),
    deviceid_nex_plat_d14_overdue_order_cnt= if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff<=14]) ),
    deviceid_nex_plat_d30_overdue_order_cnt= if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff<=30]) ),    
    
    ##同设备号app内多头  
    deviceid_nex_app_apply_order_cnt=if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name]) ),
    deviceid_nex_app_d0_apply_order_cnt=if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & apply_diff==0]) ),
    deviceid_nex_app_d3_apply_order_cnt=if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & apply_diff<=3]) ),
    deviceid_nex_app_d7_apply_order_cnt=if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & apply_diff<=7]) ),
    deviceid_nex_app_d14_apply_order_cnt=if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & apply_diff<=14]) ),
    deviceid_nex_app_d30_apply_order_cnt=if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & apply_diff<=30]) ),
    
    deviceid_nex_app_reject_order_cnt=if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & status==3]) ),
    deviceid_nex_app_d0_reject_order_cnt=if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & status==3 & apply_diff==0]) ),    
    deviceid_nex_app_d3_reject_order_cnt=if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & status==3 & apply_diff<=3]) ),    
    deviceid_nex_app_d7_reject_order_cnt=if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & status==3 & apply_diff<=7]) ),     
    deviceid_nex_app_d14_reject_order_cnt=if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & status==3 & apply_diff<=14]) ),    
    deviceid_nex_app_d30_reject_order_cnt=if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & status==3 & apply_diff<=30]) ),   
    
    deviceid_nex_app_disburse_order_cnt= if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & status>=8]) ),
    deviceid_nex_app_d0_disburse_order_cnt= if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff==0]) ),
    deviceid_nex_app_d3_disburse_order_cnt= if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff<=3]) ),
    deviceid_nex_app_d7_disburse_order_cnt= if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff<=7]) ),
    deviceid_nex_app_d14_disburse_order_cnt= if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff<=14]) ),
    deviceid_nex_app_d30_disburse_order_cnt= if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff<=30]) ),    
    
    deviceid_nex_app_overdue_order_cnt= if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & is_due==1 & is_repay==0]) ),
    deviceid_nex_app_d0_overdue_order_cnt= if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff==0]) ),
    deviceid_nex_app_d3_overdue_order_cnt= if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff<=3]) ),
    deviceid_nex_app_d7_overdue_order_cnt= if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff<=7]) ),
    deviceid_nex_app_d14_overdue_order_cnt= if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff<=14]) ),
    deviceid_nex_app_d30_overdue_order_cnt= if_else(first(tar_user_device_id)=='' | first(tar_user_device_id)=='00000000-0000-0000-0000-000000000000' |  first(str_length(tar_user_device_id)<36),-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff<=30]) ),    
    
    same_device_diff_account_apply_cnt=if_else(first(tar_user_device_id)=='',-1, uniqueN(user_bank_account[ tar_user_bank_account!=user_bank_account & user_bank_account!=''])),
    same_device_diff_account_d0_apply_cnt=if_else(first(tar_user_device_id)=='',-1, uniqueN(user_bank_account[apply_diff==0 &  tar_user_bank_account!=user_bank_account & user_bank_account!=''])),    
    same_device_diff_account_d3_apply_cnt=if_else(first(tar_user_device_id)=='',-1, uniqueN(user_bank_account[apply_diff<=3 &  tar_user_bank_account!=user_bank_account & user_bank_account!=''])),    
    same_device_diff_account_d7_apply_cnt=if_else(first(tar_user_device_id)=='',-1, uniqueN(user_bank_account[apply_diff<=7 &  tar_user_bank_account!=user_bank_account & user_bank_account!=''])),    
    same_device_diff_account_d14_apply_cnt=if_else(first(tar_user_device_id)=='',-1, uniqueN(user_bank_account[apply_diff<=14 &  tar_user_bank_account!=user_bank_account & user_bank_account!=''])),    
    same_device_diff_account_d30_apply_cnt=if_else(first(tar_user_device_id)=='',-1, uniqueN(user_bank_account[apply_diff<=30 &  tar_user_bank_account!=user_bank_account & user_bank_account!=''])),    
    
    same_device_diff_ip_apply_cnt=if_else(first(tar_user_device_id)=='',-1, uniqueN(user_ip[ tar_user_ip!=user_ip & user_ip!=''])),
    same_device_diff_ip_d0_apply_cnt=if_else(first(tar_user_device_id)=='',-1, uniqueN(user_ip[apply_diff==0 &  tar_user_ip!=user_ip & user_ip!=''])),    
    same_device_diff_ip_d3_apply_cnt=if_else(first(tar_user_device_id)=='',-1, uniqueN(user_ip[apply_diff<=3 &  tar_user_ip!=user_ip & user_ip!=''])),    
    same_device_diff_ip_d7_apply_cnt=if_else(first(tar_user_device_id)=='',-1, uniqueN(user_ip[apply_diff<=7 &  tar_user_ip!=user_ip & user_ip!=''])),    
    same_device_diff_ip_d14_apply_cnt=if_else(first(tar_user_device_id)=='',-1, uniqueN(user_ip[apply_diff<=14 &  tar_user_ip!=user_ip & user_ip!=''])),    
    same_device_diff_ip_d30_apply_cnt=if_else(first(tar_user_device_id)=='',-1, uniqueN(user_ip[apply_diff<=30 &  tar_user_ip!=user_ip & user_ip!=''])),    
    
    same_device_diff_cardid_apply_cnt=if_else(first(tar_user_device_id)=='',-1, uniqueN(user_card_id[ tar_user_card_id!=user_card_id & user_card_id!=''])),
    same_device_diff_cardid_d0_apply_cnt=if_else(first(tar_user_device_id)=='',-1, uniqueN(user_card_id[apply_diff==0 &  tar_user_card_id!=user_card_id & user_card_id!=''])),    
    same_device_diff_cardid_d3_apply_cnt=if_else(first(tar_user_device_id)=='',-1, uniqueN(user_card_id[apply_diff<=3 &  tar_user_card_id!=user_card_id & user_card_id!=''])),    
    same_device_diff_cardid_d7_apply_cnt=if_else(first(tar_user_device_id)=='',-1, uniqueN(user_card_id[apply_diff<=7 &  tar_user_card_id!=user_card_id & user_card_id!=''])),    
    same_device_diff_cardid_d14_apply_cnt=if_else(first(tar_user_device_id)=='',-1, uniqueN(user_card_id[apply_diff<=14 &  tar_user_card_id!=user_card_id & user_card_id!=''])),    
    same_device_diff_cardid_d30_apply_cnt=if_else(first(tar_user_device_id)=='',-1, uniqueN(user_card_id[apply_diff<=30 &  tar_user_card_id!=user_card_id & user_card_id!=''])),    
    
    same_device_diff_phone_apply_cnt=if_else(first(tar_user_device_id)=='',-1, uniqueN(user_phone[ tar_user_phone!=user_phone & user_phone!=''])),
    same_device_diff_phone_d0_apply_cnt=if_else(first(tar_user_device_id)=='',-1, uniqueN(user_phone[apply_diff==0 &  tar_user_phone!=user_phone & user_phone!=''])),    
    same_device_diff_phone_d3_apply_cnt=if_else(first(tar_user_device_id)=='',-1, uniqueN(user_phone[apply_diff<=3 &  tar_user_phone!=user_phone & user_phone!=''])),    
    same_device_diff_phone_d7_apply_cnt=if_else(first(tar_user_device_id)=='',-1, uniqueN(user_phone[apply_diff<=7 &  tar_user_phone!=user_phone & user_phone!=''])),    
    same_device_diff_phone_d14_apply_cnt=if_else(first(tar_user_device_id)=='',-1, uniqueN(user_phone[apply_diff<=14 &  tar_user_phone!=user_phone & user_phone!=''])),    
    same_device_diff_phone_d30_apply_cnt=if_else(first(tar_user_device_id)=='',-1, uniqueN(user_phone[apply_diff<=30 &  tar_user_phone!=user_phone & user_phone!=''])), 
  
    )%>%
  mutate(time=Sys.time())->device_multi_var

device_multi_var_frame<<-bind_rows(device_multi_var_frame,device_multi_var)
###ip

##ip特征
sample_order%>%
  rename_with(~ paste0("tar_", .), everything())%>%
  # rename(user_phone=tar_user_phone)%>%
  select(tar_order_id,tar_user_id,tar_app_name,tar_user_name,tar_user_ip,tar_user_card_id,tar_user_phone,
         tar_user_device_id,tar_user_bank_account,tar_borrow_time)%>%
  filter(tar_user_ip!='')%>%
  left_join(dwd_order%>%select(user_id,order_id,user_name,user_phone,user_card_id,user_ip,user_device_id,user_bank_account,
                               borrow_time,user_type,repay_yes_time,app_name,loan_completion_time,repay_time,borrow_time,status),
            by=c('tar_user_ip'='user_ip') )->sample_ip_order_join


sample_ip_order_join%>%
  filter(borrow_time<=tar_borrow_time)%>% ##剔除申请时间在计算订单之后的记录
  # filter(order_id!=tar_order_id)%>% ## 剔除订单号相同的记录  ## 回溯订单状态 添加时间切片
  mutate(is_repay=if_else(repay_yes_time>tar_borrow_time | repay_yes_time=='',0,1),
         is_due=if_else(as.Date(repay_time)<=as.Date(tar_borrow_time),1,0),
         apply_diff=difftime(as.Date(tar_borrow_time),as.Date(borrow_time),units='day')%>%as.numeric(),
         due_diff=difftime(as.Date(tar_borrow_time),as.Date(repay_time),units='day')%>%as.numeric(),
         disburse_diff=difftime(as.Date(tar_borrow_time),as.Date(loan_completion_time),units='day')%>%as.numeric(),
         
  )->sample_ip_dwd_order_tib

sample_ip_dwd_order_tib%>%
  as_tibble()%>%
  group_by(tar_order_id)%>%
  summarise(
    ##平台多头
    ip_nex_plat_apply_cnt=if_else(first(tar_user_ip)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name!=tar_app_name]) ),
    ip_nex_plat_d0_apply_cnt=if_else(first(tar_user_ip)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name!=tar_app_name & apply_diff==0]) ),
    ip_nex_plat_d3_apply_cnt=if_else(first(tar_user_ip)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name!=tar_app_name & apply_diff<=3]) ),
    ip_nex_plat_d7_apply_cnt=if_else(first(tar_user_ip)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name!=tar_app_name & apply_diff<=7]) ),
    ip_nex_plat_d14_apply_cnt=if_else(first(tar_user_ip)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name!=tar_app_name & apply_diff<=14]) ),
    ip_nex_plat_d30_apply_cnt=if_else(first(tar_user_ip)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name!=tar_app_name & apply_diff<=30]) ),
    
    ip_nex_plat_reject_cnt=if_else(first(tar_user_ip)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name!=tar_app_name & status==3]) ),
    ip_nex_plat_d0_reject_cnt=if_else(first(tar_user_ip)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name!=tar_app_name & status==3 & apply_diff==0]) ),    
    ip_nex_plat_d3_reject_cnt=if_else(first(tar_user_ip)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name!=tar_app_name & status==3 & apply_diff<=3]) ),    
    ip_nex_plat_d7_reject_cnt=if_else(first(tar_user_ip)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name!=tar_app_name & status==3 & apply_diff<=7]) ),     
    ip_nex_plat_d14_reject_cnt=if_else(first(tar_user_ip)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name!=tar_app_name & status==3 & apply_diff<=14]) ),    
    ip_nex_plat_d30_reject_cnt=if_else(first(tar_user_ip)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name!=tar_app_name & status==3 & apply_diff<=30]) ),   
    
    ip_nex_plat_disburse_cnt= if_else(first(tar_user_ip)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8]) ),
    ip_nex_plat_d0_disburse_cnt= if_else(first(tar_user_ip)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff==0]) ),
    ip_nex_plat_d3_disburse_cnt= if_else(first(tar_user_ip)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff<=3]) ),
    ip_nex_plat_d7_disburse_cnt= if_else(first(tar_user_ip)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff<=7]) ),
    ip_nex_plat_d14_disburse_cnt= if_else(first(tar_user_ip)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff<=14]) ),
    ip_nex_plat_d30_disburse_cnt= if_else(first(tar_user_ip)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff<=30]) ),    
    
    ip_nex_plat_overdue_cnt= if_else(first(tar_user_ip)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & is_due==1 & is_repay==0]) ),
    ip_nex_plat_d0_overdue_cnt= if_else(first(tar_user_ip)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff==0]) ),
    ip_nex_plat_d3_overdue_cnt= if_else(first(tar_user_ip)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff<=3]) ),
    ip_nex_plat_d7_overdue_cnt= if_else(first(tar_user_ip)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff<=7]) ),
    ip_nex_plat_d14_overdue_cnt= if_else(first(tar_user_ip)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff<=14]) ),
    ip_nex_plat_d30_overdue_cnt= if_else(first(tar_user_ip)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff<=30]) ),    
    
    ##app多头
    ip_nex_app_apply_cnt=if_else(first(tar_user_ip)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name==tar_app_name]) ),
    ip_nex_app_d0_apply_cnt=if_else(first(tar_user_ip)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name==tar_app_name & apply_diff==0]) ),
    ip_nex_app_d3_apply_cnt=if_else(first(tar_user_ip)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name==tar_app_name & apply_diff<=3]) ),
    ip_nex_app_d7_apply_cnt=if_else(first(tar_user_ip)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name==tar_app_name & apply_diff<=7]) ),
    ip_nex_app_d14_apply_cnt=if_else(first(tar_user_ip)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name==tar_app_name & apply_diff<=14]) ),
    ip_nex_app_d30_apply_cnt=if_else(first(tar_user_ip)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name==tar_app_name & apply_diff<=30]) ),
    
    ip_nex_app_reject_cnt=if_else(first(tar_user_ip)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name==tar_app_name & status==3]) ),
    ip_nex_app_d0_reject_cnt=if_else(first(tar_user_ip)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name==tar_app_name & status==3 & apply_diff==0]) ),    
    ip_nex_app_d3_reject_cnt=if_else(first(tar_user_ip)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name==tar_app_name & status==3 & apply_diff<=3]) ),    
    ip_nex_app_d7_reject_cnt=if_else(first(tar_user_ip)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name==tar_app_name & status==3 & apply_diff<=7]) ),     
    ip_nex_app_d14_reject_cnt=if_else(first(tar_user_ip)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name==tar_app_name & status==3 & apply_diff<=14]) ),    
    ip_nex_app_d30_reject_cnt=if_else(first(tar_user_ip)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name==tar_app_name & status==3 & apply_diff<=30]) ),   
    
    ip_nex_app_disburse_cnt= if_else(first(tar_user_ip)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name==tar_app_name & status>=8]) ),
    ip_nex_app_d0_disburse_cnt= if_else(first(tar_user_ip)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff==0]) ),
    ip_nex_app_d3_disburse_cnt= if_else(first(tar_user_ip)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff<=3]) ),
    ip_nex_app_d7_disburse_cnt= if_else(first(tar_user_ip)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff<=7]) ),
    ip_nex_app_d14_disburse_cnt= if_else(first(tar_user_ip)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff<=14]) ),
    ip_nex_app_d30_disburse_cnt= if_else(first(tar_user_ip)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff<=30]) ),    
    
    ip_nex_app_overdue_cnt= if_else(first(tar_user_ip)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & is_due==1 & is_repay==0]) ),
    ip_nex_app_d0_overdue_cnt= if_else(first(tar_user_ip)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff==0]) ),
    ip_nex_app_d3_overdue_cnt= if_else(first(tar_user_ip)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff<=3]) ),
    ip_nex_app_d7_overdue_cnt= if_else(first(tar_user_ip)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff<=7]) ),
    ip_nex_app_d14_overdue_cnt= if_else(first(tar_user_ip)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff<=14]) ),
    ip_nex_app_d30_overdue_cnt= if_else(first(tar_user_ip)=='',-1,uniqueN(app_name[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff<=30]) ),    
    
    ##平台订单多头
    ip_nex_plat_apply_order_cnt=if_else(first(tar_user_ip)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name]) ),
    ip_nex_plat_d0_apply_order_cnt=if_else(first(tar_user_ip)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & apply_diff==0]) ),
    ip_nex_plat_d3_apply_order_cnt=if_else(first(tar_user_ip)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & apply_diff<=3]) ),
    ip_nex_plat_d7_apply_order_cnt=if_else(first(tar_user_ip)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & apply_diff<=7]) ),
    ip_nex_plat_d14_apply_order_cnt=if_else(first(tar_user_ip)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & apply_diff<=14]) ),
    ip_nex_plat_d30_apply_order_cnt=if_else(first(tar_user_ip)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & apply_diff<=30]) ),
    
    ip_nex_plat_reject_order_cnt=if_else(first(tar_user_ip)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & status==3]) ),
    ip_nex_plat_d0_reject_order_cnt=if_else(first(tar_user_ip)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & status==3 & apply_diff==0]) ),    
    ip_nex_plat_d3_reject_order_cnt=if_else(first(tar_user_ip)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & status==3 & apply_diff<=3]) ),    
    ip_nex_plat_d7_reject_order_cnt=if_else(first(tar_user_ip)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & status==3 & apply_diff<=7]) ),     
    ip_nex_plat_d14_reject_order_cnt=if_else(first(tar_user_ip)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & status==3 & apply_diff<=14]) ),    
    ip_nex_plat_d30_reject_order_cnt=if_else(first(tar_user_ip)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & status==3 & apply_diff<=30]) ),   
    
    ip_nex_plat_disburse_order_cnt= if_else(first(tar_user_ip)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8]) ),
    ip_nex_plat_d0_disburse_order_cnt= if_else(first(tar_user_ip)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff==0]) ),
    ip_nex_plat_d3_disburse_order_cnt= if_else(first(tar_user_ip)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff<=3]) ),
    ip_nex_plat_d7_disburse_order_cnt= if_else(first(tar_user_ip)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff<=7]) ),
    ip_nex_plat_d14_disburse_order_cnt= if_else(first(tar_user_ip)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff<=14]) ),
    ip_nex_plat_d30_disburse_order_cnt= if_else(first(tar_user_ip)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff<=30]) ),    
    
    ip_nex_plat_overdue_order_cnt= if_else(first(tar_user_ip)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & is_due==1 & is_repay==0]) ),
    ip_nex_plat_d0_overdue_order_cnt= if_else(first(tar_user_ip)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff==0]) ),
    ip_nex_plat_d3_overdue_order_cnt= if_else(first(tar_user_ip)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff<=3]) ),
    ip_nex_plat_d7_overdue_order_cnt= if_else(first(tar_user_ip)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff<=7]) ),
    ip_nex_plat_d14_overdue_order_cnt= if_else(first(tar_user_ip)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff<=14]) ),
    ip_nex_plat_d30_overdue_order_cnt= if_else(first(tar_user_ip)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff<=30]) ),    
    
    ##app 订单多头  
    ip_nex_app_apply_order_cnt=if_else(first(tar_user_ip)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name]) ),
    ip_nex_app_d0_apply_order_cnt=if_else(first(tar_user_ip)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & apply_diff==0]) ),
    ip_nex_app_d3_apply_order_cnt=if_else(first(tar_user_ip)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & apply_diff<=3]) ),
    ip_nex_app_d7_apply_order_cnt=if_else(first(tar_user_ip)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & apply_diff<=7]) ),
    ip_nex_app_d14_apply_order_cnt=if_else(first(tar_user_ip)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & apply_diff<=14]) ),
    ip_nex_app_d30_apply_order_cnt=if_else(first(tar_user_ip)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & apply_diff<=30]) ),
    
    ip_nex_app_reject_order_cnt=if_else(first(tar_user_ip)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & status==3]) ),
    ip_nex_app_d0_reject_order_cnt=if_else(first(tar_user_ip)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & status==3 & apply_diff==0]) ),    
    ip_nex_app_d3_reject_order_cnt=if_else(first(tar_user_ip)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & status==3 & apply_diff<=3]) ),    
    ip_nex_app_d7_reject_order_cnt=if_else(first(tar_user_ip)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & status==3 & apply_diff<=7]) ),     
    ip_nex_app_d14_reject_order_cnt=if_else(first(tar_user_ip)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & status==3 & apply_diff<=14]) ),    
    ip_nex_app_d30_reject_order_cnt=if_else(first(tar_user_ip)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & status==3 & apply_diff<=30]) ),   
    
    ip_nex_app_disburse_order_cnt= if_else(first(tar_user_ip)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & status>=8]) ),
    ip_nex_app_d0_disburse_order_cnt= if_else(first(tar_user_ip)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff==0]) ),
    ip_nex_app_d3_disburse_order_cnt= if_else(first(tar_user_ip)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff<=3]) ),
    ip_nex_app_d7_disburse_order_cnt= if_else(first(tar_user_ip)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff<=7]) ),
    ip_nex_app_d14_disburse_order_cnt= if_else(first(tar_user_ip)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff<=14]) ),
    ip_nex_app_d30_disburse_order_cnt= if_else(first(tar_user_ip)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff<=30]) ),    
    
    ip_nex_app_overdue_order_cnt= if_else(first(tar_user_ip)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & is_due==1 & is_repay==0]) ),
    ip_nex_app_d0_overdue_order_cnt= if_else(first(tar_user_ip)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff==0]) ),
    ip_nex_app_d3_overdue_order_cnt= if_else(first(tar_user_ip)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff<=3]) ),
    ip_nex_app_d7_overdue_order_cnt= if_else(first(tar_user_ip)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff<=7]) ),
    ip_nex_app_d14_overdue_order_cnt= if_else(first(tar_user_ip)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff<=14]) ),
    ip_nex_app_d30_overdue_order_cnt= if_else(first(tar_user_ip)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff<=30]) ),    
    
    same_ip_diff_phone_apply_cnt=if_else(first(tar_user_ip)=='',-1, uniqueN(user_phone[  tar_user_phone!=user_phone & user_phone!=''])),
    same_ip_diff_phone_d0_apply_cnt=if_else(first(tar_user_ip)=='',-1, uniqueN(user_phone[ apply_diff==0 &  tar_user_phone!=user_phone & user_phone!=''])),    
    same_ip_diff_phone_d3_apply_cnt=if_else(first(tar_user_ip)=='',-1, uniqueN(user_phone[ apply_diff<=3 &  tar_user_phone!=user_phone & user_phone!=''])),    
    same_ip_diff_phone_d7_apply_cnt=if_else(first(tar_user_ip)=='',-1, uniqueN(user_phone[ apply_diff<=7 &  tar_user_phone!=user_phone & user_phone!=''])),    
    same_ip_diff_phone_d14_apply_cnt=if_else(first(tar_user_ip)=='',-1, uniqueN(user_phone[ apply_diff<=14 &  tar_user_phone!=user_phone & user_phone!=''])),    
    same_ip_diff_phone_d30_apply_cnt=if_else(first(tar_user_ip)=='',-1, uniqueN(user_phone[ apply_diff<=30 &  tar_user_phone!=user_phone & user_phone!=''])),    
    
    same_ip_diff_cardid_apply_cnt=if_else(first(tar_user_ip)=='',-1, uniqueN(user_card_id[  tar_user_card_id!=user_card_id & user_card_id!=''])),
    same_ip_diff_cardid_d0_apply_cnt=if_else(first(tar_user_ip)=='',-1, uniqueN(user_card_id[ apply_diff==0 &  tar_user_card_id!=user_card_id & user_card_id!=''])),    
    same_ip_diff_cardid_d3_apply_cnt=if_else(first(tar_user_ip)=='',-1, uniqueN(user_card_id[ apply_diff<=3 &  tar_user_card_id!=user_card_id & user_card_id!=''])),    
    same_ip_diff_cardid_d7_apply_cnt=if_else(first(tar_user_ip)=='',-1, uniqueN(user_card_id[ apply_diff<=7 &  tar_user_card_id!=user_card_id & user_card_id!=''])),    
    same_ip_diff_cardid_d14_apply_cnt=if_else(first(tar_user_ip)=='',-1, uniqueN(user_card_id[ apply_diff<=14 &  tar_user_card_id!=user_card_id & user_card_id!=''])),    
    same_ip_diff_cardid_d30_apply_cnt=if_else(first(tar_user_ip)=='',-1, uniqueN(user_card_id[ apply_diff<=30 &  tar_user_card_id!=user_card_id & user_card_id!=''])),   
    
    same_ip_diff_device_apply_cnt=if_else(first(tar_user_ip)=='',-1, uniqueN(user_device_id[  tar_user_device_id!=user_device_id & user_device_id!=''])),
    same_ip_diff_device_d0_apply_cnt=if_else(first(tar_user_ip)=='',-1, uniqueN(user_device_id[ apply_diff==0 &  tar_user_device_id!=user_device_id & user_device_id!=''])),    
    same_ip_diff_device_d3_apply_cnt=if_else(first(tar_user_ip)=='',-1, uniqueN(user_device_id[ apply_diff<=3 &  tar_user_device_id!=user_device_id & user_device_id!=''])),    
    same_ip_diff_device_d7_apply_cnt=if_else(first(tar_user_ip)=='',-1, uniqueN(user_device_id[ apply_diff<=7 &  tar_user_device_id!=user_device_id & user_device_id!=''])),    
    same_ip_diff_device_d14_apply_cnt=if_else(first(tar_user_ip)=='',-1, uniqueN(user_device_id[ apply_diff<=14 &  tar_user_device_id!=user_device_id & user_device_id!=''])),    
    same_ip_diff_device_d30_apply_cnt=if_else(first(tar_user_ip)=='',-1, uniqueN(user_device_id[ apply_diff<=30 &  tar_user_device_id!=user_device_id & user_device_id!=''])),  
    
    same_ip_diff_account_apply_cnt=if_else(first(tar_user_ip)=='',-1, uniqueN(user_bank_account[  tar_user_bank_account!=user_bank_account & user_bank_account!=''])),
    same_ip_diff_account_d0_apply_cnt=if_else(first(tar_user_ip)=='',-1, uniqueN(user_bank_account[ apply_diff==0 &  tar_user_bank_account!=user_bank_account & user_bank_account!=''])),    
    same_ip_diff_account_d3_apply_cnt=if_else(first(tar_user_ip)=='',-1, uniqueN(user_bank_account[ apply_diff<=3 &  tar_user_bank_account!=user_bank_account & user_bank_account!=''])),    
    same_ip_diff_account_d7_apply_cnt=if_else(first(tar_user_ip)=='',-1, uniqueN(user_bank_account[ apply_diff<=7 &  tar_user_bank_account!=user_bank_account & user_bank_account!=''])),    
    same_ip_diff_account_d14_apply_cnt=if_else(first(tar_user_ip)=='',-1, uniqueN(user_bank_account[ apply_diff<=14 &  tar_user_bank_account!=user_bank_account & user_bank_account!=''])),    
    same_ip_diff_account_d30_apply_cnt=if_else(first(tar_user_ip)=='',-1, uniqueN(user_bank_account[ apply_diff<=30 &  tar_user_bank_account!=user_bank_account & user_bank_account!=''])),   
    
    
    
  )%>%
  mutate(time=Sys.time())->ip_multi_var

ip_multi_var_frame<<-bind_rows(ip_multi_var_frame,ip_multi_var)
##account特征
sample_order%>%
  rename_with(~ paste0("tar_", .), everything())%>%
  # rename(user_phone=tar_user_phone)%>%
  select(tar_order_id,tar_user_id,tar_app_name,tar_user_name,tar_user_ip,tar_user_card_id,tar_user_phone,
         tar_user_device_id,tar_user_bank_account,tar_borrow_time)%>%
  filter(tar_user_bank_account!='')%>%
  left_join(dwd_order%>%select(user_id,order_id,user_name,user_phone,user_card_id,user_ip,user_device_id,user_bank_account,
                               borrow_time,user_type,repay_yes_time,app_name,loan_completion_time,repay_time,borrow_time,status),
            by=c('tar_user_bank_account'='user_bank_account') )->sample_account_order_join


sample_account_order_join%>%
  filter(borrow_time<=tar_borrow_time)%>% ##剔除申请时间在计算订单之后的记录
  # filter(order_id!=tar_order_id)%>% ## 剔除订单号相同的记录  ## 回溯订单状态 添加时间切片
  mutate(is_repay=if_else(repay_yes_time>tar_borrow_time | repay_yes_time=='',0,1),
         is_due=if_else(as.Date(repay_time)<=as.Date(tar_borrow_time),1,0),
         apply_diff=difftime(as.Date(tar_borrow_time),as.Date(borrow_time),units='day')%>%as.numeric(),
         due_diff=difftime(as.Date(tar_borrow_time),as.Date(repay_time),units='day')%>%as.numeric(),
         disburse_diff=difftime(as.Date(tar_borrow_time),as.Date(loan_completion_time),units='day')%>%as.numeric(),
         
  )->sample_account_dwd_order_tib



sample_account_dwd_order_tib%>%
  as_tibble()%>%
  group_by(tar_order_id)%>%
  summarise(
    
    account_nex_plat_apply_cnt=if_else(first(tar_user_bank_account)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name]) ),
    account_nex_plat_d0_apply_cnt=if_else(first(tar_user_bank_account)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & apply_diff==0]) ),
    account_nex_plat_d3_apply_cnt=if_else(first(tar_user_bank_account)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & apply_diff<=3]) ),
    account_nex_plat_d7_apply_cnt=if_else(first(tar_user_bank_account)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & apply_diff<=7]) ),
    account_nex_plat_d14_apply_cnt=if_else(first(tar_user_bank_account)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & apply_diff<=14]) ),
    account_nex_plat_d30_apply_cnt=if_else(first(tar_user_bank_account)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & apply_diff<=30]) ),
    
    account_nex_plat_reject_cnt=if_else(first(tar_user_bank_account)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & status==3]) ),
    account_nex_plat_d0_reject_cnt=if_else(first(tar_user_bank_account)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & status==3 & apply_diff==0]) ),    
    account_nex_plat_d3_reject_cnt=if_else(first(tar_user_bank_account)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & status==3 & apply_diff<=3]) ),    
    account_nex_plat_d7_reject_cnt=if_else(first(tar_user_bank_account)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & status==3 & apply_diff<=7]) ),     
    account_nex_plat_d14_reject_cnt=if_else(first(tar_user_bank_account)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & status==3 & apply_diff<=14]) ),    
    account_nex_plat_d30_reject_cnt=if_else(first(tar_user_bank_account)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & status==3 & apply_diff<=30]) ),   
    
    account_nex_plat_disburse_cnt= if_else(first(tar_user_bank_account)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8]) ),
    account_nex_plat_d0_disburse_cnt= if_else(first(tar_user_bank_account)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff==0]) ),
    account_nex_plat_d3_disburse_cnt= if_else(first(tar_user_bank_account)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff<=3]) ),
    account_nex_plat_d7_disburse_cnt= if_else(first(tar_user_bank_account)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff<=7]) ),
    account_nex_plat_d14_disburse_cnt= if_else(first(tar_user_bank_account)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff<=14]) ),
    account_nex_plat_d30_disburse_cnt= if_else(first(tar_user_bank_account)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff<=30]) ),    
    
    account_nex_plat_overdue_cnt= if_else(first(tar_user_bank_account)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & is_due==1 & is_repay==0]) ),
    account_nex_plat_d0_overdue_cnt= if_else(first(tar_user_bank_account)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff==0]) ),
    account_nex_plat_d3_overdue_cnt= if_else(first(tar_user_bank_account)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff<=3]) ),
    account_nex_plat_d7_overdue_cnt= if_else(first(tar_user_bank_account)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff<=7]) ),
    account_nex_plat_d14_overdue_cnt= if_else(first(tar_user_bank_account)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff<=14]) ),
    account_nex_plat_d30_overdue_cnt= if_else(first(tar_user_bank_account)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name!=tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff<=30]) ),    
    
    ##同证件号app内多头  
    account_nex_app_apply_cnt=if_else(first(tar_user_bank_account)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name]) ),
    account_nex_app_d0_apply_cnt=if_else(first(tar_user_bank_account)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & apply_diff==0]) ),
    account_nex_app_d3_apply_cnt=if_else(first(tar_user_bank_account)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & apply_diff<=3]) ),
    account_nex_app_d7_apply_cnt=if_else(first(tar_user_bank_account)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & apply_diff<=7]) ),
    account_nex_app_d14_apply_cnt=if_else(first(tar_user_bank_account)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & apply_diff<=14]) ),
    account_nex_app_d30_apply_cnt=if_else(first(tar_user_bank_account)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & apply_diff<=30]) ),
    
    account_nex_app_reject_cnt=if_else(first(tar_user_bank_account)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & status==3]) ),
    account_nex_app_d0_reject_cnt=if_else(first(tar_user_bank_account)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & status==3 & apply_diff==0]) ),    
    account_nex_app_d3_reject_cnt=if_else(first(tar_user_bank_account)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & status==3 & apply_diff<=3]) ),    
    account_nex_app_d7_reject_cnt=if_else(first(tar_user_bank_account)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & status==3 & apply_diff<=7]) ),     
    account_nex_app_d14_reject_cnt=if_else(first(tar_user_bank_account)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & status==3 & apply_diff<=14]) ),    
    account_nex_app_d30_reject_cnt=if_else(first(tar_user_bank_account)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & status==3 & apply_diff<=30]) ),   
    
    account_nex_app_disburse_cnt= if_else(first(tar_user_bank_account)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & status>=8]) ),
    account_nex_app_d0_disburse_cnt= if_else(first(tar_user_bank_account)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff==0]) ),
    account_nex_app_d3_disburse_cnt= if_else(first(tar_user_bank_account)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff<=3]) ),
    account_nex_app_d7_disburse_cnt= if_else(first(tar_user_bank_account)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff<=7]) ),
    account_nex_app_d14_disburse_cnt= if_else(first(tar_user_bank_account)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff<=14]) ),
    account_nex_app_d30_disburse_cnt= if_else(first(tar_user_bank_account)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & loan_completion_time<=tar_borrow_time & disburse_diff<=30]) ),    
    
    account_nex_app_overdue_cnt= if_else(first(tar_user_bank_account)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & is_due==1 & is_repay==0]) ),
    account_nex_app_d0_overdue_cnt= if_else(first(tar_user_bank_account)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff==0]) ),
    account_nex_app_d3_overdue_cnt= if_else(first(tar_user_bank_account)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff<=3]) ),
    account_nex_app_d7_overdue_cnt= if_else(first(tar_user_bank_account)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff<=7]) ),
    account_nex_app_d14_overdue_cnt= if_else(first(tar_user_bank_account)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff<=14]) ),
    account_nex_app_d30_overdue_cnt= if_else(first(tar_user_bank_account)=='',-1,uniqueN(order_id[ user_id!=tar_user_id & app_name==tar_app_name & status>=8 & is_due==1 & is_repay==0 & due_diff<=30]) ),    
    
    same_account_diff_device_apply_cnt=if_else(first(tar_user_bank_account)=='',-1, uniqueN(user_device_id[  tar_user_device_id!=user_device_id & user_device_id!=''])),
    same_account_diff_device_d0_apply_cnt=if_else(first(tar_user_bank_account)=='',-1, uniqueN(user_device_id[ apply_diff==0 &  tar_user_device_id!=user_device_id & user_device_id!=''])),    
    same_account_diff_device_d3_apply_cnt=if_else(first(tar_user_bank_account)=='',-1, uniqueN(user_device_id[ apply_diff<=3 &  tar_user_device_id!=user_device_id & user_device_id!=''])),    
    same_account_diff_device_d7_apply_cnt=if_else(first(tar_user_bank_account)=='',-1, uniqueN(user_device_id[ apply_diff<=7 &  tar_user_device_id!=user_device_id & user_device_id!=''])),    
    same_account_diff_device_d14_apply_cnt=if_else(first(tar_user_bank_account)=='',-1, uniqueN(user_device_id[ apply_diff<=14 &  tar_user_device_id!=user_device_id & user_device_id!=''])),    
    same_account_diff_device_d30_apply_cnt=if_else(first(tar_user_bank_account)=='',-1, uniqueN(user_device_id[ apply_diff<=30 &  tar_user_device_id!=user_device_id & user_device_id!=''])),  
    
    same_account_diff_cardid_apply_cnt=if_else(first(tar_user_bank_account)=='',-1, uniqueN(user_card_id[  tar_user_card_id!=user_card_id & user_card_id!=''])),
    same_account_diff_cardid_d0_apply_cnt=if_else(first(tar_user_bank_account)=='',-1, uniqueN(user_card_id[ apply_diff==0 &  tar_user_card_id!=user_card_id & user_card_id!=''])),    
    same_account_diff_cardid_d3_apply_cnt=if_else(first(tar_user_bank_account)=='',-1, uniqueN(user_card_id[ apply_diff<=3 &  tar_user_card_id!=user_card_id & user_card_id!=''])),    
    same_account_diff_cardid_d7_apply_cnt=if_else(first(tar_user_bank_account)=='',-1, uniqueN(user_card_id[ apply_diff<=7 &  tar_user_card_id!=user_card_id & user_card_id!=''])),    
    same_account_diff_cardid_d14_apply_cnt=if_else(first(tar_user_bank_account)=='',-1, uniqueN(user_card_id[ apply_diff<=14 &  tar_user_card_id!=user_card_id & user_card_id!=''])),    
    same_account_diff_cardid_d30_apply_cnt=if_else(first(tar_user_bank_account)=='',-1, uniqueN(user_card_id[ apply_diff<=30 &  tar_user_card_id!=user_card_id & user_card_id!=''])),  
    
    same_account_diff_ip_apply_cnt=if_else(first(tar_user_bank_account)=='',-1, uniqueN(user_ip[  tar_user_ip!=user_ip & user_ip!=''])),
    same_account_diff_ip_d0_apply_cnt=if_else(first(tar_user_bank_account)=='',-1, uniqueN(user_ip[ apply_diff==0 &  tar_user_ip!=user_ip & user_ip!=''])),    
    same_account_diff_ip_d3_apply_cnt=if_else(first(tar_user_bank_account)=='',-1, uniqueN(user_ip[ apply_diff<=3 &  tar_user_ip!=user_ip & user_ip!=''])),    
    same_account_diff_ip_d7_apply_cnt=if_else(first(tar_user_bank_account)=='',-1, uniqueN(user_ip[ apply_diff<=7 &  tar_user_ip!=user_ip & user_ip!=''])),    
    same_account_diff_ip_d14_apply_cnt=if_else(first(tar_user_bank_account)=='',-1, uniqueN(user_ip[ apply_diff<=14 &  tar_user_ip!=user_ip & user_ip!=''])),    
    same_account_diff_ip_d30_apply_cnt=if_else(first(tar_user_bank_account)=='',-1, uniqueN(user_ip[ apply_diff<=30 &  tar_user_ip!=user_ip & user_ip!=''])),  
    
    same_account_diff_phone_apply_cnt=if_else(first(tar_user_bank_account)=='',-1, uniqueN(user_phone[  tar_user_phone!=user_phone & user_phone!=''])),
    same_account_diff_phone_d0_apply_cnt=if_else(first(tar_user_bank_account)=='',-1, uniqueN(user_phone[ apply_diff==0 &  tar_user_phone!=user_phone & user_phone!=''])),    
    same_account_diff_phone_d3_apply_cnt=if_else(first(tar_user_bank_account)=='',-1, uniqueN(user_phone[ apply_diff<=3 &  tar_user_phone!=user_phone & user_phone!=''])),    
    same_account_diff_phone_d7_apply_cnt=if_else(first(tar_user_bank_account)=='',-1, uniqueN(user_phone[ apply_diff<=7 &  tar_user_phone!=user_phone & user_phone!=''])),    
    same_account_diff_phone_d14_apply_cnt=if_else(first(tar_user_bank_account)=='',-1, uniqueN(user_phone[ apply_diff<=14 &  tar_user_phone!=user_phone & user_phone!=''])),    
    same_account_diff_phone_d30_apply_cnt=if_else(first(tar_user_bank_account)=='',-1, uniqueN(user_phone[ apply_diff<=30 &  tar_user_phone!=user_phone & user_phone!=''])),  
    
    
  )%>%
  mutate(time=Sys.time())->account_multi_var

account_multi_var_frame<<-bind_rows(account_multi_var_frame,account_multi_var)
print(paste(i,'phone_multi_var_frame:',dim(phone_multi_var_frame)[1],' account_multi_var_frame:',dim(account_multi_var_frame)[1],Sys.time(),sep=" "))

i=num+1
print(paste('i递增后的值',i,Sys.time()),sep="",)

}



tryCatch({
  drv <- dbDriver("PostgreSQL")
  connec_bu <- dbConnect(drv,
                         dbname = "test",
                         host ="8.210.75.205",
                         port = 13343,
                         user = "BASIC$root",
                         options="-c search_path=bengal_test",
                         password = "WCjak3$RQnf3ST")
  print("Database Connected!")
},
error=function(cond) {
  print("Unable to connect to Database.")
})

dbWriteTable(connec_bu,'phone_apply_multi_var',phone_multi_var_frame,overwrite=T,row.names=F)
dbWriteTable(connec_bu,'card_multi_var',card_multi_var_frame,overwrite=T,row.names=F)
dbWriteTable(connec_bu,'device_multi_var',device_multi_var_frame,overwrite=T,row.names=F)
dbWriteTable(connec_bu,'ip_multi_var',ip_multi_var_frame,overwrite=T,row.names=F)
dbWriteTable(connec_bu,'account_multi_var',account_multi_var_frame,overwrite=T,row.names=F)

dbDisconnect(connec_bu)


