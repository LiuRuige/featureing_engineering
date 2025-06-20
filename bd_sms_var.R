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

library(tidyverse)
library(data.table)

due_order<-dbGetQuery(connec_bu,"select order_id,user_phone from bengal_test.bd_dwd_order where status>=8")

bank_content='ebl|brac-bank|city bank|ucb.|pubali bank|ific bank|brac bank|trustbank|ucb|sonali bank|ibbl .|mtb.|ebl.|dmcb|islami.bank|dhaka bank|primebank|islami bank|ibbl|agrani bank|citybank|aibl|bank asia|islamibank|islamic|sebl cards|bank|bangladesh bank|sonali bank|agrani bank|rupali bank|janata bank|সোনালী ব্যাংক|অগ্রণী ব্যাংক|রূপালী ব্যাংক|জনতা ব্যাংক|dutch bangla bank|ডাচ-বাংলা ব্যাংক|brac bank|ব্র্যাক ব্যাংক|city bank|সিটি ব্যাংক|eastern bank ltd|ebl|ইস্টার্ন ব্যাংক|islami bank bangladesh|ব্যাংক|ইসলামী ব্যাংক বাংলাদেশ|standard chartered bangladesh|স্ট্যান্ডার্ড চার্টার্ড|hsbc bangladesh|এইচএসবিসি|grameen bank|গ্রামীণ ব্যাংক|brac microfinance|ব্র্যাক ক্ষুদ্রঋণ'
bank_name='ebl|brac-bank|city bank|ucb.|pubali bank|ific bank|brac bank|trustbank|ucb|sonali bank|ibbl .|mtb.|ebl.|dmcb|islami.bank|dhaka bank|primebank|islami bank|ibbl|agrani bank|citybank|aibl|bank asia|islamibank|islamic|sebl cards'
telcom='robi|aritel|teletalk|mygp|grameenphone|গ্রামীণফোন|রবি|বাংলালিংক|টেলিটক|এয়ারটেল'

sms_var_frame<-data.frame()
sms_credit_var_frame<-data.frame()

# sample_order<-due_order[1000:2000,]


# i=8009
# num=0

while(i<dim(due_order)[1]){
  num=min((i+1000),dim(due_order)[1])
  due_order[i:num,]->sample_order
print(paste(Sys.time(),' 开始执行'))
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
sample_sms<-dbGetQuery(connec_bu,str_squish(paste("select order_id,app_name,create_time,content,phone,type,borrow_time 
                                           from bengal_test.bd_user_sms where order_id in('",paste(sample_order$order_id,collapse="','",sep=""),
                                                  "')",sep="")))

dbDisconnect(connec_bu)
print(paste(Sys.time(),' 订单查询结束'))

## 短信区分 运营商、银行、支付、金融类短信，构建特征 记录数不同公司个数，同时添加时间切片
## 银行或者支付的正则短信


##discount

# credit='cash in received|money received|b2b received'   ## uddokta 商户 sender 个人
# debit='cash out|payment|b2b transfer successful|cash out'

sample_sms%>%
  select(order_id,content,phone,type,create_time,borrow_time)%>%
  as_tibble()%>%
  mutate(apply_diff=difftime(as.Date(borrow_time),as.Date(create_time),units='days')%>%as.numeric(),
         content=str_to_lower(content),
         phone=str_to_lower(phone)
         )->sample_sms_select

# sample_sms_select%>%
#   filter(phone %in% c('nagad','bkash'))%>%
#   mutate(
#          trx=if_else(str_detect(content,'trxid|txnid')& !str_detect(content,"reverse|fail|wait for|cancel|returned|processing"),1,0),
#          is_credit=if_else(str_detect(content,"binimoy received fund|you have received|cash in received|money received|add money|cash in|b2b received|remittance received") & str_detect(content,'trxid|txnid') & !str_detect(content,"reverse|fail|wait for|cancel|returned|processing"),1,0),
#          is_debit=if_else(str_detect(content,"^payment tk|merchant payment|payment reversal|disbursement received|payment received|bkash to bank of|savings deposit|payment of|cash out received|payment to|send money|bill payment|b2b transfer|bill successfully paid|cash out|b2b transfer") & str_detect(content,'trxid|txnid') & !str_detect(content,'reverse|fail|wait for|cancel|returned|processing'),1,0),
#          amount=str_extract(content, "(?<=tk\\s{0,5})[\\d,]+(?:\\.[\\d]{2})?") %>%
#            str_replace_all(",", "") %>%
#            as.numeric(),
#          balance=str_extract(content, "(?<=balance)[\\s:tk\\.]*[\\d,]+(?:\\.[\\d]{2})?") %>%
#            str_extract("[\\d,]+(?:\\.[\\d]{2})?") %>%
#            str_replace_all(",", "") %>%
#            as.numeric(),
#          
#          )->x

##必带success
##nagad credit  	cash in received(uddokta)|money received(sender)|add money(success)|cash in(success)|b2b received|remittance received
##nagad debit     cash out received|payment to(success)|send money|bill payment(success)|b2b transfer(success)|mobile recharge request received
##bkash credit    cash in(success)|you have received
##bkash debit     payment of(success)|bill successfully paid|cash out(success)|b2b transfer(success)|

sample_sms_select%>%
  mutate(
    is_telcom=if_else(str_detect(phone,telcom)|str_detect(content,telcom),1,0),
    is_bank=if_else(str_detect(content,bank_content),1,0),
    is_due=if_else(str_detect(content,'due'),1,0),
    is_overdue=if_else(str_detect(content,'penal|overdue|বিলম্বিত'),1,0),
    is_loan=if_else(str_detect(content,'loan|ধার'),1,0),
    is_bkash=if_else(str_detect(phone,'bkash'),1,0),
    is_nagad=if_else(str_detect(phone,'nagad'),1,0),
    is_trx=if_else(str_detect(content,'trxid|txnid')& !str_detect(content,"reverse|fail|wait for|cancel|returned|processing"),1,0),
    is_credit=if_else(str_detect(content,"binimoy received fund|you have received|cash in received|money received|add money|cash in|b2b received|remittance received") & str_detect(content,'trxid|txnid') & !str_detect(content,"reverse|fail|wait for|cancel|returned|processing"),1,0),
    is_debit=if_else(str_detect(content,"^payment tk|merchant payment|payment reversal|disbursement received|payment received|bkash to bank of|savings deposit|payment of|cash out received|payment to|send money|bill payment|b2b transfer|bill successfully paid|cash out|b2b transfer") & str_detect(content,'trxid|txnid') & !str_detect(content,'reverse|fail|wait for|cancel|returned|processing'),1,0),
    amount=if_else(is_trx==1,str_extract(content, "(?<=tk\\s{0,5})[\\d,]+(?:\\.[\\d]{2})?") %>%
      str_replace_all(",", "") %>%
      as.numeric(),NA),
    balance=if_else(is_trx==1,str_extract(content, "(?<=balance)[\\s:tk\\.]*[\\d,]+(?:\\.[\\d]{2})?") %>%
      str_extract("[\\d,]+(?:\\.[\\d]{2})?") %>%
      str_replace_all(",", "") %>%
      as.numeric(),NA),
    is_otp=if_else(str_detect(content,"otp|verification|one time pin|one time code|one time password"),1,0),
    is_whatsapp_code=if_else(str_detect(content,"whatsapp code"),1,0),
    is_whatsapp_business_code=if_else(str_detect(content," whatsapp business code"),1,0),
    is_meter=if_else(str_detect(content,'meter'),1,0),
    is_personal_phone=if_else(str_detect(phone,'\\+880'),1,0),
    is_bet=if_else(str_detect(content,'bet|casino|gambl|wager|slot|বাজি|ক্যাসিনো|জুয়া|বাজি|স্লট'),1,0),
    is_recharge=if_else(str_detect(content,'recharge'),1,0),
    # is_robi=if_else(),
    
    
  )->sample_sms_keywd_detect
  


sample_sms_keywd_detect%>%
  group_by(order_id)%>%
  summarise(
    sms_cnt=n(),
    sms_in_cnt=sum(type==1),
    sms_in_d0_cnt=sum(type==1 & apply_diff<=0),
    sms_in_d3_cnt=sum(type==1 & apply_diff<=3),
    sms_in_d7_cnt=sum(type==1 & apply_diff<=7),
    sms_in_d14_cnt=sum(type==1 & apply_diff<=14),
    sms_in_d30_cnt=sum(type==1 & apply_diff<=30),
    
    sms_in_personal_phone_cnt=sum(type==1 & is_personal_phone==1),
    sms_in_d0_personal_phone_cnt=sum(type==1 & is_personal_phone==1 & apply_diff<=0),
    sms_in_d3_personal_phone_cnt=sum(type==1 & is_personal_phone==1 & apply_diff<=3),
    sms_in_d7_personal_phone_cnt=sum(type==1 & is_personal_phone==1 & apply_diff<=7),
    sms_in_d14_personal_phone_cnt=sum(type==1 & is_personal_phone==1 & apply_diff<=14),
    sms_in_d30_personal_phone_cnt=sum(type==1 & is_personal_phone==1 & apply_diff<=30),
    
    sms_in_personal_phone_unique_phone=uniqueN(phone[type==1 & is_personal_phone==1]),
    sms_in_d0_personal_phone_unique_phone=uniqueN(phone[type==1 & is_personal_phone==1 & apply_diff<=0]),
    sms_in_d3_personal_phone_unique_phone=uniqueN(phone[type==1 & is_personal_phone==1 & apply_diff<=3]),
    sms_in_d7_personal_phone_unique_phonet=uniqueN(phone[type==1 & is_personal_phone==1 & apply_diff<=7]),
    sms_in_d14_personal_phone_unique_phonet=uniqueN(phone[type==1 & is_personal_phone==1 & apply_diff<=14]),
    sms_in_d30_personal_phone_unique_phonet=uniqueN(phone[type==1 & is_personal_phone==1 & apply_diff<=30]),
    
    sms_in_unique_phone=uniqueN(phone[type==1]),
    sms_in_d0_unique_phone=uniqueN(phone[type==1 & apply_diff<=0]),
    sms_in_d3_unique_phone=uniqueN(phone[type==1 & apply_diff<=3]),
    sms_in_d7_unique_phone=uniqueN(phone[type==1 & apply_diff<=7]),
    sms_in_d14_unique_phone=uniqueN(phone[type==1 & apply_diff<=14]),
    sms_in_d30_unique_phone=uniqueN(phone[type==1 & apply_diff<=30]),
    
    sms_in_otp_cnt=sum(type==1 & is_otp==1),
    sms_in_d0_otp_cnt=sum(type==1 & is_otp==1 & apply_diff<=0),
    sms_in_d3_otp_cnt=sum(type==1 & is_otp==1 & apply_diff<=3),
    sms_in_d7_otp_cnt=sum(type==1 & is_otp==1 & apply_diff<=7),
    sms_in_d14_otp_cnt=sum(type==1 & is_otp==1 & apply_diff<=14),
    sms_in_d30_otp_cnt=sum(type==1 & is_otp==1 & apply_diff<=30),
    
    sms_in_otp_unique_phone=uniqueN(phone[type==1 & is_otp==1]),
    sms_in_otp_d0_unique_phone=uniqueN(phone[type==1 & is_otp==1 & apply_diff<=0]),
    sms_in_otp_d3_unique_phone=uniqueN(phone[type==1 & is_otp==1 & apply_diff<=3]),
    sms_in_otp_d7_unique_phone=uniqueN(phone[type==1 & is_otp==1 & apply_diff<=7]),
    sms_in_otp_d14_unique_phone=uniqueN(phone[type==1 & is_otp==1 & apply_diff<=14]),
    sms_in_otp_d30_unique_phone=uniqueN(phone[type==1 & is_otp==1 & apply_diff<=30]),
    
    sms_in_telcom_cnt=sum(type==1 & is_telcom==1),
    sms_in_d0_telcom_cnt=sum(type==1 & is_telcom==1 & apply_diff<=0),
    sms_in_d3_telcom_cnt=sum(type==1 & is_telcom==1 & apply_diff<=3),
    sms_in_d7_telcom_cnt=sum(type==1 & is_telcom==1 & apply_diff<=7),
    sms_in_d14_telcom_cnt=sum(type==1 & is_telcom==1 & apply_diff<=14),
    sms_in_d30_telcom_cnt=sum(type==1 & is_telcom==1 & apply_diff<=30),
    
    sms_in_telcom_unique_phone=uniqueN(phone[type==1 & is_telcom==1]),
    sms_in_telcom_d0_unique_phone=uniqueN(phone[type==1 & is_telcom==1 & apply_diff<=0]),
    sms_in_telcom_d3_unique_phone=uniqueN(phone[type==1 & is_telcom==1 & apply_diff<=3]),
    sms_in_telcom_d7_unique_phone=uniqueN(phone[type==1 & is_telcom==1 & apply_diff<=7]),
    sms_in_telcom_d14_unique_phone=uniqueN(phone[type==1 & is_telcom==1 & apply_diff<=14]),
    sms_in_telcom_d30_unique_phone=uniqueN(phone[type==1 & is_telcom==1 & apply_diff<=30]),
    
    sms_in_bank_cnt=sum(type==1 & is_bank==1),
    sms_in_d0_bank_cnt=sum(type==1 & is_bank==1 & apply_diff<=0),
    sms_in_d3_bank_cnt=sum(type==1 & is_bank==1 & apply_diff<=3),
    sms_in_d7_bank_cnt=sum(type==1 & is_bank==1 & apply_diff<=7),
    sms_in_d14_bank_cnt=sum(type==1 & is_bank==1 & apply_diff<=14),
    sms_in_d30_bank_cnt=sum(type==1 & is_bank==1 & apply_diff<=30),
    
    sms_in_bank_unique_phone=uniqueN(phone[type==1 & is_bank==1]),
    sms_in_bank_d0_unique_phone=uniqueN(phone[type==1 & is_bank==1 & apply_diff<=0]),
    sms_in_bank_d3_unique_phone=uniqueN(phone[type==1 & is_bank==1 & apply_diff<=3]),
    sms_in_bank_d7_unique_phone=uniqueN(phone[type==1 & is_bank==1 & apply_diff<=7]),
    sms_in_bank_d14_unique_phone=uniqueN(phone[type==1 & is_bank==1 & apply_diff<=14]),
    sms_in_bank_d30_unique_phone=uniqueN(phone[type==1 & is_bank==1 & apply_diff<=30]),
    
    sms_in_due_cnt=sum(type==1 & is_due==1),
    sms_in_d0_due_cnt=sum(type==1 & is_due==1 & apply_diff<=0),
    sms_in_d3_due_cnt=sum(type==1 & is_due==1 & apply_diff<=3),
    sms_in_d7_due_cnt=sum(type==1 & is_due==1 & apply_diff<=7),
    sms_in_d14_due_cnt=sum(type==1 & is_due==1 & apply_diff<=14),
    sms_in_d30_due_cnt=sum(type==1 & is_due==1 & apply_diff<=30),
    
    sms_in_due_unique_phone=uniqueN(phone[type==1 & is_due==1]),
    sms_in_due_d0_unique_phone=uniqueN(phone[type==1 & is_due==1 & apply_diff<=0]),
    sms_in_due_d3_unique_phone=uniqueN(phone[type==1 & is_due==1 & apply_diff<=3]),
    sms_in_due_d7_unique_phone=uniqueN(phone[type==1 & is_due==1 & apply_diff<=7]),
    sms_in_due_d14_unique_phone=uniqueN(phone[type==1 & is_due==1 & apply_diff<=14]),
    sms_in_due_d30_unique_phone=uniqueN(phone[type==1 & is_due==1 & apply_diff<=30]),
    
    sms_in_overdue_cnt=sum(type==1 & is_overdue==1),
    sms_in_d0_overdue_cnt=sum(type==1 & is_overdue==1 & apply_diff<=0),
    sms_in_d3_overdue_cnt=sum(type==1 & is_overdue==1 & apply_diff<=3),
    sms_in_d7_overdue_cnt=sum(type==1 & is_overdue==1 & apply_diff<=7),
    sms_in_d14_overdue_cnt=sum(type==1 & is_overdue==1 & apply_diff<=14),
    sms_in_d30_overdue_cnt=sum(type==1 & is_overdue==1 & apply_diff<=30),
    
    sms_in_overdue_unique_phone=uniqueN(phone[type==1 & is_overdue==1]),
    sms_in_overdue_d0_unique_phone=uniqueN(phone[type==1 & is_overdue==1 & apply_diff<=0]),
    sms_in_overdue_d3_unique_phone=uniqueN(phone[type==1 & is_overdue==1 & apply_diff<=3]),
    sms_in_overdue_d7_unique_phone=uniqueN(phone[type==1 & is_overdue==1 & apply_diff<=7]),
    sms_in_overdue_d14_unique_phone=uniqueN(phone[type==1 & is_overdue==1 & apply_diff<=14]),
    sms_in_overdue_d30_unique_phone=uniqueN(phone[type==1 & is_overdue==1 & apply_diff<=30]),
    
    sms_in_loan_cnt=sum(type==1 & is_loan==1),
    sms_in_d0_loan_cnt=sum(type==1 & is_loan==1 & apply_diff<=0),
    sms_in_d3_loan_cnt=sum(type==1 & is_loan==1 & apply_diff<=3),
    sms_in_d7_loan_cnt=sum(type==1 & is_loan==1 & apply_diff<=7),
    sms_in_d14_loan_cnt=sum(type==1 & is_loan==1 & apply_diff<=14),
    sms_in_d30_loan_cnt=sum(type==1 & is_loan==1 & apply_diff<=30),
    
    sms_in_loan_unique_phone=uniqueN(phone[type==1 & is_loan==1]),
    sms_in_loan_d0_unique_phone=uniqueN(phone[type==1 & is_loan==1 & apply_diff<=0]),
    sms_in_loan_d3_unique_phone=uniqueN(phone[type==1 & is_loan==1 & apply_diff<=3]),
    sms_in_loan_d7_unique_phone=uniqueN(phone[type==1 & is_loan==1 & apply_diff<=7]),
    sms_in_loan_d14_unique_phone=uniqueN(phone[type==1 & is_loan==1 & apply_diff<=14]),
    sms_in_loan_d30_unique_phone=uniqueN(phone[type==1 & is_loan==1 & apply_diff<=30]),
    
    sms_in_meter_cnt=sum(type==1 & is_meter==1),
    sms_in_d0_meter_cnt=sum(type==1 & is_meter==1 & apply_diff<=0),
    sms_in_d3_meter_cnt=sum(type==1 & is_meter==1 & apply_diff<=3),
    sms_in_d7_meter_cnt=sum(type==1 & is_meter==1 & apply_diff<=7),
    sms_in_d14_meter_cnt=sum(type==1 & is_meter==1 & apply_diff<=14),
    sms_in_d30_meter_cnt=sum(type==1 & is_meter==1 & apply_diff<=30),
    
    sms_in_bet_cnt=sum(type==1 & is_bet==1),
    sms_in_d0_bet_cnt=sum(type==1 & is_bet==1 & apply_diff<=0),
    sms_in_d3_bet_cnt=sum(type==1 & is_bet==1 & apply_diff<=3),
    sms_in_d7_bet_cnt=sum(type==1 & is_bet==1 & apply_diff<=7),
    sms_in_d14_bet_cnt=sum(type==1 & is_bet==1 & apply_diff<=14),
    sms_in_d30_bet_cnt=sum(type==1 & is_bet==1 & apply_diff<=30),
    
    sms_in_recharge_cnt=sum(type==1 & is_recharge==1),
    sms_in_d0_recharge_cnt=sum(type==1 & is_recharge==1 & apply_diff<=0),
    sms_in_d3_recharge_cnt=sum(type==1 & is_recharge==1 & apply_diff<=3),
    sms_in_d7_recharge_cnt=sum(type==1 & is_recharge==1 & apply_diff<=7),
    sms_in_d14_recharge_cnt=sum(type==1 & is_recharge==1 & apply_diff<=14),
    sms_in_d30_recharge_cnt=sum(type==1 & is_recharge==1 & apply_diff<=30),
    
    sms_in_recharge_telcom_cnt=sum(type==1 & is_recharge==1& is_telcom==1),
    sms_in_d0_recharge_telcom_cnt=sum(type==1 & is_recharge==1 & apply_diff<=0& is_telcom==1),
    sms_in_d3_recharge_telcom_cnt=sum(type==1 & is_recharge==1 & apply_diff<=3& is_telcom==1),
    sms_in_d7_recharge_telcom_cnt=sum(type==1 & is_recharge==1 & apply_diff<=7& is_telcom==1),
    sms_in_d14_recharge_telcom_cnt=sum(type==1 & is_recharge==1 & apply_diff<=14& is_telcom==1),
    sms_in_d30_recharge_telcom_cnt=sum(type==1 & is_recharge==1 & apply_diff<=30& is_telcom==1),
    
    sms_in_whatsapp_code_cnt=sum(type==1 & is_whatsapp_code==1),
    sms_in_d0_whatsapp_code_cnt=sum(type==1 & is_whatsapp_code==1 & apply_diff<=0),
    sms_in_d3_whatsapp_code_cnt=sum(type==1 & is_whatsapp_code==1 & apply_diff<=3),
    sms_in_d7_whatsapp_code_cnt=sum(type==1 & is_whatsapp_code==1 & apply_diff<=7),
    sms_in_d14_whatsapp_code_cnt=sum(type==1 & is_whatsapp_code==1 & apply_diff<=14),
    sms_in_d30_whatsapp_code_cnt=sum(type==1 & is_whatsapp_code==1 & apply_diff<=30),
    
    sms_in_whatsapp_business_code_cnt=sum(type==1 & is_whatsapp_business_code==1),
    sms_in_d0_whatsapp_business_code_cnt=sum(type==1 & is_whatsapp_business_code==1 & apply_diff<=0),
    sms_in_d3_whatsapp_business_code_cnt=sum(type==1 & is_whatsapp_business_code==1 & apply_diff<=3),
    sms_in_d7_whatsapp_business_code_cnt=sum(type==1 & is_whatsapp_business_code==1 & apply_diff<=7),
    sms_in_d14_whatsapp_business_code_cnt=sum(type==1 & is_whatsapp_business_code==1 & apply_diff<=14),
    sms_in_d30_whatsapp_business_code_cnt=sum(type==1 & is_whatsapp_business_code==1 & apply_diff<=30),
    
    sms_out_cnt=sum(type==2),
    sms_out_d0_cnt=sum(type==2 & apply_diff<=0),
    sms_out_d3_cnt=sum(type==2 & apply_diff<=3),
    sms_out_d7_cnt=sum(type==2 & apply_diff<=7),
    sms_out_d14_cnt=sum(type==2 & apply_diff<=14),
    sms_out_d30_cnt=sum(type==2 & apply_diff<=30),
    
  )->sms_var

sample_sms_keywd_detect%>%
  group_by(order_id)%>%
  summarise(
    sms_in_bkash_cnt=sum(type==1 & is_bkash==1),
    sms_in_d0_bkash_cnt=sum(type==1 & is_bkash==1 & apply_diff<=0),
    sms_in_d3_bkash_cnt=sum(type==1 & is_bkash==1 & apply_diff<=3),
    sms_in_d7_bkash_cnt=sum(type==1 & is_bkash==1 & apply_diff<=7),
    sms_in_d14_bkash_cnt=sum(type==1 & is_bkash==1 & apply_diff<=14),
    sms_in_d30_bkash_cnt=sum(type==1 & is_bkash==1 & apply_diff<=30),
    
    sms_in_nagad_cnt=sum(type==1 & is_nagad==1),
    sms_in_d0_nagad_cnt=sum(type==1 & is_nagad==1 & apply_diff<=0),
    sms_in_d3_nagad_cnt=sum(type==1 & is_nagad==1 & apply_diff<=3),
    sms_in_d7_nagad_cnt=sum(type==1 & is_nagad==1 & apply_diff<=7),
    sms_in_d14_nagad_cnt=sum(type==1 & is_nagad==1 & apply_diff<=14),
    sms_in_d30_nagad_cnt=sum(type==1 & is_nagad==1 & apply_diff<=30),
    
    sms_in_trx_cnt=sum(type==1 & (is_bkash==1| is_nagad==1) & is_trx==1),
    sms_in_d0_trx_cnt=sum(type==1 & (is_bkash==1| is_nagad==1) & is_trx==1 & apply_diff<=0),
    sms_in_d3_trx_cnt=sum(type==1 & (is_bkash==1| is_nagad==1) & is_trx==1 & apply_diff<=3),
    sms_in_d7_trx_cnt=sum(type==1 & (is_bkash==1| is_nagad==1) & is_trx==1 & apply_diff<=7),
    sms_in_d14_trx_cnt=sum(type==1 & (is_bkash==1| is_nagad==1) & is_trx==1 & apply_diff<=14),
    sms_in_d30_trx_cnt=sum(type==1 & (is_bkash==1| is_nagad==1) & is_trx==1 & apply_diff<=30),
    
    sms_in_trx_unique_phone=uniqueN(phone[type==1 & (is_bkash==1| is_nagad==1) & is_trx==1]),
    sms_in_d0_trx_unique_phone=uniqueN(phone[type==1 & (is_bkash==1| is_nagad==1) & is_trx==1 & apply_diff<=0]),
    sms_in_d3_trx_unique_phone=uniqueN(phone[type==1 & (is_bkash==1| is_nagad==1) & is_trx==1 & apply_diff<=3]),
    sms_in_d7_trx_unique_phone=uniqueN(phone[type==1 & (is_bkash==1| is_nagad==1) & is_trx==1 & apply_diff<=7]),
    sms_in_d14_trx_unique_phone=uniqueN(phone[type==1 & (is_bkash==1| is_nagad==1) & is_trx==1 & apply_diff<=14]),
    sms_in_d30_trx_unique_phone=uniqueN(phone[type==1 & (is_bkash==1| is_nagad==1) & is_trx==1 & apply_diff<=30]),
    
    sms_in_bkash_trx_cnt=sum(type==1 & is_bkash==1 & is_trx==1),
    sms_in_d0_bkash_trx_cnt=sum(type==1 & is_bkash==1 & is_trx==1 & apply_diff<=0),
    sms_in_d3_bkash_trx_cnt=sum(type==1 & is_bkash==1 & is_trx==1 & apply_diff<=3),
    sms_in_d7_bkash_trx_cnt=sum(type==1 & is_bkash==1 & is_trx==1 & apply_diff<=7),
    sms_in_d14_bkash_trx_cnt=sum(type==1 & is_bkash==1 & is_trx==1 & apply_diff<=14),
    sms_in_d30_bkash_trx_cnt=sum(type==1 & is_bkash==1 & is_trx==1 & apply_diff<=30),
    
    sms_in_nagad_trx_cnt=sum(type==1 & is_nagad==1 & is_trx==1),
    sms_in_d0_nagad_trx_cnt=sum(type==1 & is_nagad==1 & is_trx==1 & apply_diff<=0),
    sms_in_d3_nagad_trx_cnt=sum(type==1 & is_nagad==1 & is_trx==1 & apply_diff<=3),
    sms_in_d7_nagad_trx_cnt=sum(type==1 & is_nagad==1 & is_trx==1 & apply_diff<=7),
    sms_in_d14_nagad_trx_cnt=sum(type==1 & is_nagad==1 & is_trx==1 & apply_diff<=14),
    sms_in_d30_nagad_trx_cnt=sum(type==1 & is_nagad==1 & is_trx==1 & apply_diff<=30),
    
    sms_in_credit_cnt=sum(type==1 & is_trx==1 & (is_nagad==1 | is_nagad==1) & is_credit==1),
    sms_in_d0_credit_cnt=sum(type==1 & is_trx==1 & (is_nagad==1 | is_nagad==1) & is_credit==1 & apply_diff<=0),
    sms_in_d3_credit_cnt=sum(type==1 & is_trx==1 & (is_nagad==1 | is_nagad==1) & is_credit==1 & apply_diff<=3),
    sms_in_d7_credit_cnt=sum(type==1 & is_trx==1 & (is_nagad==1 | is_nagad==1) & is_credit==1 & apply_diff<=7),
    sms_in_d14_credit_cnt=sum(type==1 & is_trx==1 & (is_nagad==1 | is_nagad==1) & is_credit==1 & apply_diff<=14),
    sms_in_d30_credit_cnt=sum(type==1 & is_trx==1 & (is_nagad==1 | is_nagad==1) & is_credit==1 & apply_diff<=30), 
    
    sms_in_nagad_credit_cnt=sum(type==1 & is_trx==1 & is_nagad==1 & is_credit==1),
    sms_in_d0_nagad_credit_cnt=sum(type==1 & is_trx==1 & is_nagad==1 & is_credit==1 & apply_diff<=0),
    sms_in_d3_nagad_credit_cnt=sum(type==1 & is_trx==1 & is_nagad==1 & is_credit==1 & apply_diff<=3),
    sms_in_d7_nagad_credit_cnt=sum(type==1 & is_trx==1 & is_nagad==1 & is_credit==1 & apply_diff<=7),
    sms_in_d14_nagad_credit_cnt=sum(type==1 & is_trx==1 & is_nagad==1 & is_credit==1 & apply_diff<=14),
    sms_in_d30_nagad_credit_cnt=sum(type==1 & is_trx==1 & is_nagad==1 & is_credit==1 & apply_diff<=30), 
    
    sms_in_bkash_credit_cnt=sum(type==1 & is_trx==1 & is_bkash==1 & is_credit==1),
    sms_in_d0_bkash_credit_cnt=sum(type==1 & is_trx==1 & is_bkash==1 & is_credit==1 & apply_diff<=0),
    sms_in_d3_bkash_credit_cnt=sum(type==1 & is_trx==1 & is_bkash==1 & is_credit==1 & apply_diff<=3),
    sms_in_d7_bkash_credit_cnt=sum(type==1 & is_trx==1 & is_bkash==1 & is_credit==1 & apply_diff<=7),
    sms_in_d14_bkash_credit_cnt=sum(type==1 & is_trx==1 & is_bkash==1 & is_credit==1 & apply_diff<=14),
    sms_in_d30_bkash_credit_cnt=sum(type==1 & is_trx==1 & is_bkash==1 & is_credit==1 & apply_diff<=30), 
    
    sms_in_nagad_debit_cnt=sum(type==1 & is_trx==1 & is_nagad==1 & is_debit==1),
    sms_in_d0_nagad_debit_cnt=sum(type==1 & is_trx==1 & is_nagad==1 & is_debit==1 & apply_diff<=0),
    sms_in_d3_nagad_debit_cnt=sum(type==1 & is_trx==1 & is_nagad==1 & is_debit==1 & apply_diff<=3),
    sms_in_d7_nagad_debit_cnt=sum(type==1 & is_trx==1 & is_nagad==1 & is_debit==1 & apply_diff<=7),
    sms_in_d14_nagad_debit_cnt=sum(type==1 & is_trx==1 & is_nagad==1 & is_debit==1 & apply_diff<=14),
    sms_in_d30_nagad_debit_cnt=sum(type==1 & is_trx==1 & is_nagad==1 & is_debit==1 & apply_diff<=30), 
    
    sms_in_bkash_debit_cnt=sum(type==1 & is_trx==1 & is_bkash==1 & is_debit==1),
    sms_in_d0_bkash_debit_cnt=sum(type==1 & is_trx==1 & is_bkash==1 & is_debit==1 & apply_diff<=0),
    sms_in_d3_bkash_debit_cnt=sum(type==1 & is_trx==1 & is_bkash==1 & is_debit==1 & apply_diff<=3),
    sms_in_d7_bkash_debit_cnt=sum(type==1 & is_trx==1 & is_bkash==1 & is_debit==1 & apply_diff<=7),
    sms_in_d14_bkash_debit_cnt=sum(type==1 & is_trx==1 & is_bkash==1 & is_debit==1 & apply_diff<=14),
    sms_in_d30_bkash_debit_cnt=sum(type==1 & is_trx==1 & is_bkash==1 & is_debit==1 & apply_diff<=30), 
    
    sms_in_credit_amount=sum(amount[type==1 & is_trx==1 & (is_nagad==1 | is_nagad==1) & is_credit==1],na.rm=T),
    sms_in_d0_credit_amount=sum(amount[type==1 & is_trx==1 & (is_nagad==1 | is_nagad==1) & is_credit==1 & apply_diff<=0],na.rm=T),
    sms_in_d3_credit_amount=sum(amount[type==1 & is_trx==1 & (is_nagad==1 | is_nagad==1) & is_credit==1 & apply_diff<=3],na.rm=T),
    sms_in_d7_credit_amount=sum(amount[type==1 & is_trx==1 & (is_nagad==1 | is_nagad==1) & is_credit==1 & apply_diff<=7],na.rm=T),
    sms_in_d14_credit_amount=sum(amount[type==1 & is_trx==1 & (is_nagad==1 | is_nagad==1) & is_credit==1 & apply_diff<=14],na.rm=T),
    sms_in_d30_credit_amount=sum(amount[type==1 & is_trx==1 & (is_nagad==1 | is_nagad==1) & is_credit==1 & apply_diff<=30],na.rm=T),
    
    sms_in_min_credit_amount=if_else(min(amount[type==1 & is_trx==1 & is_credit==1 & (is_bkash==1 | is_nagad==1)],na.rm=T)%>%is.infinite()|min(amount[type==1 & is_trx==1 & is_credit==1 & (is_bkash==1 | is_nagad==1)],na.rm=T)%>%is.na(),-1,min(amount[type==1 & is_trx==1 & is_credit==1 & (is_bkash==1 | is_nagad==1)],na.rm=T)),
    sms_in_d0_min_credit_amount=if_else(min(amount[type==1 & is_trx==1 & is_credit==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=0],na.rm=T)%>%is.infinite()|min(amount[type==1 & is_trx==1 & is_credit==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=0],na.rm=T)%>%is.na(),-1,min(amount[type==1 & is_trx==1 & is_credit==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=0],na.rm=T)),
    sms_in_d3_min_credit_amount=if_else(min(amount[type==1 & is_trx==1 & is_credit==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=3],na.rm=T)%>%is.infinite()|min(amount[type==1 & is_trx==1 & is_credit==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=3],na.rm=T)%>%is.na(),-1,min(amount[type==1 & is_trx==1 & is_credit==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=3],na.rm=T)),
    sms_in_d7_min_credit_amount=if_else(min(amount[type==1 & is_trx==1 & is_credit==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=7],na.rm=T)%>%is.infinite()|min(amount[type==1 & is_trx==1 & is_credit==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=7],na.rm=T)%>%is.na(),-1,min(amount[type==1 & is_trx==1 & is_credit==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=7],na.rm=T)),
    sms_in_d14_min_credit_amount=if_else(min(amount[type==1 & is_trx==1 & is_credit==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=14],na.rm=T)%>%is.infinite()|min(amount[type==1 & is_trx==1 & is_credit==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=14],na.rm=T)%>%is.na(),-1,min(amount[type==1 & is_trx==1 & is_credit==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=14],na.rm=T)),
    sms_in_d30_min_credit_amount=if_else(min(amount[type==1 & is_trx==1 & is_credit==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=30],na.rm=T)%>%is.infinite()|min(amount[type==1 & is_trx==1 & is_credit==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=30],na.rm=T)%>%is.na(),-1,min(amount[type==1 & is_trx==1 & is_credit==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=30],na.rm=T)),
    
    sms_in_max_credit_amount=if_else(max(amount[type==1 & is_trx==1 & is_credit==1 & (is_bkash==1 | is_nagad==1)],na.rm=T)%>%is.infinite()|max(amount[type==1 & is_trx==1 & is_credit==1 & (is_bkash==1 | is_nagad==1)],na.rm=T)%>%is.na(),-1,max(amount[type==1 & is_trx==1 & is_credit==1 & (is_bkash==1 | is_nagad==1)],na.rm=T)),
    sms_in_d0_max_credit_amount=if_else(max(amount[type==1 & is_trx==1 & is_credit==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=0],na.rm=T)%>%is.infinite()|max(amount[type==1 & is_trx==1 & is_credit==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=0],na.rm=T)%>%is.na(),-1,max(amount[type==1 & is_trx==1 & is_credit==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=0],na.rm=T)),
    sms_in_d3_max_credit_amount=if_else(max(amount[type==1 & is_trx==1 & is_credit==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=3],na.rm=T)%>%is.infinite()|max(amount[type==1 & is_trx==1 & is_credit==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=3],na.rm=T)%>%is.na(),-1,max(amount[type==1 & is_trx==1 & is_credit==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=3],na.rm=T)),
    sms_in_d7_max_credit_amount=if_else(max(amount[type==1 & is_trx==1 & is_credit==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=7],na.rm=T)%>%is.infinite()|max(amount[type==1 & is_trx==1 & is_credit==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=7],na.rm=T)%>%is.na(),-1,max(amount[type==1 & is_trx==1 & is_credit==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=7],na.rm=T)),
    sms_in_d14_max_credit_amount=if_else(max(amount[type==1 & is_trx==1 & is_credit==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=14],na.rm=T)%>%is.infinite()|max(amount[type==1 & is_trx==1 & is_credit==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=14],na.rm=T)%>%is.na(),-1,max(amount[type==1 & is_trx==1 & is_credit==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=14],na.rm=T)),
    sms_in_d30_max_credit_amount=if_else(max(amount[type==1 & is_trx==1 & is_credit==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=30],na.rm=T)%>%is.infinite()|max(amount[type==1 & is_trx==1 & is_credit==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=30],na.rm=T)%>%is.na(),-1,max(amount[type==1 & is_trx==1 & is_credit==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=30],na.rm=T)),
    
    sms_in_min_debit_amount=if_else(min(amount[type==1 & is_trx==1 & is_debit==1 & (is_bkash==1 | is_nagad==1)],na.rm=T)%>%is.infinite()|min(amount[type==1 & is_trx==1 & is_debit==1 & (is_bkash==1 | is_nagad==1)],na.rm=T)%>%is.na(),-1,min(amount[type==1 & is_trx==1 & is_debit==1 & (is_bkash==1 | is_nagad==1)],na.rm=T)),
    sms_in_d0_min_debit_amount=if_else(min(amount[type==1 & is_trx==1 & is_debit==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=0],na.rm=T)%>%is.infinite()|min(amount[type==1 & is_trx==1 & is_debit==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=0],na.rm=T)%>%is.na(),-1,min(amount[type==1 & is_trx==1 & is_debit==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=0],na.rm=T)),
    sms_in_d3_min_debit_amount=if_else(min(amount[type==1 & is_trx==1 & is_debit==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=3],na.rm=T)%>%is.infinite()|min(amount[type==1 & is_trx==1 & is_debit==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=3],na.rm=T)%>%is.na(),-1,min(amount[type==1 & is_trx==1 & is_debit==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=3],na.rm=T)),
    sms_in_d7_min_debit_amount=if_else(min(amount[type==1 & is_trx==1 & is_debit==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=7],na.rm=T)%>%is.infinite()|min(amount[type==1 & is_trx==1 & is_debit==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=7],na.rm=T)%>%is.na(),-1,min(amount[type==1 & is_trx==1 & is_debit==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=7],na.rm=T)),
    sms_in_d14_min_debit_amount=if_else(min(amount[type==1 & is_trx==1 & is_debit==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=14],na.rm=T)%>%is.infinite()|min(amount[type==1 & is_trx==1 & is_debit==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=14],na.rm=T)%>%is.na(),-1,min(amount[type==1 & is_trx==1 & is_debit==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=14],na.rm=T)),
    sms_in_d30_min_debit_amount=if_else(min(amount[type==1 & is_trx==1 & is_debit==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=30],na.rm=T)%>%is.infinite()|min(amount[type==1 & is_trx==1 & is_debit==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=30],na.rm=T)%>%is.na(),-1,min(amount[type==1 & is_trx==1 & is_debit==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=30],na.rm=T)),
    
    sms_in_max_debit_amount=if_else(max(amount[type==1 & is_trx==1 & is_debit==1 & (is_bkash==1 | is_nagad==1)],na.rm=T)%>%is.infinite()|max(amount[type==1 & is_trx==1 & is_debit==1 & (is_bkash==1 | is_nagad==1)],na.rm=T)%>%is.na(),-1,max(amount[type==1 & is_trx==1 & is_debit==1 & (is_bkash==1 | is_nagad==1)],na.rm=T)),
    sms_in_d0_max_debit_amount=if_else(max(amount[type==1 & is_trx==1 & is_debit==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=0],na.rm=T)%>%is.infinite()|max(amount[type==1 & is_trx==1 & is_debit==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=0],na.rm=T)%>%is.na(),-1,max(amount[type==1 & is_trx==1 & is_debit==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=0],na.rm=T)),
    sms_in_d3_max_debit_amount=if_else(max(amount[type==1 & is_trx==1 & is_debit==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=3],na.rm=T)%>%is.infinite()|max(amount[type==1 & is_trx==1 & is_debit==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=3],na.rm=T)%>%is.na(),-1,max(amount[type==1 & is_trx==1 & is_debit==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=3],na.rm=T)),
    sms_in_d7_max_debit_amount=if_else(max(amount[type==1 & is_trx==1 & is_debit==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=7],na.rm=T)%>%is.infinite()|max(amount[type==1 & is_trx==1 & is_debit==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=7],na.rm=T)%>%is.na(),-1,max(amount[type==1 & is_trx==1 & is_debit==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=7],na.rm=T)),
    sms_in_d14_max_debit_amount=if_else(max(amount[type==1 & is_trx==1 & is_debit==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=14],na.rm=T)%>%is.infinite()|max(amount[type==1 & is_trx==1 & is_debit==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=14],na.rm=T)%>%is.na(),-1,max(amount[type==1 & is_trx==1 & is_debit==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=14],na.rm=T)),
    sms_in_d30_max_debit_amount=if_else(max(amount[type==1 & is_trx==1 & is_debit==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=30],na.rm=T)%>%is.infinite()|max(amount[type==1 & is_trx==1 & is_debit==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=30],na.rm=T)%>%is.na(),-1,max(amount[type==1 & is_trx==1 & is_debit==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=30],na.rm=T)),
    
    sms_in_min_bkash_credit_amount=if_else(min(amount[type==1 & is_trx==1 & is_credit==1 & is_bkash==1],na.rm=T)%>%is.infinite()|min(amount[type==1 & is_trx==1 & is_credit==1 & is_bkash==1],na.rm=T)%>%is.na(),-1,min(amount[type==1 & is_trx==1 & is_credit==1 & is_bkash==1],na.rm=T)),
    sms_in_d0_min_bkash_credit_amount=if_else(min(amount[type==1 & is_trx==1 & is_credit==1 & is_bkash==1 & apply_diff<=0],na.rm=T)%>%is.infinite()|min(amount[type==1 & is_trx==1 & is_credit==1 & is_bkash==1 & apply_diff<=0],na.rm=T)%>%is.na(),-1,min(amount[type==1 & is_trx==1 & is_credit==1 & is_bkash==1 & apply_diff<=0],na.rm=T)),
    sms_in_d3_min_bkash_credit_amount=if_else(min(amount[type==1 & is_trx==1 & is_credit==1 & is_bkash==1 & apply_diff<=3],na.rm=T)%>%is.infinite()|min(amount[type==1 & is_trx==1 & is_credit==1 & is_bkash==1 & apply_diff<=3],na.rm=T)%>%is.na(),-1,min(amount[type==1 & is_trx==1 & is_credit==1 & is_bkash==1 & apply_diff<=3],na.rm=T)),
    sms_in_d7_min_bkash_credit_amount=if_else(min(amount[type==1 & is_trx==1 & is_credit==1 & is_bkash==1 & apply_diff<=7],na.rm=T)%>%is.infinite()|min(amount[type==1 & is_trx==1 & is_credit==1 & is_bkash==1 & apply_diff<=7],na.rm=T)%>%is.na(),-1,min(amount[type==1 & is_trx==1 & is_credit==1 & is_bkash==1 & apply_diff<=7],na.rm=T)),
    sms_in_d14_min_bkash_credit_amount=if_else(min(amount[type==1 & is_trx==1 & is_credit==1 & is_bkash==1 & apply_diff<=14],na.rm=T)%>%is.infinite()|min(amount[type==1 & is_trx==1 & is_credit==1 & is_bkash==1 & apply_diff<=14],na.rm=T)%>%is.na(),-1,min(amount[type==1 & is_trx==1 & is_credit==1 & is_bkash==1 & apply_diff<=14],na.rm=T)),
    sms_in_d30_min_bkash_credit_amount=if_else(min(amount[type==1 & is_trx==1 & is_credit==1 & is_bkash==1 & apply_diff<=30],na.rm=T)%>%is.infinite()|min(amount[type==1 & is_trx==1 & is_credit==1 & is_bkash==1 & apply_diff<=30],na.rm=T)%>%is.na(),-1,min(amount[type==1 & is_trx==1 & is_credit==1 & is_bkash==1 & apply_diff<=30],na.rm=T)),
    
    sms_in_max_bkash_credit_amount=if_else(max(amount[type==1 & is_trx==1 & is_credit==1 & is_bkash==1],na.rm=T)%>%is.infinite()|max(amount[type==1 & is_trx==1 & is_credit==1 & is_bkash==1],na.rm=T)%>%is.na(),-1,max(amount[type==1 & is_trx==1 & is_credit==1 & is_bkash==1],na.rm=T)),
    sms_in_d0_max_bkash_credit_amount=if_else(max(amount[type==1 & is_trx==1 & is_credit==1 & is_bkash==1 & apply_diff<=0],na.rm=T)%>%is.infinite()|max(amount[type==1 & is_trx==1 & is_credit==1 & is_bkash==1 & apply_diff<=0],na.rm=T)%>%is.na(),-1,max(amount[type==1 & is_trx==1 & is_credit==1 & is_bkash==1 & apply_diff<=0],na.rm=T)),
    sms_in_d3_max_bkash_credit_amount=if_else(max(amount[type==1 & is_trx==1 & is_credit==1 & is_bkash==1 & apply_diff<=3],na.rm=T)%>%is.infinite()|max(amount[type==1 & is_trx==1 & is_credit==1 & is_bkash==1 & apply_diff<=3],na.rm=T)%>%is.na(),-1,max(amount[type==1 & is_trx==1 & is_credit==1 & is_bkash==1 & apply_diff<=3],na.rm=T)),
    sms_in_d7_max_bkash_credit_amount=if_else(max(amount[type==1 & is_trx==1 & is_credit==1 & is_bkash==1 & apply_diff<=7],na.rm=T)%>%is.infinite()|max(amount[type==1 & is_trx==1 & is_credit==1 & is_bkash==1 & apply_diff<=7],na.rm=T)%>%is.na(),-1,max(amount[type==1 & is_trx==1 & is_credit==1 & is_bkash==1 & apply_diff<=7],na.rm=T)),
    sms_in_d14_max_bkash_credit_amount=if_else(max(amount[type==1 & is_trx==1 & is_credit==1 & is_bkash==1 & apply_diff<=14],na.rm=T)%>%is.infinite()|max(amount[type==1 & is_trx==1 & is_credit==1 & is_bkash==1 & apply_diff<=14],na.rm=T)%>%is.na(),-1,max(amount[type==1 & is_trx==1 & is_credit==1 & is_bkash==1 & apply_diff<=14],na.rm=T)),
    sms_in_d30_max_bkash_credit_amount=if_else(max(amount[type==1 & is_trx==1 & is_credit==1 & is_bkash==1 & apply_diff<=30],na.rm=T)%>%is.infinite()|max(amount[type==1 & is_trx==1 & is_credit==1 & is_bkash==1 & apply_diff<=30],na.rm=T)%>%is.na(),-1,max(amount[type==1 & is_trx==1 & is_credit==1 & is_bkash==1 & apply_diff<=30],na.rm=T)),
    
    sms_in_min_bkash_debit_amount=if_else(min(amount[type==1 & is_trx==1 & is_debit==1 & is_bkash==1],na.rm=T)%>%is.infinite()|min(amount[type==1 & is_trx==1 & is_debit==1 & is_bkash==1],na.rm=T)%>%is.na(),-1,min(amount[type==1 & is_trx==1 & is_debit==1 & is_bkash==1],na.rm=T)),
    sms_in_d0_min_bkash_debit_amount=if_else(min(amount[type==1 & is_trx==1 & is_debit==1 & is_bkash==1 & apply_diff<=0],na.rm=T)%>%is.infinite()|min(amount[type==1 & is_trx==1 & is_debit==1 & is_bkash==1 & apply_diff<=0],na.rm=T)%>%is.na(),-1,min(amount[type==1 & is_trx==1 & is_debit==1 & is_bkash==1 & apply_diff<=0],na.rm=T)),
    sms_in_d3_min_bkash_debit_amount=if_else(min(amount[type==1 & is_trx==1 & is_debit==1 & is_bkash==1 & apply_diff<=3],na.rm=T)%>%is.infinite()|min(amount[type==1 & is_trx==1 & is_debit==1 & is_bkash==1 & apply_diff<=3],na.rm=T)%>%is.na(),-1,min(amount[type==1 & is_trx==1 & is_debit==1 & is_bkash==1 & apply_diff<=3],na.rm=T)),
    sms_in_d7_min_bkash_debit_amount=if_else(min(amount[type==1 & is_trx==1 & is_debit==1 & is_bkash==1 & apply_diff<=7],na.rm=T)%>%is.infinite()|min(amount[type==1 & is_trx==1 & is_debit==1 & is_bkash==1 & apply_diff<=7],na.rm=T)%>%is.na(),-1,min(amount[type==1 & is_trx==1 & is_debit==1 & is_bkash==1 & apply_diff<=7],na.rm=T)),
    sms_in_d14_min_bkash_debit_amount=if_else(min(amount[type==1 & is_trx==1 & is_debit==1 & is_bkash==1 & apply_diff<=14],na.rm=T)%>%is.infinite()|min(amount[type==1 & is_trx==1 & is_debit==1 & is_bkash==1 & apply_diff<=14],na.rm=T)%>%is.na(),-1,min(amount[type==1 & is_trx==1 & is_debit==1 & is_bkash==1 & apply_diff<=14],na.rm=T)),
    sms_in_d30_min_bkash_debit_amount=if_else(min(amount[type==1 & is_trx==1 & is_debit==1 & is_bkash==1 & apply_diff<=30],na.rm=T)%>%is.infinite()|min(amount[type==1 & is_trx==1 & is_debit==1 & is_bkash==1 & apply_diff<=30],na.rm=T)%>%is.na(),-1,min(amount[type==1 & is_trx==1 & is_debit==1 & is_bkash==1 & apply_diff<=30],na.rm=T)),
    
    sms_in_max_bkash_debit_amount=if_else(max(amount[type==1 & is_trx==1 & is_debit==1 & is_bkash==1],na.rm=T)%>%is.infinite()|max(amount[type==1 & is_trx==1 & is_debit==1 & is_bkash==1],na.rm=T)%>%is.na(),-1,max(amount[type==1 & is_trx==1 & is_debit==1 & is_bkash==1],na.rm=T)),
    sms_in_d0_max_bkash_debit_amount=if_else(max(amount[type==1 & is_trx==1 & is_debit==1 & is_bkash==1 & apply_diff<=0],na.rm=T)%>%is.infinite()|max(amount[type==1 & is_trx==1 & is_debit==1 & is_bkash==1 & apply_diff<=0],na.rm=T)%>%is.na(),-1,max(amount[type==1 & is_trx==1 & is_debit==1 & is_bkash==1 & apply_diff<=0],na.rm=T)),
    sms_in_d3_max_bkash_debit_amount=if_else(max(amount[type==1 & is_trx==1 & is_debit==1 & is_bkash==1 & apply_diff<=3],na.rm=T)%>%is.infinite()|max(amount[type==1 & is_trx==1 & is_debit==1 & is_bkash==1 & apply_diff<=3],na.rm=T)%>%is.na(),-1,max(amount[type==1 & is_trx==1 & is_debit==1 & is_bkash==1 & apply_diff<=3],na.rm=T)),
    sms_in_d7_max_bkash_debit_amount=if_else(max(amount[type==1 & is_trx==1 & is_debit==1 & is_bkash==1 & apply_diff<=7],na.rm=T)%>%is.infinite()|max(amount[type==1 & is_trx==1 & is_debit==1 & is_bkash==1 & apply_diff<=7],na.rm=T)%>%is.na(),-1,max(amount[type==1 & is_trx==1 & is_debit==1 & is_bkash==1 & apply_diff<=7],na.rm=T)),
    sms_in_d14_max_bkash_debit_amount=if_else(max(amount[type==1 & is_trx==1 & is_debit==1 & is_bkash==1 & apply_diff<=14],na.rm=T)%>%is.infinite()|max(amount[type==1 & is_trx==1 & is_debit==1 & is_bkash==1 & apply_diff<=14],na.rm=T)%>%is.na(),-1,max(amount[type==1 & is_trx==1 & is_debit==1 & is_bkash==1 & apply_diff<=14],na.rm=T)),
    sms_in_d30_max_bkash_debit_amount=if_else(max(amount[type==1 & is_trx==1 & is_debit==1 & is_bkash==1 & apply_diff<=30],na.rm=T)%>%is.infinite()|max(amount[type==1 & is_trx==1 & is_debit==1 & is_bkash==1 & apply_diff<=30],na.rm=T)%>%is.na(),-1,max(amount[type==1 & is_trx==1 & is_debit==1 & is_bkash==1 & apply_diff<=30],na.rm=T)),
    
    sms_in_min_nagad_credit_amount=if_else(min(amount[type==1 & is_trx==1 & is_credit==1 & is_nagad==1],na.rm=T)%>%is.infinite()|min(amount[type==1 & is_trx==1 & is_credit==1 & is_nagad==1],na.rm=T)%>%is.na(),-1,min(amount[type==1 & is_trx==1 & is_credit==1 & is_nagad==1],na.rm=T)),
    sms_in_d0_min_nagad_credit_amount=if_else(min(amount[type==1 & is_trx==1 & is_credit==1 & is_nagad==1 & apply_diff<=0],na.rm=T)%>%is.infinite()|min(amount[type==1 & is_trx==1 & is_credit==1 & is_nagad==1 & apply_diff<=0],na.rm=T)%>%is.na(),-1,min(amount[type==1 & is_trx==1 & is_credit==1 & is_nagad==1 & apply_diff<=0],na.rm=T)),
    sms_in_d3_min_nagad_credit_amount=if_else(min(amount[type==1 & is_trx==1 & is_credit==1 & is_nagad==1 & apply_diff<=3],na.rm=T)%>%is.infinite()|min(amount[type==1 & is_trx==1 & is_credit==1 & is_nagad==1 & apply_diff<=3],na.rm=T)%>%is.na(),-1,min(amount[type==1 & is_trx==1 & is_credit==1 & is_nagad==1 & apply_diff<=3],na.rm=T)),
    sms_in_d7_min_nagad_credit_amount=if_else(min(amount[type==1 & is_trx==1 & is_credit==1 & is_nagad==1 & apply_diff<=7],na.rm=T)%>%is.infinite()|min(amount[type==1 & is_trx==1 & is_credit==1 & is_nagad==1 & apply_diff<=7],na.rm=T)%>%is.na(),-1,min(amount[type==1 & is_trx==1 & is_credit==1 & is_nagad==1 & apply_diff<=7],na.rm=T)),
    sms_in_d14_min_nagad_credit_amount=if_else(min(amount[type==1 & is_trx==1 & is_credit==1 & is_nagad==1 & apply_diff<=14],na.rm=T)%>%is.infinite()|min(amount[type==1 & is_trx==1 & is_credit==1 & is_nagad==1 & apply_diff<=14],na.rm=T)%>%is.na(),-1,min(amount[type==1 & is_trx==1 & is_credit==1 & is_nagad==1 & apply_diff<=14],na.rm=T)),
    sms_in_d30_min_nagad_credit_amount=if_else(min(amount[type==1 & is_trx==1 & is_credit==1 & is_nagad==1 & apply_diff<=30],na.rm=T)%>%is.infinite()|min(amount[type==1 & is_trx==1 & is_credit==1 & is_nagad==1 & apply_diff<=30],na.rm=T)%>%is.na(),-1,min(amount[type==1 & is_trx==1 & is_credit==1 & is_nagad==1 & apply_diff<=30],na.rm=T)),
    
    sms_in_max_nagad_credit_amount=if_else(max(amount[type==1 & is_trx==1 & is_credit==1 & is_nagad==1],na.rm=T)%>%is.infinite()|max(amount[type==1 & is_trx==1 & is_credit==1 & is_nagad==1],na.rm=T)%>%is.na(),-1,max(amount[type==1 & is_trx==1 & is_credit==1 & is_nagad==1],na.rm=T)),
    sms_in_d0_max_nagad_credit_amount=if_else(max(amount[type==1 & is_trx==1 & is_credit==1 & is_nagad==1 & apply_diff<=0],na.rm=T)%>%is.infinite()|max(amount[type==1 & is_trx==1 & is_credit==1 & is_nagad==1 & apply_diff<=0],na.rm=T)%>%is.na(),-1,max(amount[type==1 & is_trx==1 & is_credit==1 & is_nagad==1 & apply_diff<=0],na.rm=T)),
    sms_in_d3_max_nagad_credit_amount=if_else(max(amount[type==1 & is_trx==1 & is_credit==1 & is_nagad==1 & apply_diff<=3],na.rm=T)%>%is.infinite()|max(amount[type==1 & is_trx==1 & is_credit==1 & is_nagad==1 & apply_diff<=3],na.rm=T)%>%is.na(),-1,max(amount[type==1 & is_trx==1 & is_credit==1 & is_nagad==1 & apply_diff<=3],na.rm=T)),
    sms_in_d7_max_nagad_credit_amount=if_else(max(amount[type==1 & is_trx==1 & is_credit==1 & is_nagad==1 & apply_diff<=7],na.rm=T)%>%is.infinite()|max(amount[type==1 & is_trx==1 & is_credit==1 & is_nagad==1 & apply_diff<=7],na.rm=T)%>%is.na(),-1,max(amount[type==1 & is_trx==1 & is_credit==1 & is_nagad==1 & apply_diff<=7],na.rm=T)),
    sms_in_d14_max_nagad_credit_amount=if_else(max(amount[type==1 & is_trx==1 & is_credit==1 & is_nagad==1 & apply_diff<=14],na.rm=T)%>%is.infinite()|max(amount[type==1 & is_trx==1 & is_credit==1 & is_nagad==1 & apply_diff<=14],na.rm=T)%>%is.na(),-1,max(amount[type==1 & is_trx==1 & is_credit==1 & is_nagad==1 & apply_diff<=14],na.rm=T)),
    sms_in_d30_max_nagad_credit_amount=if_else(max(amount[type==1 & is_trx==1 & is_credit==1 & is_nagad==1 & apply_diff<=30],na.rm=T)%>%is.infinite()|max(amount[type==1 & is_trx==1 & is_credit==1 & is_nagad==1 & apply_diff<=30],na.rm=T)%>%is.na(),-1,max(amount[type==1 & is_trx==1 & is_credit==1 & is_nagad==1 & apply_diff<=30],na.rm=T)),
    
    sms_in_min_nagad_debit_amount=if_else(min(amount[type==1 & is_trx==1 & is_debit==1 & is_nagad==1],na.rm=T)%>%is.infinite()|min(amount[type==1 & is_trx==1 & is_debit==1 & is_nagad==1],na.rm=T)%>%is.na(),-1,min(amount[type==1 & is_trx==1 & is_debit==1 & is_nagad==1],na.rm=T)),
    sms_in_d0_min_nagad_debit_amount=if_else(min(amount[type==1 & is_trx==1 & is_debit==1 & is_nagad==1 & apply_diff<=0],na.rm=T)%>%is.infinite()|min(amount[type==1 & is_trx==1 & is_debit==1 & is_nagad==1 & apply_diff<=0],na.rm=T)%>%is.na(),-1,min(amount[type==1 & is_trx==1 & is_debit==1 & is_nagad==1 & apply_diff<=0],na.rm=T)),
    sms_in_d3_min_nagad_debit_amount=if_else(min(amount[type==1 & is_trx==1 & is_debit==1 & is_nagad==1 & apply_diff<=3],na.rm=T)%>%is.infinite()|min(amount[type==1 & is_trx==1 & is_debit==1 & is_nagad==1 & apply_diff<=3],na.rm=T)%>%is.na(),-1,min(amount[type==1 & is_trx==1 & is_debit==1 & is_nagad==1 & apply_diff<=3],na.rm=T)),
    sms_in_d7_min_nagad_debit_amount=if_else(min(amount[type==1 & is_trx==1 & is_debit==1 & is_nagad==1 & apply_diff<=7],na.rm=T)%>%is.infinite()|min(amount[type==1 & is_trx==1 & is_debit==1 & is_nagad==1 & apply_diff<=7],na.rm=T)%>%is.na(),-1,min(amount[type==1 & is_trx==1 & is_debit==1 & is_nagad==1 & apply_diff<=7],na.rm=T)),
    sms_in_d14_min_nagad_debit_amount=if_else(min(amount[type==1 & is_trx==1 & is_debit==1 & is_nagad==1 & apply_diff<=14],na.rm=T)%>%is.infinite()|min(amount[type==1 & is_trx==1 & is_debit==1 & is_nagad==1 & apply_diff<=14],na.rm=T)%>%is.na(),-1,min(amount[type==1 & is_trx==1 & is_debit==1 & is_nagad==1 & apply_diff<=14],na.rm=T)),
    sms_in_d30_min_nagad_debit_amount=if_else(min(amount[type==1 & is_trx==1 & is_debit==1 & is_nagad==1 & apply_diff<=30],na.rm=T)%>%is.infinite()|min(amount[type==1 & is_trx==1 & is_debit==1 & is_nagad==1 & apply_diff<=30],na.rm=T)%>%is.na(),-1,min(amount[type==1 & is_trx==1 & is_debit==1 & is_nagad==1 & apply_diff<=30],na.rm=T)),
    
    sms_in_max_nagad_debit_amount=if_else(max(amount[type==1 & is_trx==1 & is_debit==1 & is_nagad==1],na.rm=T)%>%is.infinite()|max(amount[type==1 & is_trx==1 & is_debit==1 & is_nagad==1],na.rm=T)%>%is.na(),-1,max(amount[type==1 & is_trx==1 & is_debit==1 & is_nagad==1],na.rm=T)),
    sms_in_d0_max_nagad_debit_amount=if_else(max(amount[type==1 & is_trx==1 & is_debit==1 & is_nagad==1 & apply_diff<=0],na.rm=T)%>%is.infinite()|max(amount[type==1 & is_trx==1 & is_debit==1 & is_nagad==1 & apply_diff<=0],na.rm=T)%>%is.na(),-1,max(amount[type==1 & is_trx==1 & is_debit==1 & is_nagad==1 & apply_diff<=0],na.rm=T)),
    sms_in_d3_max_nagad_debit_amount=if_else(max(amount[type==1 & is_trx==1 & is_debit==1 & is_nagad==1 & apply_diff<=3],na.rm=T)%>%is.infinite()|max(amount[type==1 & is_trx==1 & is_debit==1 & is_nagad==1 & apply_diff<=3],na.rm=T)%>%is.na(),-1,max(amount[type==1 & is_trx==1 & is_debit==1 & is_nagad==1 & apply_diff<=3],na.rm=T)),
    sms_in_d7_max_nagad_debit_amount=if_else(max(amount[type==1 & is_trx==1 & is_debit==1 & is_nagad==1 & apply_diff<=7],na.rm=T)%>%is.infinite()|max(amount[type==1 & is_trx==1 & is_debit==1 & is_nagad==1 & apply_diff<=7],na.rm=T)%>%is.na(),-1,max(amount[type==1 & is_trx==1 & is_debit==1 & is_nagad==1 & apply_diff<=7],na.rm=T)),
    sms_in_d14_max_nagad_debit_amount=if_else(max(amount[type==1 & is_trx==1 & is_debit==1 & is_nagad==1 & apply_diff<=14],na.rm=T)%>%is.infinite()|max(amount[type==1 & is_trx==1 & is_debit==1 & is_nagad==1 & apply_diff<=14],na.rm=T)%>%is.na(),-1,max(amount[type==1 & is_trx==1 & is_debit==1 & is_nagad==1 & apply_diff<=14],na.rm=T)),
    sms_in_d30_max_nagad_debit_amount=if_else(max(amount[type==1 & is_trx==1 & is_debit==1 & is_nagad==1 & apply_diff<=30],na.rm=T)%>%is.infinite()|max(amount[type==1 & is_trx==1 & is_debit==1 & is_nagad==1 & apply_diff<=30],na.rm=T)%>%is.na(),-1,max(amount[type==1 & is_trx==1 & is_debit==1 & is_nagad==1 & apply_diff<=30],na.rm=T)),
    
    sms_in_debit_amount=sum(amount[type==1 & is_trx==1 & (is_nagad==1 | is_nagad==1) & is_debit==1],na.rm=T),
    sms_in_d0_debit_amount=sum(amount[type==1 & is_trx==1 & (is_nagad==1 | is_nagad==1) & is_debit==1 & apply_diff<=0],na.rm=T),
    sms_in_d3_debit_amount=sum(amount[type==1 & is_trx==1 & (is_nagad==1 | is_nagad==1) & is_debit==1 & apply_diff<=3],na.rm=T),
    sms_in_d7_debit_amount=sum(amount[type==1 & is_trx==1 & (is_nagad==1 | is_nagad==1) & is_debit==1 & apply_diff<=7],na.rm=T),
    sms_in_d14_debit_amount=sum(amount[type==1 & is_trx==1 & (is_nagad==1 | is_nagad==1) & is_debit==1 & apply_diff<=14],na.rm=T),
    sms_in_d30_debit_amount=sum(amount[type==1 & is_trx==1 & (is_nagad==1 | is_nagad==1) & is_debit==1 & apply_diff<=30],na.rm=T),
    
    sms_in_nagad_credit_amount=sum(amount[type==1 & is_trx==1 & is_nagad==1 & is_credit==1],na.rm=T),
    sms_in_d0_nagad_credit_amount=sum(amount[type==1 & is_trx==1 & is_nagad==1 & is_credit==1 & apply_diff<=0],na.rm=T),
    sms_in_d3_nagad_credit_amount=sum(amount[type==1 & is_trx==1 & is_nagad==1 & is_credit==1 & apply_diff<=3],na.rm=T),
    sms_in_d7_nagad_credit_amount=sum(amount[type==1 & is_trx==1 & is_nagad==1 & is_credit==1 & apply_diff<=7],na.rm=T),
    sms_in_d14_nagad_credit_amount=sum(amount[type==1 & is_trx==1 & is_nagad==1 & is_credit==1 & apply_diff<=14],na.rm=T),
    sms_in_d30_nagad_credit_amount=sum(amount[type==1 & is_trx==1 & is_nagad==1 & is_credit==1 & apply_diff<=30],na.rm=T),
    
    sms_in_nagad_debit_amount=sum(amount[type==1 & is_trx==1 & is_nagad==1 & is_debit==1],na.rm=T),
    sms_in_d0_nagad_debit_amount=sum(amount[type==1 & is_trx==1 & is_nagad==1 & is_debit==1 & apply_diff<=0],na.rm=T),
    sms_in_d3_nagad_debit_amount=sum(amount[type==1 & is_trx==1 & is_nagad==1 & is_debit==1 & apply_diff<=3],na.rm=T),
    sms_in_d7_nagad_debit_amount=sum(amount[type==1 & is_trx==1 & is_nagad==1 & is_debit==1 & apply_diff<=7],na.rm=T),
    sms_in_d14_nagad_debit_amount=sum(amount[type==1 & is_trx==1 & is_nagad==1 & is_debit==1 & apply_diff<=14],na.rm=T),
    sms_in_d30_nagad_debit_amount=sum(amount[type==1 & is_trx==1 & is_nagad==1 & is_debit==1 & apply_diff<=30],na.rm=T),
    
    sms_in_bkash_credit_amount=sum(amount[type==1 & is_trx==1 & is_bkash==1 & is_credit==1],na.rm=T),
    sms_in_d0_bkash_credit_amount=sum(amount[type==1 & is_trx==1 & is_bkash==1 & is_credit==1 & apply_diff<=0],na.rm=T),
    sms_in_d3_bkash_credit_amount=sum(amount[type==1 & is_trx==1 & is_bkash==1 & is_credit==1 & apply_diff<=3],na.rm=T),
    sms_in_d7_bkash_credit_amount=sum(amount[type==1 & is_trx==1 & is_bkash==1 & is_credit==1 & apply_diff<=7],na.rm=T),
    sms_in_d14_bkash_credit_amount=sum(amount[type==1 & is_trx==1 & is_bkash==1 & is_credit==1 & apply_diff<=14],na.rm=T),
    sms_in_d30_bkash_credit_amount=sum(amount[type==1 & is_trx==1 & is_bkash==1 & is_credit==1 & apply_diff<=30],na.rm=T),
    
    sms_in_bkash_debit_amount=sum(amount[type==1 & is_trx==1 & is_bkash==1 & is_debit==1],na.rm=T),
    sms_in_d0_bkash_debit_amount=sum(amount[type==1 & is_trx==1 & is_bkash==1 & is_debit==1 & apply_diff<=0],na.rm=T),
    sms_in_d3_bkash_debit_amount=sum(amount[type==1 & is_trx==1 & is_bkash==1 & is_debit==1 & apply_diff<=3],na.rm=T),
    sms_in_d7_bkash_debit_amount=sum(amount[type==1 & is_trx==1 & is_bkash==1 & is_debit==1 & apply_diff<=7],na.rm=T),
    sms_in_d14_bkash_debit_amount=sum(amount[type==1 & is_trx==1 & is_bkash==1 & is_debit==1 & apply_diff<=14],na.rm=T),
    sms_in_d30_bkash_debit_amount=sum(amount[type==1 & is_trx==1 & is_bkash==1 & is_debit==1 & apply_diff<=30],na.rm=T),
    
    sms_in_max_balance=if_else(max(balance[type==1 & is_trx==1 & (is_bkash==1 | is_nagad==1)],na.rm=T)%>%is.infinite()|max(balance[type==1 & is_trx==1 & (is_bkash==1 | is_nagad==1)],na.rm=T)%>%is.na(),-1,max(balance[type==1 & is_trx==1 & (is_bkash==1 | is_nagad==1)],na.rm=T)),
    sms_in_d0_max_balance=if_else(max(balance[type==1 & is_trx==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=0],na.rm=T)%>%is.infinite()|max(balance[type==1 & is_trx==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=0],na.rm=T)%>%is.na(),-1,max(balance[type==1 & is_trx==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=0],na.rm=T)),
    sms_in_d3_max_balance=if_else(max(balance[type==1 & is_trx==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=3],na.rm=T)%>%is.infinite()|max(balance[type==1 & is_trx==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=3],na.rm=T)%>%is.na(),-1,max(balance[type==1 & is_trx==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=3],na.rm=T)),
    sms_in_d7_max_balance=if_else(max(balance[type==1 & is_trx==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=7],na.rm=T)%>%is.infinite()|max(balance[type==1 & is_trx==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=7],na.rm=T)%>%is.na(),-1,max(balance[type==1 & is_trx==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=7],na.rm=T)),
    sms_in_d14_max_balance=if_else(max(balance[type==1 & is_trx==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=14],na.rm=T)%>%is.infinite()|max(balance[type==1 & is_trx==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=14],na.rm=T)%>%is.na(),-1,max(balance[type==1 & is_trx==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=14],na.rm=T)),
    sms_in_d30_max_balance=if_else(max(balance[type==1 & is_trx==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=30],na.rm=T)%>%is.infinite()|max(balance[type==1 & is_trx==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=30],na.rm=T)%>%is.na(),-1,max(balance[type==1 & is_trx==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=30],na.rm=T)),

    sms_in_min_balance=if_else(min(balance[type==1 & is_trx==1 & (is_bkash==1 | is_nagad==1)],na.rm=T)%>%is.infinite()|min(balance[type==1 & is_trx==1 & (is_bkash==1 | is_nagad==1)],na.rm=T)%>%is.na(),-1,min(balance[type==1 & is_trx==1 & (is_bkash==1 | is_nagad==1)],na.rm=T)),
    sms_in_d0_min_balance=if_else(min(balance[type==1 & is_trx==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=0],na.rm=T)%>%is.infinite()|min(balance[type==1 & is_trx==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=0],na.rm=T)%>%is.na(),-1,min(balance[type==1 & is_trx==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=0],na.rm=T)),
    sms_in_d3_min_balance=if_else(min(balance[type==1 & is_trx==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=3],na.rm=T)%>%is.infinite()|min(balance[type==1 & is_trx==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=3],na.rm=T)%>%is.na(),-1,min(balance[type==1 & is_trx==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=3],na.rm=T)),
    sms_in_d7_min_balance=if_else(min(balance[type==1 & is_trx==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=7],na.rm=T)%>%is.infinite()|min(balance[type==1 & is_trx==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=7],na.rm=T)%>%is.na(),-1,min(balance[type==1 & is_trx==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=7],na.rm=T)),
    sms_in_d14_min_balance=if_else(min(balance[type==1 & is_trx==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=14],na.rm=T)%>%is.infinite()|min(balance[type==1 & is_trx==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=14],na.rm=T)%>%is.na(),-1,min(balance[type==1 & is_trx==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=14],na.rm=T)),
    sms_in_d30_min_balance=if_else(min(balance[type==1 & is_trx==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=30],na.rm=T)%>%is.infinite()|min(balance[type==1 & is_trx==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=30],na.rm=T)%>%is.na(),-1,min(balance[type==1 & is_trx==1 & (is_bkash==1 | is_nagad==1) & apply_diff<=30],na.rm=T)),
    
        
  )->sms_credit_var


sms_var_frame<<-rbind(sms_var_frame,sms_var)
sms_credit_var_frame<<-rbind(sms_credit_var_frame,sms_credit_var)
print(paste(Sys.time(),' 特征计算结束',sep=""))

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

dbWriteTable(connec_bu,'sms_common_var',sms_var_frame,overwrite=T,row.names=F)
dbWriteTable(connec_bu,'sms_credit_var',sms_credit_var_frame,overwrite=T,row.names=F)



dbWriteTable(connec_bu,'device_multi_var',device_multi_var_frame,overwrite=T,row.names=F)
dbWriteTable(connec_bu,'ip_multi_var',ip_multi_var_frame,overwrite=T,row.names=F)
dbWriteTable(connec_bu,'account_multi_var',account_multi_var_frame,overwrite=T,row.names=F)

    
