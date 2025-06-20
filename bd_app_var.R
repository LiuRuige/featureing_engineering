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

app_cate<-read.table(file = 'app_cate.txt',he=T,sep='\t')

due_order<-dbGetQuery(connec_bu,"select order_id,user_phone from bengal_test.bd_dwd_order where status>=8")
applist_cate_var_frame<-data.frame()

# due_order[80000:81000,]->sample_order
# i=1
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

sample_app<-dbGetQuery(connec_bu,str_squish(paste("select order_id,collect_app_name,in_time,up_time,collect_app_package,app_type,borrow_time 
                                           from bengal_test.bd_user_app_info where order_id in('",paste(sample_order$order_id,collapse="','",sep=""),
                                                  "')",sep="")))

dbDisconnect(connec_bu)
print(paste(Sys.time(),' 订单查询结束'))

##定义app类型 ##定义时间切片 #定义关键词类 # 定义

sample_app%>%
  as_tibble()%>%
  mutate(in_diff=difftime(as.Date(borrow_time),as.Date(in_time),units='days')%>%as.numeric(),
         up_diff=difftime(as.Date(borrow_time),as.Date(in_time),units='days')%>%as.numeric(),
         app_name=str_to_lower(collect_app_name),
         package=str_to_lower(collect_app_package),
         pre_in=if_else(as.Date(in_time)<'2019-01-01',1,0),
         
  )%>%
  left_join(app_cate%>%select(package_name,cate)%>%mutate(package_name=str_to_lower(package_name)),by=c('package'='package_name'))%>%
  select(order_id,app_name,package,cate,in_diff,up_diff,app_type,pre_in)->sample_app_select

# sample_app_select%>%
#   filter(app_type==0)->x

#### 1.1.1.1 风险app truecaller 
### 博彩 bet|gambl|casino
## 贷款 loan|cash|lendora|
## 常用app 

sample_app_select%>%
  mutate(
    is_loan=if_else(str_detect(app_name,'loan|ধার|cash|lendora')|cate=='loan'|str_detect(app_name,'loan|ধার|cash|lendora'),1,0),
    is_bet=if_else(str_detect(app_name,'bet|gambl|casino|slot')|str_detect(package,'bet|gambl|casino|slot'),1,0),
    is_rider=if_else(str_detect(package,'rider|driver'),1,0),
    is_agent=if_else(str_detect(package,'agent|merchant|business'),1,0),
    
  )->sample_app_select_derive


sample_app_select_derive%>%
  as_tibble()%>%
  group_by(order_id)%>%
  summarise(
    applist_cnt=n(),
    applist_sys_cnt=sum(app_type==1),
    applist_pre_cnt=sum(app_type==0 & pre_in==1),
    applist_in_cnt=sum(app_type==0 & pre_in==0),
    applist_d0_in_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & in_diff<=0]),
    applist_d3_in_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & in_diff<=3]),
    applist_d7_in_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & in_diff<=7]),
    applist_d14_in_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & in_diff<=14]),
    applist_d30_in_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & in_diff<=30]),
    applist_d60_in_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & in_diff<=60]),
    applist_d90_in_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & in_diff<=90]),
    
    applist_in_date_max=if_else(max(in_diff[app_type==0 & pre_in==0],na.rm=T)%>%is.infinite(),-1,max(in_diff[app_type==0 & pre_in==0],na.rm=T)),
    
    applist_d0_up_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & up_diff<=0]),
    applist_d3_up_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & up_diff<=3]),
    applist_d7_up_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & up_diff<=7]),
    applist_d14_up_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & up_diff<=14]),
    applist_d30_up_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & up_diff<=30]),
    applist_d60_up_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & up_diff<=60]),
    applist_d90_up_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & up_diff<=90]),
    
    applist_up_date_max=if_else(max(up_diff[app_type==0 & pre_in==0],na.rm=T)%>%is.infinite(),-1,max(up_diff[app_type==0 & pre_in==0],na.rm=T)),
    
    applist_in_regloan_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & is_loan==1]),
    applist_d0_in_regloan_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & is_loan==1 & in_diff<=0]),    
    applist_d3_in_regloan_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & is_loan==1 & in_diff<=3]),    
    applist_d7_in_regloan_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & is_loan==1 & in_diff<=7]),    
    applist_d14_in_regloan_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & is_loan==1 & in_diff<=14]),    
    applist_d30_in_regloan_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & is_loan==1 & in_diff<=30]),
    applist_in_regloan_date_max=if_else(max(in_diff[app_type==0 & is_loan==1 & pre_in==0],na.rm=T)%>%is.infinite(),-1,max(in_diff[app_type==0  & is_loan==1 & pre_in==0],na.rm=T)),
    
    applist_in_regbet_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & is_bet==1]),
    applist_d0_in_regbet_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & is_bet==1 & in_diff<=0]),    
    applist_d3_in_regbet_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & is_bet==1 & in_diff<=3]),    
    applist_d7_in_regbet_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & is_bet==1 & in_diff<=7]),    
    applist_d14_in_regbet_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & is_bet==1 & in_diff<=14]),    
    applist_d30_in_regbet_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & is_bet==1 & in_diff<=30]),
    applist_in_regbet_date_max=if_else(max(in_diff[app_type==0 & is_bet==1 & pre_in==0],na.rm=T)%>%is.infinite(),-1,max(in_diff[app_type==0  & is_bet==1 & pre_in==0],na.rm=T)),
    
    applist_in_regrider_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & is_rider==1]),
    applist_d0_in_regrider_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & is_rider==1 & in_diff<=0]),    
    applist_d3_in_regrider_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & is_rider==1 & in_diff<=3]),    
    applist_d7_in_regrider_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & is_rider==1 & in_diff<=7]),    
    applist_d14_in_regrider_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & is_rider==1 & in_diff<=14]),    
    applist_d30_in_regrider_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & is_rider==1 & in_diff<=30]),
    applist_in_regrider_date_max=if_else(max(in_diff[app_type==0 & is_rider==1 & pre_in==0],na.rm=T)%>%is.infinite(),-1,max(in_diff[app_type==0  & is_rider==1 & pre_in==0],na.rm=T)),
    
    applist_in_regagent_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & is_agent==1]),
    applist_d0_in_regagent_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & is_agent==1 & in_diff<=0]),    
    applist_d3_in_regagent_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & is_agent==1 & in_diff<=3]),    
    applist_d7_in_regagent_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & is_agent==1 & in_diff<=7]),    
    applist_d14_in_regagent_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & is_agent==1 & in_diff<=14]),    
    applist_d30_in_regagent_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & is_agent==1 & in_diff<=30]),
    applist_in_regagent_date_max=if_else(max(in_diff[app_type==0 & is_agent==1 & pre_in==0],na.rm=T)%>%is.infinite(),-1,max(in_diff[app_type==0  & is_agent==1 & pre_in==0],na.rm=T)),
    
    applist_in_catevpn_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & (cate=='vpn'|str_detect(app_name,'vpn'))]),
    applist_d0_in_catevpn_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & (cate=='vpn'|str_detect(app_name,'vpn')) & in_diff<=0]),    
    applist_d3_in_catevpn_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & (cate=='vpn'|str_detect(app_name,'vpn')) & in_diff<=3]),    
    applist_d7_in_catevpn_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & (cate=='vpn'|str_detect(app_name,'vpn')) & in_diff<=7]),    
    applist_d14_in_catevpn_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & (cate=='vpn'|str_detect(app_name,'vpn')) & in_diff<=14]),    
    applist_d30_in_catevpn_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & (cate=='vpn'|str_detect(app_name,'vpn')) & in_diff<=30]),
    applist_in_catevpn_date_max=if_else(max(in_diff[app_type==0 & (cate=='vpn'|str_detect(app_name,'vpn')) & pre_in==0],na.rm=T)%>%is.infinite(),-1,max(in_diff[app_type==0  & (cate=='vpn'|str_detect(app_name,'vpn')) & pre_in==0],na.rm=T)),
 
    applist_in_catebank_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='bank']),
    applist_d0_in_catebank_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='bank' & in_diff<=0]),    
    applist_d3_in_catebank_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='bank' & in_diff<=3]),    
    applist_d7_in_catebank_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='bank' & in_diff<=7]),    
    applist_d14_in_catebank_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='bank' & in_diff<=14]),    
    applist_d30_in_catebank_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='bank' & in_diff<=30]),
    applist_in_catebank_date_max=if_else(max(in_diff[app_type==0 & cate=='bank' & pre_in==0],na.rm=T)%>%is.infinite(),-1,max(in_diff[app_type==0  & cate=='bank' & pre_in==0],na.rm=T)),
    
    applist_in_catecoin_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='coin']),
    applist_d0_in_catecoin_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='coin' & in_diff<=0]),    
    applist_d3_in_catecoin_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='coin' & in_diff<=3]),    
    applist_d7_in_catecoin_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='coin' & in_diff<=7]),    
    applist_d14_in_catecoin_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='coin' & in_diff<=14]),    
    applist_d30_in_catecoin_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='coin' & in_diff<=30]),
    applist_in_catecoin_date_max=if_else(max(in_diff[app_type==0 & cate=='coin' & pre_in==0],na.rm=T)%>%is.infinite(),-1,max(in_diff[app_type==0  & cate=='coin' & pre_in==0],na.rm=T)),
    
    applist_in_catecommu_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='commu']),
    applist_d0_in_catecommu_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='commu' & in_diff<=0]),    
    applist_d3_in_catecommu_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='commu' & in_diff<=3]),    
    applist_d7_in_catecommu_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='commu' & in_diff<=7]),    
    applist_d14_in_catecommu_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='commu' & in_diff<=14]),    
    applist_d30_in_catecommu_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='commu' & in_diff<=30]),
    applist_in_catecommu_date_max=if_else(max(in_diff[app_type==0 & cate=='commu' & pre_in==0],na.rm=T)%>%is.infinite(),-1,max(in_diff[app_type==0  & cate=='commu' & pre_in==0],na.rm=T)),
    
    applist_in_catecontact_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='contact']),
    applist_d0_in_catecontact_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='contact' & in_diff<=0]),    
    applist_d3_in_catecontact_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='contact' & in_diff<=3]),    
    applist_d7_in_catecontact_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='contact' & in_diff<=7]),    
    applist_d14_in_catecontact_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='contact' & in_diff<=14]),    
    applist_d30_in_catecontact_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='contact' & in_diff<=30]),
    applist_in_catecontact_date_max=if_else(max(in_diff[app_type==0 & cate=='contact' & pre_in==0],na.rm=T)%>%is.infinite(),-1,max(in_diff[app_type==0  & cate=='contact' & pre_in==0],na.rm=T)),
   
    applist_in_cateedu_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='edu']),
    applist_d0_in_cateedu_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='edu' & in_diff<=0]),    
    applist_d3_in_cateedu_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='edu' & in_diff<=3]),    
    applist_d7_in_cateedu_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='edu' & in_diff<=7]),    
    applist_d14_in_cateedu_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='edu' & in_diff<=14]),    
    applist_d30_in_cateedu_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='edu' & in_diff<=30]),
    applist_in_cateedu_date_max=if_else(max(in_diff[app_type==0 & cate=='edu' & pre_in==0],na.rm=T)%>%is.infinite(),-1,max(in_diff[app_type==0  & cate=='edu' & pre_in==0],na.rm=T)),
   
    applist_in_categame_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='game']),
    applist_d0_in_categame_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='game' & in_diff<=0]),    
    applist_d3_in_categame_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='game' & in_diff<=3]),    
    applist_d7_in_categame_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='game' & in_diff<=7]),    
    applist_d14_in_categame_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='game' & in_diff<=14]),    
    applist_d30_in_categame_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='game' & in_diff<=30]),
    applist_in_categame_date_max=if_else(max(in_diff[app_type==0 & cate=='game' & pre_in==0],na.rm=T)%>%is.infinite(),-1,max(in_diff[app_type==0  & cate=='game' & pre_in==0],na.rm=T)),
   
    applist_in_catehealth_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='health']),
    applist_d0_in_catehealth_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='health' & in_diff<=0]),    
    applist_d3_in_catehealth_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='health' & in_diff<=3]),    
    applist_d7_in_catehealth_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='health' & in_diff<=7]),    
    applist_d14_in_catehealth_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='health' & in_diff<=14]),    
    applist_d30_in_catehealth_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='health' & in_diff<=30]),
    applist_in_catehealth_date_max=if_else(max(in_diff[app_type==0 & cate=='health' & pre_in==0],na.rm=T)%>%is.infinite(),-1,max(in_diff[app_type==0  & cate=='health' & pre_in==0],na.rm=T)),
   
    applist_in_cateoperator_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='operator']),
    applist_d0_in_cateoperator_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='operator' & in_diff<=0]),    
    applist_d3_in_cateoperator_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='operator' & in_diff<=3]),    
    applist_d7_in_cateoperator_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='operator' & in_diff<=7]),    
    applist_d14_in_cateoperator_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='operator' & in_diff<=14]),    
    applist_d30_in_cateoperator_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='operator' & in_diff<=30]),
    applist_in_cateoperator_date_max=if_else(max(in_diff[app_type==0 & cate=='operator' & pre_in==0],na.rm=T)%>%is.infinite(),-1,max(in_diff[app_type==0  & cate=='operator' & pre_in==0],na.rm=T)),
    
    applist_in_catepaymentewallet_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='paymentewallet']),
    applist_d0_in_catepaymentewallet_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='paymentewallet' & in_diff<=0]),    
    applist_d3_in_catepaymentewallet_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='paymentewallet' & in_diff<=3]),    
    applist_d7_in_catepaymentewallet_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='paymentewallet' & in_diff<=7]),    
    applist_d14_in_catepaymentewallet_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='paymentewallet' & in_diff<=14]),    
    applist_d30_in_catepaymentewallet_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='paymentewallet' & in_diff<=30]),
    applist_in_catepaymentewallet_date_max=if_else(max(in_diff[app_type==0 & cate=='paymentewallet' & pre_in==0],na.rm=T)%>%is.infinite(),-1,max(in_diff[app_type==0  & cate=='paymentewallet' & pre_in==0],na.rm=T)),
    applist_in_catepaymentewallet_date_min=if_else(min(in_diff[app_type==0 & cate=='paymentewallet' & pre_in==0],na.rm=T)%>%is.infinite(),-1,min(in_diff[app_type==0  & cate=='paymentewallet' & pre_in==0],na.rm=T)),
    
    applist_in_catepaymentmerchant_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='paymentmerchant']),
    applist_d0_in_catepaymentmerchant_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='paymentmerchant' & in_diff<=0]),    
    applist_d3_in_catepaymentmerchant_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='paymentmerchant' & in_diff<=3]),    
    applist_d7_in_catepaymentmerchant_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='paymentmerchant' & in_diff<=7]),    
    applist_d14_in_catepaymentmerchant_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='paymentmerchant' & in_diff<=14]),    
    applist_d30_in_catepaymentmerchant_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='paymentmerchant' & in_diff<=30]),
    applist_in_catepaymentmerchant_date_max=if_else(max(in_diff[app_type==0 & cate=='paymentmerchant' & pre_in==0],na.rm=T)%>%is.infinite(),-1,max(in_diff[app_type==0  & cate=='paymentmerchant' & pre_in==0],na.rm=T)),

    applist_in_catepaymentmobilbank_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='paymentmobilbank']),
    applist_d0_in_catepaymentmobilbank_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='paymentmobilbank' & in_diff<=0]),    
    applist_d3_in_catepaymentmobilbank_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='paymentmobilbank' & in_diff<=3]),    
    applist_d7_in_catepaymentmobilbank_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='paymentmobilbank' & in_diff<=7]),    
    applist_d14_in_catepaymentmobilbank_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='paymentmobilbank' & in_diff<=14]),    
    applist_d30_in_catepaymentmobilbank_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='paymentmobilbank' & in_diff<=30]),
    applist_in_catepaymentmobilbank_date_max=if_else(max(in_diff[app_type==0 & cate=='paymentmobilbank' & pre_in==0],na.rm=T)%>%is.infinite(),-1,max(in_diff[app_type==0  & cate=='paymentmobilbank' & pre_in==0],na.rm=T)),

    applist_in_catephonebackup_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='phonebackup']),
    applist_d0_in_catephonebackup_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='phonebackup' & in_diff<=0]),    
    applist_d3_in_catephonebackup_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='phonebackup' & in_diff<=3]),    
    applist_d7_in_catephonebackup_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='phonebackup' & in_diff<=7]),    
    applist_d14_in_catephonebackup_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='phonebackup' & in_diff<=14]),    
    applist_d30_in_catephonebackup_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='phonebackup' & in_diff<=30]),
    applist_in_catephonebackup_date_max=if_else(max(in_diff[app_type==0 & cate=='phonebackup' & pre_in==0],na.rm=T)%>%is.infinite(),-1,max(in_diff[app_type==0  & cate=='phonebackup' & pre_in==0],na.rm=T)),

    applist_in_catepublic_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='public']),
    applist_d0_in_catepublic_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='public' & in_diff<=0]),    
    applist_d3_in_catepublic_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='public' & in_diff<=3]),    
    applist_d7_in_catepublic_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='public' & in_diff<=7]),    
    applist_d14_in_catepublic_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='public' & in_diff<=14]),    
    applist_d30_in_catepublic_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='public' & in_diff<=30]),
    applist_in_catepublic_date_max=if_else(max(in_diff[app_type==0 & cate=='public' & pre_in==0],na.rm=T)%>%is.infinite(),-1,max(in_diff[app_type==0  & cate=='public' & pre_in==0],na.rm=T)),

    applist_in_cateregion_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='region']),
    applist_d0_in_cateregion_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='region' & in_diff<=0]),    
    applist_d3_in_cateregion_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='region' & in_diff<=3]),    
    applist_d7_in_cateregion_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='region' & in_diff<=7]),    
    applist_d14_in_cateregion_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='region' & in_diff<=14]),    
    applist_d30_in_cateregion_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='region' & in_diff<=30]),
    applist_in_cateregion_date_max=if_else(max(in_diff[app_type==0 & cate=='region' & pre_in==0],na.rm=T)%>%is.infinite(),-1,max(in_diff[app_type==0  & cate=='region' & pre_in==0],na.rm=T)),

    applist_in_cateshopping_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='shopping']),
    applist_d0_in_cateshopping_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='shopping' & in_diff<=0]),    
    applist_d3_in_cateshopping_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='shopping' & in_diff<=3]),    
    applist_d7_in_cateshopping_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='shopping' & in_diff<=7]),    
    applist_d14_in_cateshopping_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='shopping' & in_diff<=14]),    
    applist_d30_in_cateshopping_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='shopping' & in_diff<=30]),
    applist_in_cateshopping_date_max=if_else(max(in_diff[app_type==0 & cate=='shopping' & pre_in==0],na.rm=T)%>%is.infinite(),-1,max(in_diff[app_type==0  & cate=='shopping' & pre_in==0],na.rm=T)),

    applist_in_catetelcom_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='telcom']),
    applist_d0_in_catetelcom_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='telcom' & in_diff<=0]),    
    applist_d3_in_catetelcom_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='telcom' & in_diff<=3]),    
    applist_d7_in_catetelcom_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='telcom' & in_diff<=7]),    
    applist_d14_in_catetelcom_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='telcom' & in_diff<=14]),    
    applist_d30_in_catetelcom_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='telcom' & in_diff<=30]),
    applist_in_catetelcom_date_max=if_else(max(in_diff[app_type==0 & cate=='telcom' & pre_in==0],na.rm=T)%>%is.infinite(),-1,max(in_diff[app_type==0  & cate=='telcom' & pre_in==0],na.rm=T)),

    applist_in_catetelcomcommu_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='telcomcommu']),
    applist_d0_in_catetelcomcommu_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='telcomcommu' & in_diff<=0]),    
    applist_d3_in_catetelcomcommu_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='telcomcommu' & in_diff<=3]),    
    applist_d7_in_catetelcomcommu_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='telcomcommu' & in_diff<=7]),    
    applist_d14_in_catetelcomcommu_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='telcomcommu' & in_diff<=14]),    
    applist_d30_in_catetelcomcommu_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='telcomcommu' & in_diff<=30]),
    applist_in_catetelcomcommu_date_max=if_else(max(in_diff[app_type==0 & cate=='telcomcommu' & pre_in==0],na.rm=T)%>%is.infinite(),-1,max(in_diff[app_type==0  & cate=='telcomcommu' & pre_in==0],na.rm=T)),

    applist_in_catework_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='work']),
    applist_d0_in_catework_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='work' & in_diff<=0]),    
    applist_d3_in_catework_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='work' & in_diff<=3]),    
    applist_d7_in_catework_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='work' & in_diff<=7]),    
    applist_d14_in_catework_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='work' & in_diff<=14]),    
    applist_d30_in_catework_cnt=uniqueN(app_name[app_type==0 & pre_in==0 & cate=='work' & in_diff<=30]),
    applist_in_catework_date_max=if_else(max(in_diff[app_type==0 & cate=='work' & pre_in==0],na.rm=T)%>%is.infinite(),-1,max(in_diff[app_type==0  & cate=='work' & pre_in==0],na.rm=T)),

    applist_is_in_cloudflare=uniqueN(app_name[app_name=='1.1.1.1' | package=='com.cloudflare.onedotonedotonedotone']),
    applist_in_cloudflare_date_max=if_else(max(in_diff[app_type==0 & (app_name=='1.1.1.1' | package=='com.cloudflare.onedotonedotonedotone') & pre_in==0],na.rm=T)%>%is.infinite(),-1,max(in_diff[app_type==0  & (app_name=='1.1.1.1' | package=='com.cloudflare.onedotonedotonedotone') & pre_in==0],na.rm=T)),
    
    applist_is_in_nidwallet=uniqueN(app_name[app_name=='nid wallet' | package=='bd.gov.nidw.nid.wallet']),
    applist_in_nidwallet_date_max=if_else(max(in_diff[app_type==0 & (app_name=='nid wallet' | package=='bd.gov.nidw.nid.wallet') & pre_in==0],na.rm=T)%>%is.infinite(),-1,max(in_diff[app_type==0  & (app_name=='nid wallet' | package=='bd.gov.nidw.nid.wallet') & pre_in==0],na.rm=T)),
    
    applist_is_in_phoneclone=uniqueN(app_name[str_detect(app_name,'clone') | package %in% c('com.coloros.backuprestore','com.oneplus.backuprestore','com.sec.android.easyMover')]),
    applist_in_phoneclone_date_max=if_else(max(in_diff[app_type==0 & (str_detect(app_name,'clone') | package %in% c('com.coloros.backuprestore','com.oneplus.backuprestore','com.sec.android.easyMover')) & pre_in==0],na.rm=T)%>%is.infinite(),-1,max(in_diff[app_type==0  & (str_detect(app_name,'clone') | package %in% c('com.coloros.backuprestore','com.oneplus.backuprestore','com.sec.android.easyMover')) & pre_in==0],na.rm=T)),
    
    applist_is_in_imo=uniqueN(app_name[package %in% c('com.imo.android.imoim')]),
    applist_in_phoneclone_date_max=if_else(max(in_diff[app_type==0 & (package %in% c('com.imo.android.imoim')) & pre_in==0],na.rm=T)%>%is.infinite(),-1,max(in_diff[app_type==0  & (package %in% c('com.imo.android.imoim')) & pre_in==0],na.rm=T)),
    
    
    
      )->applist_cate_var

applist_cate_var_frame<<-rbind(applist_cate_var_frame,applist_cate_var)

print(paste(Sys.time(),' 特征计算结束',sep=""))

i=num+1
print(paste('i递增后的值',i,Sys.time()),sep="",)

}
dbWriteTable(connec_bu,'applist_var',applist_cate_var_frame,overwrite=T,row.names=F)
# dbWriteTable(connec_bu,'card_multi_var',card_multi_var_frame,overwrite=T,row.names=F)






