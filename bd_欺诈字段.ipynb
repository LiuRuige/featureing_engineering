# same_phone_diff_device_apply_cnt
def fraud_variable(order_id):
     conn = create_engine('postgresql://BASIC$root:WCjak3$RQnf3ST@8.210.75.205:13343/test')
     sql = '''select a.order_id,
          count(distinct case when b.order_id is not null and a.user_device_id is not null and a.user_device_id != b.user_device_id and b.user_device_id ~* '(\w|\d){{8}}-(\w|\d){{4}}-(\w|\d){{4}}-(\w|\d){{4}}-(\w|\d){{12}}' and b.user_device_id not in ('','00000000-0000-0000-0000-000000000000','f54bed004c6896417635db54baeac499','e4944f2e-1114-48d0-a93e-c943bad10221','bbd64611-c2cb-4726-87fa-9e3f8149a25e') then b.user_device_id 
          when b.order_id is not null and a.user_device_id is null and b.user_device_id ~* '(\w|\d){{8}}-(\w|\d){{4}}-(\w|\d){{4}}-(\w|\d){{4}}-(\w|\d){{12}}' and b.user_device_id not in ('','00000000-0000-0000-0000-000000000000','f54bed004c6896417635db54baeac499','e4944f2e-1114-48d0-a93e-c943bad10221','bbd64611-c2cb-4726-87fa-9e3f8149a25e') then b.user_device_id
          else null end) as same_phone_diff_device_apply_cnt
          from bengal_test.bd_dwd_order a left join bengal_test.bd_dwd_order b on a.user_phone=b.user_phone and b.borrow_time<=a.borrow_time
          where a.order_id = '{}'        
          group by 1
          order by 1 desc;'''.format(order_id)
     same_phone_diff_device_apply_cnt = pd.read_sql_query(sql, conn)
     conn.dispose()
      

     # same_phone_diff_cardid_apply_cnt
     conn = create_engine('postgresql://BASIC$root:WCjak3$RQnf3ST@8.210.75.205:13343/test')
     sql = '''select a.order_id,
          uniq(case when b.order_id is not null then b.user_card_id else null end) as same_phone_diff_cardid_apply_cnt
          from bengal_test.bd_dwd_order a left join bengal_test.bd_dwd_order b on a.user_phone=b.user_phone and a.user_card_id != b.user_card_id and b.borrow_time<=a.borrow_time
          where a.order_id = '{}'
          group by 1
          order by 1 desc;'''.format(order_id) 
     same_phone_diff_cardid_apply_cnt = pd.read_sql_query(sql, conn)
     conn.dispose()
     


     # same_phone_diff_account_apply_cnt
     conn = create_engine('postgresql://BASIC$root:WCjak3$RQnf3ST@8.210.75.205:13343/test')
     sql = '''select a.order_id,
          count(distinct case when b.order_id is not null and a.user_bank_account is not null and a.user_bank_account != b.user_bank_account then b.user_bank_account 
          when b.order_id is not null and a.user_bank_account is null then b.user_bank_account
          else null end) as same_phone_diff_account_apply_cnt
          from bengal_test.bd_dwd_order a left join bengal_test.bd_dwd_order b on a.user_phone=b.user_phone and b.borrow_time<=a.borrow_time
          where a.order_id = '{}'
          group by 1
          order by 1 desc;'''.format(order_id) 
     same_phone_diff_account_apply_cnt = pd.read_sql_query(sql, conn)
     conn.dispose()
 

     # same_device_diff_phone_apply_cnt
     conn = create_engine('postgresql://BASIC$root:WCjak3$RQnf3ST@8.210.75.205:13343/test')
     sql = '''select a.order_id,
          uniq(case when b.order_id is not null then b.user_phone else null end) as same_device_diff_phone_apply_cnt
          from bengal_test.bd_dwd_order a left join bengal_test.bd_dwd_order b on a.user_device_id=b.user_device_id and a.user_phone != b.user_phone and b.borrow_time<=a.borrow_time
          where a.order_id = '{}'
          and a.user_device_id ~* '(\w|\d){{8}}-(\w|\d){{4}}-(\w|\d){{4}}-(\w|\d){{4}}-(\w|\d){{12}}' 
          and a.user_device_id is not null
          and a.user_device_id not in ('','00000000-0000-0000-0000-000000000000','f54bed004c6896417635db54baeac499','e4944f2e-1114-48d0-a93e-c943bad10221','bbd64611-c2cb-4726-87fa-9e3f8149a25e')
          group by 1
          order by 1 desc;'''.format(order_id)
     same_device_diff_phone_apply_cnt = pd.read_sql_query(sql, conn)
     conn.dispose()
 

     # same_device_diff_cardid_apply_cnt
     conn = create_engine('postgresql://BASIC$root:WCjak3$RQnf3ST@8.210.75.205:13343/test')
     sql = '''select a.order_id,
          uniq(case when b.order_id is not null then b.user_card_id else null end) as same_device_diff_cardid_apply_cnt
          from bengal_test.bd_dwd_order a left join bengal_test.bd_dwd_order b on a.user_device_id=b.user_device_id and a.user_card_id != b.user_card_id and b.borrow_time<=a.borrow_time
          where a.order_id = '{}'
          and a.user_device_id ~* '(\w|\d){{8}}-(\w|\d){{4}}-(\w|\d){{4}}-(\w|\d){{4}}-(\w|\d){{12}}' 
          and a.user_device_id is not null 
          and a.user_device_id not in ('','00000000-0000-0000-0000-000000000000','f54bed004c6896417635db54baeac499','e4944f2e-1114-48d0-a93e-c943bad10221','bbd64611-c2cb-4726-87fa-9e3f8149a25e')
          group by 1
          order by 1 desc;'''.format(order_id)
     same_device_diff_cardid_apply_cnt = pd.read_sql_query(sql, conn)
     conn.dispose()
 

     # same_device_diff_account_apply_cnt
     # same_device_diff_account_d7_apply_cnt
     # same_device_diff_account_d14_apply_cnt
     # same_device_diff_account_d30_apply_cnt
     conn = create_engine('postgresql://BASIC$root:WCjak3$RQnf3ST@8.210.75.205:13343/test')
     sql = '''select a.order_id,
          count(distinct case when b.order_id is not null and a.user_bank_account is not null and a.user_bank_account != b.user_bank_account then b.user_bank_account 
          when b.order_id is not null and a.user_bank_account is null then b.user_bank_account
          else null end) as same_device_diff_account_apply_cnt,
          count(distinct case when b.order_id is not null and a.user_bank_account is not null and a.user_bank_account != b.user_bank_account and date(a.borrow_time)-date(b.borrow_time)<=7 then b.user_bank_account 
          when b.order_id is not null and a.user_bank_account is null and date(a.borrow_time)-date(b.borrow_time)<=7 then b.user_bank_account
          else null end) as same_device_diff_account_d7_apply_cnt,
          count(distinct case when b.order_id is not null and a.user_bank_account is not null and a.user_bank_account != b.user_bank_account and date(a.borrow_time)-date(b.borrow_time)<=14 then b.user_bank_account 
          when b.order_id is not null and a.user_bank_account is null and date(a.borrow_time)-date(b.borrow_time)<=14 then b.user_bank_account
          else null end) as same_device_diff_account_d14_apply_cnt,
          count(distinct case when b.order_id is not null and a.user_bank_account is not null and a.user_bank_account != b.user_bank_account and date(a.borrow_time)-date(b.borrow_time)<=30 then b.user_bank_account 
          when b.order_id is not null and a.user_bank_account is null and date(a.borrow_time)-date(b.borrow_time)<=30 then b.user_bank_account
          else null end) as same_device_diff_account_d30_apply_cnt
          from bengal_test.bd_dwd_order a left join bengal_test.bd_dwd_order b on a.user_device_id=b.user_device_id and b.borrow_time<=a.borrow_time
          where a.order_id = '{}'
          and a.user_device_id ~* '(\w|\d){{8}}-(\w|\d){{4}}-(\w|\d){{4}}-(\w|\d){{4}}-(\w|\d){{12}}' 
          and a.user_device_id is not null 
          and a.user_device_id not in ('','00000000-0000-0000-0000-000000000000','f54bed004c6896417635db54baeac499','e4944f2e-1114-48d0-a93e-c943bad10221','bbd64611-c2cb-4726-87fa-9e3f8149a25e')
          group by 1
          order by 1 desc;'''.format(order_id)
     same_device_diff_account_apply_cnt = pd.read_sql_query(sql, conn)
     conn.dispose()
   

     # same_cardid_diff_phone_apply_cnt
     conn = create_engine('postgresql://BASIC$root:WCjak3$RQnf3ST@8.210.75.205:13343/test')
     sql = '''select a.order_id,
          uniq(case when b.order_id is not null then b.user_phone else null end) as same_cardid_diff_phone_apply_cnt
          from bengal_test.bd_dwd_order a left join bengal_test.bd_dwd_order b on a.user_card_id=b.user_card_id and a.user_phone != b.user_phone and b.borrow_time<=a.borrow_time
          where a.order_id = '{}'
          group by 1
          order by 1 desc;'''.format(order_id)
     same_cardid_diff_phone_apply_cnt = pd.read_sql_query(sql, conn)
     conn.dispose()
 

     # same_cardid_diff_device_apply_cnt
     conn = create_engine('postgresql://BASIC$root:WCjak3$RQnf3ST@8.210.75.205:13343/test')
     sql = '''select a.order_id,
          count(distinct case when b.order_id is not null and a.user_device_id is not null and a.user_device_id != b.user_device_id and b.user_device_id ~* '(\w|\d){{8}}-(\w|\d){{4}}-(\w|\d){{4}}-(\w|\d){{4}}-(\w|\d){{12}}' and b.user_device_id not in ('','00000000-0000-0000-0000-000000000000','f54bed004c6896417635db54baeac499','e4944f2e-1114-48d0-a93e-c943bad10221','bbd64611-c2cb-4726-87fa-9e3f8149a25e') then b.user_device_id 
          when b.order_id is not null and a.user_device_id is null and b.user_device_id ~* '(\w|\d){{8}}-(\w|\d){{4}}-(\w|\d){{4}}-(\w|\d){{4}}-(\w|\d){{12}}' and b.user_device_id not in ('','00000000-0000-0000-0000-000000000000','f54bed004c6896417635db54baeac499','e4944f2e-1114-48d0-a93e-c943bad10221','bbd64611-c2cb-4726-87fa-9e3f8149a25e') then b.user_device_id
          else null end) as same_cardid_diff_device_apply_cnt
          from bengal_test.bd_dwd_order a left join bengal_test.bd_dwd_order b on a.user_card_id=b.user_card_id and b.borrow_time<=a.borrow_time
          where a.order_id = '{}'
          group by 1
          order by 1 desc;'''.format(order_id)
     same_cardid_diff_device_apply_cnt = pd.read_sql_query(sql, conn)
     conn.dispose()
 

     # same_cardid_diff_account_apply_cnt
     conn = create_engine('postgresql://BASIC$root:WCjak3$RQnf3ST@8.210.75.205:13343/test')
     sql = '''select a.order_id,
          count(distinct case when b.order_id is not null and a.user_bank_account is not null and a.user_bank_account != b.user_bank_account then b.user_bank_account 
          when b.order_id is not null and a.user_bank_account is null then b.user_bank_account
          else null end) as same_cardid_diff_account_apply_cnt
          from bengal_test.bd_dwd_order a left join bengal_test.bd_dwd_order b on a.user_card_id=b.user_card_id and b.borrow_time<=a.borrow_time
          where a.order_id = '{}'
          group by 1
          order by 1 desc;'''.format(order_id)
     same_cardid_diff_account_apply_cnt = pd.read_sql_query(sql, conn)
     conn.dispose()
 

     # same_account_diff_phone_apply_cnt 特殊情况user_bank_account=''线上计算值为0
     conn = create_engine('postgresql://BASIC$root:WCjak3$RQnf3ST@8.210.75.205:13343/test')
     sql = '''select a.order_id,
          uniq(case when b.order_id is not null and a.user_bank_account != '' then b.user_phone else null end) as same_account_diff_phone_apply_cnt
          from bengal_test.bd_dwd_order a left join bengal_test.bd_dwd_order b on a.user_bank_account=b.user_bank_account and a.user_phone != b.user_phone and b.borrow_time<=a.borrow_time
          where a.order_id = '{}'
          and a.user_bank_account is not null
          group by 1
          order by 1 desc;'''.format(order_id)
     same_account_diff_phone_apply_cnt = pd.read_sql_query(sql, conn)
     conn.dispose()
 

     # same_account_diff_cardid_apply_cnt 特殊情况user_bank_account=''线上计算值为0
     conn = create_engine('postgresql://BASIC$root:WCjak3$RQnf3ST@8.210.75.205:13343/test')
     sql = '''select a.order_id,
          uniq(case when b.order_id is not null and a.user_bank_account != '' then b.user_card_id else null end) as same_account_diff_cardid_apply_cnt
          from bengal_test.bd_dwd_order a left join bengal_test.bd_dwd_order b on a.user_bank_account=b.user_bank_account and a.user_card_id != b.user_card_id and b.borrow_time<=a.borrow_time
          where a.order_id = '{}'
          and a.user_bank_account is not null
          group by 1
          order by 1 desc;'''.format(order_id)
     same_account_diff_cardid_apply_cnt = pd.read_sql_query(sql, conn)
     conn.dispose()
 
     # same_account_diff_device_apply_cnt
     # same_account_diff_device_d7_apply_cnt
     # same_account_diff_device_d14_apply_cnt
     # same_account_diff_device_d30_apply_cnt 特殊情况user_bank_account=''线上计算值为0
     conn = create_engine('postgresql://BASIC$root:WCjak3$RQnf3ST@8.210.75.205:13343/test')
     sql = '''select a.order_id,
          count(distinct case when b.order_id is not null and a.user_bank_account != '' and a.user_device_id is not null and a.user_device_id != b.user_device_id and b.user_device_id ~* '(\w|\d){{8}}-(\w|\d){{4}}-(\w|\d){{4}}-(\w|\d){{4}}-(\w|\d){{12}}' and b.user_device_id not in ('','00000000-0000-0000-0000-000000000000','f54bed004c6896417635db54baeac499','e4944f2e-1114-48d0-a93e-c943bad10221','bbd64611-c2cb-4726-87fa-9e3f8149a25e') then b.user_device_id 
          when b.order_id is not null and a.user_device_id is null and b.user_device_id ~* '(\w|\d){{8}}-(\w|\d){{4}}-(\w|\d){{4}}-(\w|\d){{4}}-(\w|\d){{12}}' and b.user_device_id not in ('','00000000-0000-0000-0000-000000000000','f54bed004c6896417635db54baeac499','e4944f2e-1114-48d0-a93e-c943bad10221','bbd64611-c2cb-4726-87fa-9e3f8149a25e') then b.user_device_id
          else null end) as same_account_diff_device_apply_cnt,
          count(distinct case when b.order_id is not null and a.user_bank_account != '' and a.user_device_id is not null and a.user_device_id != b.user_device_id and b.user_device_id ~* '(\w|\d){{8}}-(\w|\d){{4}}-(\w|\d){{4}}-(\w|\d){{4}}-(\w|\d){{12}}' and b.user_device_id not in ('','00000000-0000-0000-0000-000000000000','f54bed004c6896417635db54baeac499','e4944f2e-1114-48d0-a93e-c943bad10221','bbd64611-c2cb-4726-87fa-9e3f8149a25e') and date(a.borrow_time)-date(b.borrow_time)<=7 then b.user_device_id 
          when b.order_id is not null and a.user_device_id is null and b.user_device_id ~* '(\w|\d){{8}}-(\w|\d){{4}}-(\w|\d){{4}}-(\w|\d){{4}}-(\w|\d){{12}}' and b.user_device_id not in ('','00000000-0000-0000-0000-000000000000','f54bed004c6896417635db54baeac499','e4944f2e-1114-48d0-a93e-c943bad10221','bbd64611-c2cb-4726-87fa-9e3f8149a25e') and date(a.borrow_time)-date(b.borrow_time)<=7 then b.user_device_id
          else null end) as same_account_diff_device_d7_apply_cnt,
          count(distinct case when b.order_id is not null and a.user_bank_account != '' and a.user_device_id is not null and a.user_device_id != b.user_device_id and b.user_device_id ~* '(\w|\d){{8}}-(\w|\d){{4}}-(\w|\d){{4}}-(\w|\d){{4}}-(\w|\d){{12}}' and b.user_device_id not in ('','00000000-0000-0000-0000-000000000000','f54bed004c6896417635db54baeac499','e4944f2e-1114-48d0-a93e-c943bad10221','bbd64611-c2cb-4726-87fa-9e3f8149a25e') and date(a.borrow_time)-date(b.borrow_time)<=14 then b.user_device_id 
          when b.order_id is not null and a.user_device_id is null and b.user_device_id ~* '(\w|\d){{8}}-(\w|\d){{4}}-(\w|\d){{4}}-(\w|\d){{4}}-(\w|\d){{12}}' and b.user_device_id not in ('','00000000-0000-0000-0000-000000000000','f54bed004c6896417635db54baeac499','e4944f2e-1114-48d0-a93e-c943bad10221','bbd64611-c2cb-4726-87fa-9e3f8149a25e') and date(a.borrow_time)-date(b.borrow_time)<=14 then b.user_device_id
          else null end) as same_account_diff_device_d14_apply_cnt,
          count(distinct case when b.order_id is not null and a.user_bank_account != '' and a.user_device_id is not null and a.user_device_id != b.user_device_id and b.user_device_id ~* '(\w|\d){{8}}-(\w|\d){{4}}-(\w|\d){{4}}-(\w|\d){{4}}-(\w|\d){{12}}' and b.user_device_id not in ('','00000000-0000-0000-0000-000000000000','f54bed004c6896417635db54baeac499','e4944f2e-1114-48d0-a93e-c943bad10221','bbd64611-c2cb-4726-87fa-9e3f8149a25e') and date(a.borrow_time)-date(b.borrow_time)<=30 then b.user_device_id 
          when b.order_id is not null and a.user_device_id is null and b.user_device_id ~* '(\w|\d){{8}}-(\w|\d){{4}}-(\w|\d){{4}}-(\w|\d){{4}}-(\w|\d){{12}}' and b.user_device_id not in ('','00000000-0000-0000-0000-000000000000','f54bed004c6896417635db54baeac499','e4944f2e-1114-48d0-a93e-c943bad10221','bbd64611-c2cb-4726-87fa-9e3f8149a25e') and date(a.borrow_time)-date(b.borrow_time)<=30 then b.user_device_id
          else null end) as same_account_diff_device_d30_apply_cnt
          from bengal_test.bd_dwd_order a left join bengal_test.bd_dwd_order b on a.user_bank_account=b.user_bank_account and b.borrow_time<=a.borrow_time
          where a.order_id = '{}'
          and a.user_bank_account is not null
          group by 1
          order by 1 desc;'''.format(order_id)
     same_account_diff_device_apply_cnt = pd.read_sql_query(sql, conn)
     conn.dispose()
 

     # same_name_diff_phone_apply_cnt
     conn = create_engine('postgresql://BASIC$root:WCjak3$RQnf3ST@8.210.75.205:13343/test')
     sql = '''select a.order_id,
          uniq(case when b.order_id is not null then b.user_phone else null end) as same_name_diff_phone_apply_cnt
          from bengal_test.bd_dwd_order a left join bengal_test.bd_dwd_order b on a.user_name=b.user_name and a.user_phone != b.user_phone and b.borrow_time<=a.borrow_time
          where a.order_id = '{}'
          group by 1
          order by 1 desc;'''.format(order_id)
     same_name_diff_phone_apply_cnt = pd.read_sql_query(sql, conn)
     conn.dispose()
 

     # same_name_diff_cardid_apply_cnt
     conn = create_engine('postgresql://BASIC$root:WCjak3$RQnf3ST@8.210.75.205:13343/test')
     sql = '''select a.order_id,
          uniq(case when b.order_id is not null then b.user_card_id else null end) as same_name_diff_cardid_apply_cnt
          from bengal_test.bd_dwd_order a left join bengal_test.bd_dwd_order b on a.user_name=b.user_name and a.user_card_id != b.user_card_id and b.borrow_time<=a.borrow_time
          where a.order_id = '{}'
          group by 1
          order by 1 desc;'''.format(order_id)
     same_name_diff_cardid_apply_cnt = pd.read_sql_query(sql, conn)
     conn.dispose()
   

     # same_name_diff_device_apply_cnt
     conn = create_engine('postgresql://BASIC$root:WCjak3$RQnf3ST@8.210.75.205:13343/test')
     sql = '''select a.order_id,
          count(distinct case when b.order_id is not null and a.user_device_id is not null and a.user_device_id != b.user_device_id and b.user_device_id ~* '(\w|\d){{8}}-(\w|\d){{4}}-(\w|\d){{4}}-(\w|\d){{4}}-(\w|\d){{12}}' and b.user_device_id not in ('','00000000-0000-0000-0000-000000000000','f54bed004c6896417635db54baeac499','e4944f2e-1114-48d0-a93e-c943bad10221','bbd64611-c2cb-4726-87fa-9e3f8149a25e') then b.user_device_id 
          when b.order_id is not null and a.user_device_id is null and b.user_device_id ~* '(\w|\d){{8}}-(\w|\d){{4}}-(\w|\d){{4}}-(\w|\d){{4}}-(\w|\d){{12}}' and b.user_device_id not in ('','00000000-0000-0000-0000-000000000000','f54bed004c6896417635db54baeac499','e4944f2e-1114-48d0-a93e-c943bad10221','bbd64611-c2cb-4726-87fa-9e3f8149a25e') then b.user_device_id
          else null end) as same_name_diff_device_apply_cnt
          from bengal_test.bd_dwd_order a left join bengal_test.bd_dwd_order b on a.user_name=b.user_name and b.borrow_time<=a.borrow_time
          where a.order_id = '{}'
          group by 1
          order by 1 desc;'''.format(order_id)
     same_name_diff_device_apply_cnt = pd.read_sql_query(sql, conn)
     conn.dispose()
 

     # same_name_diff_account_apply_cnt
     conn = create_engine('postgresql://BASIC$root:WCjak3$RQnf3ST@8.210.75.205:13343/test')
     sql = '''select a.order_id,
          count(distinct case when b.order_id is not null and a.user_bank_account is not null and a.user_bank_account != b.user_bank_account then b.user_bank_account 
          when b.order_id is not null and a.user_bank_account is null then b.user_bank_account
          else null end) as same_name_diff_account_apply_cnt
          from bengal_test.bd_dwd_order a left join bengal_test.bd_dwd_order b on a.user_name=b.user_name and b.borrow_time<=a.borrow_time
          where a.order_id = '{}'
          group by 1
          order by 1 desc;'''.format(order_id)
     same_name_diff_account_apply_cnt = pd.read_sql_query(sql, conn)
     conn.dispose()
 

     # same_relatone_diff_phone_apply_cnt
     conn = create_engine('postgresql://BASIC$root:WCjak3$RQnf3ST@8.210.75.205:13343/test')
     sql = '''select a.order_id,
          uniq(case when b.order_id is not null then b.user_phone else null end) as same_relatone_diff_phone_apply_cnt
          from bengal_test.bd_dwd_order a left join bengal_test.bd_dwd_order b on a.user_relate_one=b.user_relate_one and a.user_phone != b.user_phone and b.borrow_time<=a.borrow_time
          where a.order_id = '{}'
          group by 1
          order by 1 desc;'''.format(order_id)
     same_relatone_diff_phone_apply_cnt = pd.read_sql_query(sql, conn)
     conn.dispose()
 

     # same_relatone_diff_cardid_apply_cnt
     conn = create_engine('postgresql://BASIC$root:WCjak3$RQnf3ST@8.210.75.205:13343/test')
     sql = '''select a.order_id,
          uniq(case when b.order_id is not null then b.user_card_id else null end) as same_relatone_diff_cardid_apply_cnt
          from bengal_test.bd_dwd_order a left join bengal_test.bd_dwd_order b on a.user_relate_one=b.user_relate_one and a.user_card_id != b.user_card_id and b.borrow_time<=a.borrow_time
          where a.order_id = '{}'
          group by 1
          order by 1 desc;'''.format(order_id)
     same_relatone_diff_cardid_apply_cnt = pd.read_sql_query(sql, conn)
     conn.dispose()
 

     # same_relatone_diff_device_apply_cnt
     conn = create_engine('postgresql://BASIC$root:WCjak3$RQnf3ST@8.210.75.205:13343/test')
     sql = '''select a.order_id,
          count(distinct case when b.order_id is not null and a.user_device_id is not null and a.user_device_id != b.user_device_id and b.user_device_id ~* '(\w|\d){{8}}-(\w|\d){{4}}-(\w|\d){{4}}-(\w|\d){{4}}-(\w|\d){{12}}' and b.user_device_id not in ('','00000000-0000-0000-0000-000000000000','f54bed004c6896417635db54baeac499','e4944f2e-1114-48d0-a93e-c943bad10221','bbd64611-c2cb-4726-87fa-9e3f8149a25e') then b.user_device_id 
          when b.order_id is not null and a.user_device_id is null and b.user_device_id ~* '(\w|\d){{8}}-(\w|\d){{4}}-(\w|\d){{4}}-(\w|\d){{4}}-(\w|\d){{12}}' and b.user_device_id not in ('','00000000-0000-0000-0000-000000000000','f54bed004c6896417635db54baeac499','e4944f2e-1114-48d0-a93e-c943bad10221','bbd64611-c2cb-4726-87fa-9e3f8149a25e') then b.user_device_id
          else null end) as same_relatone_diff_device_apply_cnt
          from bengal_test.bd_dwd_order a left join bengal_test.bd_dwd_order b on a.user_relate_one=b.user_relate_one and b.borrow_time<=a.borrow_time
          where a.order_id = '{}'
          group by 1
          order by 1 desc;'''.format(order_id)
     same_relatone_diff_device_apply_cnt = pd.read_sql_query(sql, conn)
     conn.dispose()
  

     # same_relatone_diff_account_apply_cnt
     conn = create_engine('postgresql://BASIC$root:WCjak3$RQnf3ST@8.210.75.205:13343/test')
     sql = '''select a.order_id,
          count(distinct case when b.order_id is not null and a.user_bank_account is not null and a.user_bank_account != b.user_bank_account then b.user_bank_account 
          when b.order_id is not null and a.user_bank_account is null then b.user_bank_account
          else null end) as same_relatone_diff_account_apply_cnt
          from bengal_test.bd_dwd_order a left join bengal_test.bd_dwd_order b on a.user_relate_one=b.user_relate_one and b.borrow_time<=a.borrow_time
          where a.order_id = '{}'
          group by 1
          order by 1 desc;'''.format(order_id)
     same_relatone_diff_account_apply_cnt = pd.read_sql_query(sql, conn)
     conn.dispose()
 

     # same_relattwo_diff_phone_apply_cnt
     conn = create_engine('postgresql://BASIC$root:WCjak3$RQnf3ST@8.210.75.205:13343/test')
     sql = '''select a.order_id,
          uniq(case when b.order_id is not null then b.user_phone else null end) as same_relattwo_diff_phone_apply_cnt
          from bengal_test.bd_dwd_order a left join bengal_test.bd_dwd_order b on a.user_relate_two=b.user_relate_two and a.user_phone != b.user_phone and b.borrow_time<=a.borrow_time
          where a.order_id = '{}'
          group by 1
          order by 1 desc;'''.format(order_id)
     same_relattwo_diff_phone_apply_cnt = pd.read_sql_query(sql, conn)
     conn.dispose()
 
     # same_relattwo_diff_cardid_apply_cnt
     conn = create_engine('postgresql://BASIC$root:WCjak3$RQnf3ST@8.210.75.205:13343/test')
     sql = '''select a.order_id,
          uniq(case when b.order_id is not null then b.user_card_id else null end) as same_relattwo_diff_cardid_apply_cnt
          from bengal_test.bd_dwd_order a left join bengal_test.bd_dwd_order b on a.user_relate_two=b.user_relate_two and a.user_card_id != b.user_card_id and b.borrow_time<=a.borrow_time
          where a.order_id = '{}'
          group by 1
          order by 1 desc;'''.format(order_id)
     same_relattwo_diff_cardid_apply_cnt = pd.read_sql_query(sql, conn)
     conn.dispose()
 

     # same_relattwo_diff_device_apply_cnt
     conn = create_engine('postgresql://BASIC$root:WCjak3$RQnf3ST@8.210.75.205:13343/test')
     sql = '''select a.order_id,
          count(distinct case when b.order_id is not null and a.user_device_id is not null and a.user_device_id != b.user_device_id and b.user_device_id ~* '(\w|\d){{8}}-(\w|\d){{4}}-(\w|\d){{4}}-(\w|\d){{4}}-(\w|\d){{12}}' and b.user_device_id not in ('','00000000-0000-0000-0000-000000000000','f54bed004c6896417635db54baeac499','e4944f2e-1114-48d0-a93e-c943bad10221','bbd64611-c2cb-4726-87fa-9e3f8149a25e') then b.user_device_id 
          when b.order_id is not null and a.user_device_id is null and b.user_device_id ~* '(\w|\d){{8}}-(\w|\d){{4}}-(\w|\d){{4}}-(\w|\d){{4}}-(\w|\d){{12}}' and b.user_device_id not in ('','00000000-0000-0000-0000-000000000000','f54bed004c6896417635db54baeac499','e4944f2e-1114-48d0-a93e-c943bad10221','bbd64611-c2cb-4726-87fa-9e3f8149a25e') then b.user_device_id
          else null end) as same_relattwo_diff_device_apply_cnt
          from bengal_test.bd_dwd_order a left join bengal_test.bd_dwd_order b on a.user_relate_two=b.user_relate_two and b.borrow_time<=a.borrow_time
          where a.order_id = '{}'
          group by 1
          order by 1 desc;'''.format(order_id)
     same_relattwo_diff_device_apply_cnt = pd.read_sql_query(sql, conn)
     conn.dispose()
 

     # same_relattwo_diff_account_apply_cnt
     conn = create_engine('postgresql://BASIC$root:WCjak3$RQnf3ST@8.210.75.205:13343/test')
     sql = '''select a.order_id,
          count(distinct case when b.order_id is not null and a.user_bank_account is not null and a.user_bank_account != b.user_bank_account then b.user_bank_account 
          when b.order_id is not null and a.user_bank_account is null then b.user_bank_account
          else null end) as same_relattwo_diff_account_apply_cnt
          from bengal_test.bd_dwd_order a left join bengal_test.bd_dwd_order b on a.user_relate_two=b.user_relate_two and b.borrow_time<=a.borrow_time
          where a.order_id = '{}'
          group by 1
          order by 1 desc;'''.format(order_id)
     same_relattwo_diff_account_apply_cnt = pd.read_sql_query(sql, conn)
     conn.dispose()
 

     # relatone_is_relattwo_phone_cnt 第一联系人关联不到第二联系人值为0
     conn = create_engine('postgresql://BASIC$root:WCjak3$RQnf3ST@8.210.75.205:13343/test')
     sql = '''select a.order_id,
          uniq(case when b.order_id is not null then b.user_phone else null end) as relatone_is_relattwo_phone_cnt
          from bengal_test.bd_dwd_order a left join bengal_test.bd_dwd_order b on a.user_relate_one=b.user_relate_two and b.borrow_time<=a.borrow_time
          where a.order_id = '{}'
          group by 1
          order by 1 desc'''
     relatone_is_relattwo_phone_cnt = pd.read_sql_query(sql, conn)
     conn.dispose()
 

     # relatone_is_relatthree_phone_cnt 第一联系人关联不到第三联系人值为0
     conn = create_engine('postgresql://BASIC$root:WCjak3$RQnf3ST@8.210.75.205:13343/test')
     sql = '''select a.order_id,
          uniq(case when b.order_id is not null then b.user_phone else null end) as relatone_is_relatthree_phone_cnt
          from bengal_test.bd_dwd_order a left join (select c.*,d.phone_three as user_relate_three from bengal_test.bd_dwd_order c left join ctm_user d on c.order_id=d.order_id)b on a.user_relate_one=b.user_relate_three and b.borrow_time<=a.borrow_time
          where a.order_id = '{}'
          group by 1
          order by 1 desc'''
     relatone_is_relatthree_phone_cnt = pd.read_sql_query(sql, conn)
     conn.dispose()
 

     # phonebk_is_relatone_phone_cnt 备用手机号关联不到第一联系人值为0
     conn = create_engine('postgresql://BASIC$root:WCjak3$RQnf3ST@8.210.75.205:13343/test')
     sql = '''select a.order_id,
          uniq(case when b.order_id is not null then b.user_phone else null end) as phonebk_is_relatone_phone_cnt
          from bengal_test.bd_dwd_order a left join bengal_test.bd_dwd_order b on a.user_phone_backup=b.user_relate_one and b.borrow_time<=a.borrow_time
          where a.order_id = '{}'
          and a.user_phone_backup is not null
          and a.user_phone_backup != ''
          group by 1
          order by 1 desc'''
     phonebk_is_relatone_phone_cnt = pd.read_sql_query(sql, conn)
     conn.dispose()
  

     # phonebk_is_relattwo_phone_cnt 备用手机号关联不到第二联系人值为0
     conn = create_engine('postgresql://BASIC$root:WCjak3$RQnf3ST@8.210.75.205:13343/test')
     sql = '''select a.order_id,
          uniq(case when b.order_id is not null then b.user_phone else null end) as phonebk_is_relattwo_phone_cnt
          from bengal_test.bd_dwd_order a left join bengal_test.bd_dwd_order b on a.user_phone_backup=b.user_relate_two and b.borrow_time<=a.borrow_time
          where a.order_id = '{}'
          and a.user_phone_backup is not null
          and a.user_phone_backup != ''
          group by 1
          order by 1 desc'''
     phonebk_is_relattwo_phone_cnt = pd.read_sql_query(sql, conn)
     conn.dispose()
 

     # phonebk_is_relatthree_phone_cnt 备用手机号关联不到第三联系人值为0
     conn = create_engine('postgresql://BASIC$root:WCjak3$RQnf3ST@8.210.75.205:13343/test')
     sql = '''select a.order_id,
          uniq(case when b.order_id is not null then b.user_phone else null end) as phonebk_is_relatthree_phone_cnt
          from bengal_test.bd_dwd_order a left join (select c.*,d.phone_three as user_relate_three from bengal_test.bd_dwd_order c left join ctm_user d on c.order_id=d.order_id)b on a.user_phone_backup=b.user_relate_three and b.borrow_time<=a.borrow_time
          where a.order_id = '{}'
          and a.user_phone_backup is not null
          and a.user_phone_backup != ''
          group by 1
          order by 1 desc;'''.format(order_id)
     phonebk_is_relatthree_phone_cnt = pd.read_sql_query(sql, conn)
     conn.dispose()
 

     # account_is_phone_cnt 取款账号关联不到手机号值为-1
     conn = create_engine('postgresql://BASIC$root:WCjak3$RQnf3ST@8.210.75.205:13343/test')
     sql = '''select a.order_id,
          uniq(case when b.user_phone != a.user_phone then b.user_phone else null end) as account_is_phone_cnt
          from bengal_test.bd_dwd_order a left join bengal_test.bd_dwd_order b on a.user_bank_account=b.user_phone and b.borrow_time<=a.borrow_time
          where a.order_id = '{}'
          and a.user_bank_account is not null
          and b.order_id is not null
          group by 1
          order by 1 desc;'''.format(order_id)
     account_is_phone_cnt = pd.read_sql_query(sql, conn)
     conn.dispose()
    

     # account_is_phone_cardid_cnt 取款账号关联不到手机号值为-1
     conn = create_engine('postgresql://BASIC$root:WCjak3$RQnf3ST@8.210.75.205:13343/test')
     sql = '''select a.order_id,
          uniq(case when b.user_card_id != a.user_card_id then b.user_card_id else null end) as account_is_phone_cardid_cnt
          from bengal_test.bd_dwd_order a left join bengal_test.bd_dwd_order b on a.user_bank_account=b.user_phone and b.borrow_time<=a.borrow_time
          where a.order_id = '{}'
          and a.user_bank_account is not null
          and b.order_id is not null
          group by 1
          order by 1 desc;'''.format(order_id)
     account_is_phone_cardid_cnt = pd.read_sql_query(sql, conn)
     conn.dispose()
 

     # account_is_phone_device_cnt 取款账号关联不到手机号值为-1
     conn = create_engine('postgresql://BASIC$root:WCjak3$RQnf3ST@8.210.75.205:13343/test')
     sql = '''select a.order_id,
          count(distinct case when a.user_device_id is not null and a.user_device_id != b.user_device_id and b.user_device_id ~* '(\w|\d){{8}}-(\w|\d){{4}}-(\w|\d){{4}}-(\w|\d){{4}}-(\w|\d){{12}}' and b.user_device_id not in ('','00000000-0000-0000-0000-000000000000','f54bed004c6896417635db54baeac499','e4944f2e-1114-48d0-a93e-c943bad10221','bbd64611-c2cb-4726-87fa-9e3f8149a25e') then b.user_device_id 
          when a.user_device_id is null and b.user_device_id ~* '(\w|\d){{8}}-(\w|\d){{4}}-(\w|\d){{4}}-(\w|\d){{4}}-(\w|\d){{12}}' and b.user_device_id not in ('','00000000-0000-0000-0000-000000000000','f54bed004c6896417635db54baeac499','e4944f2e-1114-48d0-a93e-c943bad10221','bbd64611-c2cb-4726-87fa-9e3f8149a25e') then b.user_device_id
          else null end) as account_is_phone_device_cnt
          from bengal_test.bd_dwd_order a left join bengal_test.bd_dwd_order b on a.user_bank_account=b.user_phone and b.borrow_time<=a.borrow_time
          where a.order_id = '{}'
          and a.user_bank_account is not null
          and b.order_id is not null
          group by 1
          order by 1 desc;'''.format(order_id)
     account_is_phone_device_cnt = pd.read_sql_query(sql, conn)
     conn.dispose()
     fraud_df = pd.DataFrame({'order_id': [order_id]})  
     fraud_df = fraud_df \
     .merge(same_phone_diff_device_apply_cnt,how='left',on='order_id') \
     .merge(same_phone_diff_cardid_apply_cnt,how='left',on='order_id') \
     .merge(same_phone_diff_account_apply_cnt,how='left',on='order_id') \
     .merge(same_device_diff_phone_apply_cnt,how='left',on='order_id') \
     .merge(same_device_diff_cardid_apply_cnt,how='left',on='order_id') \
     .merge(same_device_diff_account_apply_cnt,how='left',on='order_id') \
     .merge(same_cardid_diff_phone_apply_cnt,how='left',on='order_id') \
     .merge(same_cardid_diff_device_apply_cnt,how='left',on='order_id') \
     .merge(same_cardid_diff_account_apply_cnt,how='left',on='order_id') \
     .merge(same_account_diff_phone_apply_cnt,how='left',on='order_id') \
     .merge(same_account_diff_cardid_apply_cnt,how='left',on='order_id') \
     .merge(same_account_diff_device_apply_cnt,how='left',on='order_id') \
     .merge(same_name_diff_phone_apply_cnt,how='left',on='order_id') \
     .merge(same_name_diff_cardid_apply_cnt,how='left',on='order_id') \
     .merge(same_name_diff_device_apply_cnt,how='left',on='order_id') \
     .merge(same_name_diff_account_apply_cnt,how='left',on='order_id') \
     .merge(same_relatone_diff_phone_apply_cnt,how='left',on='order_id') \
     .merge(same_relatone_diff_cardid_apply_cnt,how='left',on='order_id') \
     .merge(same_relatone_diff_device_apply_cnt,how='left',on='order_id') \
     .merge(same_relatone_diff_account_apply_cnt,how='left',on='order_id') \
     .merge(same_relattwo_diff_phone_apply_cnt,how='left',on='order_id') \
     .merge(same_relattwo_diff_cardid_apply_cnt,how='left',on='order_id') \
     .merge(same_relattwo_diff_device_apply_cnt,how='left',on='order_id') \
     .merge(same_relattwo_diff_account_apply_cnt,how='left',on='order_id') \
     .merge(relatone_is_relattwo_phone_cnt,how='left',on='order_id') \
     .merge(relatone_is_relatthree_phone_cnt,how='left',on='order_id') \
     .merge(phonebk_is_relatone_phone_cnt,how='left',on='order_id') \
     .merge(phonebk_is_relattwo_phone_cnt,how='left',on='order_id') \
     .merge(phonebk_is_relatthree_phone_cnt,how='left',on='order_id') \
     .merge(account_is_phone_cnt,how='left',on='order_id') \
     .merge(account_is_phone_cardid_cnt,how='left',on='order_id') \
     .merge(account_is_phone_device_cnt,how='left',on='order_id') 



     fraud_df['same_phone_diff_device_apply_cnt'] = fraud_df['same_phone_diff_device_apply_cnt'].fillna(-1)
     fraud_df['same_phone_diff_cardid_apply_cnt'] = fraud_df['same_phone_diff_cardid_apply_cnt'].fillna(-1)
     fraud_df['same_phone_diff_account_apply_cnt'] = fraud_df['same_phone_diff_account_apply_cnt'].fillna(-1)

     fraud_df['same_device_diff_phone_apply_cnt'] = fraud_df['same_device_diff_phone_apply_cnt'].fillna(-1)
     fraud_df['same_device_diff_cardid_apply_cnt'] = fraud_df['same_device_diff_cardid_apply_cnt'].fillna(-1)
     fraud_df['same_device_diff_account_apply_cnt'] = fraud_df['same_device_diff_account_apply_cnt'].fillna(-1)
     fraud_df['same_device_diff_account_d7_apply_cnt'] = fraud_df['same_device_diff_account_d7_apply_cnt'].fillna(-1)
     fraud_df['same_device_diff_account_d14_apply_cnt'] = fraud_df['same_device_diff_account_d14_apply_cnt'].fillna(-1)
     fraud_df['same_device_diff_account_d30_apply_cnt'] = fraud_df['same_device_diff_account_d30_apply_cnt'].fillna(-1)

     fraud_df['same_cardid_diff_phone_apply_cnt'] = fraud_df['same_cardid_diff_phone_apply_cnt'].fillna(-1)
     fraud_df['same_cardid_diff_device_apply_cnt'] = fraud_df['same_cardid_diff_device_apply_cnt'].fillna(-1)
     fraud_df['same_cardid_diff_account_apply_cnt'] = fraud_df['same_cardid_diff_account_apply_cnt'].fillna(-1)

     fraud_df['same_account_diff_phone_apply_cnt'] = fraud_df['same_account_diff_phone_apply_cnt'].fillna(-1)
     fraud_df['same_account_diff_cardid_apply_cnt'] = fraud_df['same_account_diff_cardid_apply_cnt'].fillna(-1)
     fraud_df['same_account_diff_device_apply_cnt'] = fraud_df['same_account_diff_device_apply_cnt'].fillna(-1)
     fraud_df['same_account_diff_device_d7_apply_cnt'] = fraud_df['same_account_diff_device_d7_apply_cnt'].fillna(-1)
     fraud_df['same_account_diff_device_d14_apply_cnt'] = fraud_df['same_account_diff_device_d14_apply_cnt'].fillna(-1)
     fraud_df['same_account_diff_device_d30_apply_cnt'] = fraud_df['same_account_diff_device_d30_apply_cnt'].fillna(-1)

     fraud_df['same_name_diff_phone_apply_cnt'] = fraud_df['same_name_diff_phone_apply_cnt'].fillna(-1)
     fraud_df['same_name_diff_cardid_apply_cnt'] = fraud_df['same_name_diff_cardid_apply_cnt'].fillna(-1)
     fraud_df['same_name_diff_device_apply_cnt'] = fraud_df['same_name_diff_device_apply_cnt'].fillna(-1)
     fraud_df['same_name_diff_account_apply_cnt'] = fraud_df['same_name_diff_account_apply_cnt'].fillna(-1)

     fraud_df['same_relatone_diff_phone_apply_cnt'] = fraud_df['same_relatone_diff_phone_apply_cnt'].fillna(-1)
     fraud_df['same_relatone_diff_cardid_apply_cnt'] = fraud_df['same_relatone_diff_cardid_apply_cnt'].fillna(-1)
     fraud_df['same_relatone_diff_device_apply_cnt'] = fraud_df['same_relatone_diff_device_apply_cnt'].fillna(-1)
     fraud_df['same_relatone_diff_account_apply_cnt'] = fraud_df['same_relatone_diff_account_apply_cnt'].fillna(-1)

     fraud_df['same_relattwo_diff_phone_apply_cnt'] = fraud_df['same_relattwo_diff_phone_apply_cnt'].fillna(-1)
     fraud_df['same_relattwo_diff_cardid_apply_cnt'] = fraud_df['same_relattwo_diff_cardid_apply_cnt'].fillna(-1)
     fraud_df['same_relattwo_diff_device_apply_cnt'] = fraud_df['same_relattwo_diff_device_apply_cnt'].fillna(-1)
     fraud_df['same_relattwo_diff_account_apply_cnt'] = fraud_df['same_relattwo_diff_account_apply_cnt'].fillna(-1)

     fraud_df['relatone_is_relattwo_phone_cnt'] = fraud_df['relatone_is_relattwo_phone_cnt'].fillna(-1)
     fraud_df['relatone_is_relatthree_phone_cnt'] = fraud_df['relatone_is_relatthree_phone_cnt'].fillna(-1)

     fraud_df['phonebk_is_relatone_phone_cnt'] = fraud_df['phonebk_is_relatone_phone_cnt'].fillna(-1)
     fraud_df['phonebk_is_relattwo_phone_cnt'] = fraud_df['phonebk_is_relattwo_phone_cnt'].fillna(-1)
     fraud_df['phonebk_is_relatthree_phone_cnt'] = fraud_df['phonebk_is_relatthree_phone_cnt'].fillna(-1)

     fraud_df['account_is_phone_cnt'] = fraud_df['account_is_phone_cnt'].fillna(-1)
     fraud_df['account_is_phone_cardid_cnt'] = fraud_df['account_is_phone_cardid_cnt'].fillna(-1)
     fraud_df['account_is_phone_device_cnt'] = fraud_df['account_is_phone_device_cnt'].fillna(-1)
     return fraud_df
