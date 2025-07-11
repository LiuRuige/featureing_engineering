import pandas as pd
import numpy as np

import math
# import scorecardpy as sc 
import pandas as pd
from sqlalchemy import create_engine
import time
import numpy as np
import datetime
from dateutil.relativedelta import relativedelta
import os, json 
from sqlalchemy import create_engine
from openpyxl import Workbook
from openpyxl.utils import get_column_letter
import pymysql
import pytz,re
import traceback

def sms_variable(order_id):
    # XJD  贷款现金存款  loan|ঋণ|লোন|ক্যাশ|নগদ|ক্রেডিট|cash|bill|ত ক্যাশব্যা|নগদে|কাশ|bonus|fee|deposit|Taka|credit|withdraw|পোজিটে|money|transfer|transact|repayment|paid|debited|পরিশোধিত না|Installment|
    # Bank 银行  bank|visa|card|bKash|ATM|
    # OTP    otp|Verification Code|PIN|One-Time Password|re-activation code|
    # due    due|

    # account  account| 
    # pay   পরিশোধ করতে|paid|payment|wallet|
    # currency   bdt|tk\\.?|টাকা|
#    补充  seen read 已读已看 

    xjd_words = r'loan|l0an|cash|credit|kredit|money|สินเชื่อ|เงิน|เครดิต|peso|rupee|dong|Đồng|Vay|Tiền mặt|Tín dụng|ঋণ|লোন|ক্যাশ|নগদ|ক্রেডিট|préstamo|prestamo|crédito|dinero'
    due_words = r'\bdue\b|ครบกำหนด|วันกำหนดที่ต้องชำระหนี้|หนดชำระ|đến hạn|pendiente|fecha|vencimiento'
    due_exclude_words = r'due to'
    co_due_deuda=r'deuda'
    co_due_caduca=r'caduca'
    overdue_words = r'overdue|unpaid|penalty|เกินกำหนดเวลา|ค้างชำระ|เกินกำหนด|ยังไม่ชำระ|quá hạn|chưa thanh toán|phạt|ওভারডিউ'
    co_overdue_deuda=r'deuda'
    co_overdue_atrasado=r'atrasado'
    otp_words = r'otp|one time pin|one time code|one time password|รหัสยืนยัน|ভেরিফিকেশন কোড|clave temporal|codigo temporal|código de verificación|codigo de verificacion|pin temporal'
    bank_words = r'bank|ธนาคาร|Ngân hàng|ব্যাংক|banco'
    bankphone_words = r'pnb|scb|sbi|icici|hdfc|citi|nagad|bank|bnk|bk'
    currency_1 = r'(p|thb|rs\.?|rs\:?|inr\.?|inr\:?|inr dr\.?|vnd|bdt|tk\.?|$)\s?\d+'
    currency_2 = r'\d+\s?(php|thb|บาท|฿|rs|inr|₫|vnd|bdt|cop|pesos)'
    payment_words = r'pay|wallet|gcash|true money|scb easy|g wallet|phonepe|upi|พร้อมเพย์|กระเป๋าเงิน|กระเป๋าสตางค์|thanh toán|Ví|momo|পরিশোধ|pagar|billetera'
    blacklist_words = r'blacklist|รายชื่อดำ|บัญชีดำ|Danh sách đen|কালো তালিকা|lista negra'
    account_words = r'a\/c|account|บัญชี|tkhoản|হিসাবটি|cuenta'
    casino_words= r'casino|rummy|เล่นการพนัน|คาสิโน|รัมมี่|Sòng bạc|cờ bạc'
    gcash_words = r'gcash'
    whatsapp_words = r'whatsapp'
    
    conn = create_engine('postgresql://BASIC$readonly:ZSJ7P2sxJop2L4@8.210.75.205:13343/ph_ods')
    sql = '''select 
            order_id, borrow_time, created_time, content, type, phone,read,seen 
            from user_sms    
            where order_id = '{}' and  seen != '2';'''.format(order_id)
    sms = pd.read_sql_query(sql,conn)
    
    sms['borrow_time'] = pd.to_datetime(sms['borrow_time'], errors='coerce')
    sms['created_time'] = pd.to_datetime(sms['created_time'], errors='coerce')
    
    sms_cnt= sms['content'].count()
    sms_d0_cnt = sms.loc[sms.borrow_time.dt.date == sms.created_time.dt.date]['content'].count()
    sms_d3_cnt = sms.loc[sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(3,'D')]['content'].count()
    sms_d7_cnt = sms.loc[sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(7,'D')]['content'].count()
    sms_d14_cnt = sms.loc[sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(14,'D')]['content'].count()
    sms_d30_cnt = sms.loc[sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(30,'D')]['content'].count()
    sms_d60_cnt = sms.loc[sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(60,'D')]['content'].count()
    
    sms_in_cnt= sms.loc[sms.type==1]['content'].count()
    sms_in_d0_cnt = sms.loc[(sms.borrow_time.dt.date == sms.created_time.dt.date) & (sms.type==1)]['content'].count()
    sms_in_d3_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(3,'D')) & (sms.type==1)]['content'].count()
    sms_in_d7_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(7,'D')) & (sms.type==1)]['content'].count()
    sms_in_d14_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(14,'D')) & (sms.type==1)]['content'].count()
    sms_in_d30_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(30,'D')) & (sms.type==1)]['content'].count()
    sms_in_d60_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(60,'D')) & (sms.type==1)]['content'].count()
    
    sms_out_cnt= sms.loc[sms.type==2]['content'].count()
    sms_out_d0_cnt = sms.loc[(sms.borrow_time.dt.date == sms.created_time.dt.date) & (sms.type==2)]['content'].count()
    sms_out_d3_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(3,'D')) & (sms.type==2)]['content'].count()
    sms_out_d7_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(7,'D')) & (sms.type==2)]['content'].count()
    sms_out_d14_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(14,'D')) & (sms.type==2)]['content'].count()
    sms_out_d30_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(30,'D')) & (sms.type==2)]['content'].count()
    sms_out_d60_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(60,'D')) & (sms.type==2)]['content'].count()
    
    sms_in_xjd_cnt= sms.loc[(sms.type==1) & (sms.content.str.contains(xjd_words,case= False,na= False))]['content'].count()
    sms_in_d0_xjd_cnt = sms.loc[(sms.borrow_time.dt.date == sms.created_time.dt.date) & (sms.type==1) & (sms.content.str.contains(xjd_words,case= False,na= False))]['content'].count()
    sms_in_d3_xjd_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(3,'D')) & (sms.type==1) & (sms.content.str.contains(xjd_words,case= False,na= False))]['content'].count()
    sms_in_d7_xjd_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(7,'D')) & (sms.type==1) & (sms.content.str.contains(xjd_words,case= False,na= False))]['content'].count()
    sms_in_d14_xjd_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(14,'D')) & (sms.type==1) & (sms.content.str.contains(xjd_words,case= False,na= False))]['content'].count()
    sms_in_d30_xjd_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(30,'D')) & (sms.type==1) & (sms.content.str.contains(xjd_words,case= False,na= False))]['content'].count()
    sms_in_d60_xjd_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(60,'D')) & (sms.type==1) & (sms.content.str.contains(xjd_words,case= False,na= False))]['content'].count()
    
    sms_out_xjd_cnt= sms.loc[(sms.type==2) & (sms.content.str.contains(xjd_words,case= False,na= False))]['content'].count()
    sms_out_d0_xjd_cnt = sms.loc[(sms.borrow_time.dt.date == sms.created_time.dt.date) & (sms.type==2) & (sms.content.str.contains(xjd_words,case= False,na= False))]['content'].count()
    sms_out_d3_xjd_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(3,'D')) & (sms.type==2) & (sms.content.str.contains(xjd_words,case= False,na= False))]['content'].count()
    sms_out_d7_xjd_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(7,'D')) & (sms.type==2) & (sms.content.str.contains(xjd_words,case= False,na= False))]['content'].count()
    sms_out_d14_xjd_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(14,'D')) & (sms.type==2) & (sms.content.str.contains(xjd_words,case= False,na= False))]['content'].count()
    sms_out_d30_xjd_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(30,'D')) & (sms.type==2) & (sms.content.str.contains(xjd_words,case= False,na= False))]['content'].count()
    sms_out_d60_xjd_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(60,'D')) & (sms.type==2) & (sms.content.str.contains(xjd_words,case= False,na= False))]['content'].count()
    
    sms_in_due_cnt= sms.loc[(sms.type==1) & ((sms.content.str.contains(due_words,case= False,na= False)) | ((sms.content.str.contains(co_due_deuda,case= False,na= False)) & (sms.content.str.contains(co_due_caduca,case= False,na= False)))) & (~sms.content.str.contains(due_exclude_words,case= False,na= False)) & (~sms.content.str.contains(overdue_words,case= False,na= False))]['content'].count()
    sms_in_d0_due_cnt = sms.loc[(sms.borrow_time.dt.date == sms.created_time.dt.date) & (sms.type==1) & ((sms.content.str.contains(due_words,case= False,na= False)) | ((sms.content.str.contains(co_due_deuda,case= False,na= False)) & (sms.content.str.contains(co_due_caduca,case= False,na= False)))) & (~sms.content.str.contains(due_exclude_words,case= False,na= False)) & (~sms.content.str.contains(overdue_words,case= False,na= False))]['content'].count()
    sms_in_d3_due_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(3,'D')) & (sms.type==1) & ((sms.content.str.contains(due_words,case= False,na= False)) | ((sms.content.str.contains(co_due_deuda,case= False,na= False)) & (sms.content.str.contains(co_due_caduca,case= False,na= False)))) & (~sms.content.str.contains(due_exclude_words,case= False,na= False)) & (~sms.content.str.contains(overdue_words,case= False,na= False))]['content'].count()
    sms_in_d7_due_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(7,'D')) & (sms.type==1) & ((sms.content.str.contains(due_words,case= False,na= False)) | ((sms.content.str.contains(co_due_deuda,case= False,na= False)) & (sms.content.str.contains(co_due_caduca,case= False,na= False)))) & (~sms.content.str.contains(due_exclude_words,case= False,na= False)) & (~sms.content.str.contains(overdue_words,case= False,na= False))]['content'].count()
    sms_in_d14_due_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(14,'D')) & (sms.type==1) & ((sms.content.str.contains(due_words,case= False,na= False)) | ((sms.content.str.contains(co_due_deuda,case= False,na= False)) & (sms.content.str.contains(co_due_caduca,case= False,na= False)))) & (~sms.content.str.contains(due_exclude_words,case= False,na= False)) & (~sms.content.str.contains(overdue_words,case= False,na= False))]['content'].count()
    sms_in_d30_due_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(30,'D')) & (sms.type==1) & ((sms.content.str.contains(due_words,case= False,na= False)) | ((sms.content.str.contains(co_due_deuda,case= False,na= False)) & (sms.content.str.contains(co_due_caduca,case= False,na= False)))) & (~sms.content.str.contains(due_exclude_words,case= False,na= False)) & (~sms.content.str.contains(overdue_words,case= False,na= False))]['content'].count()
    sms_in_d60_due_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(60,'D')) & (sms.type==1) & ((sms.content.str.contains(due_words,case= False,na= False)) | ((sms.content.str.contains(co_due_deuda,case= False,na= False)) & (sms.content.str.contains(co_due_caduca,case= False,na= False)))) & (~sms.content.str.contains(due_exclude_words,case= False,na= False)) & (~sms.content.str.contains(overdue_words,case= False,na= False))]['content'].count()

    sms_in_overdue_cnt= sms.loc[(sms.type==1) & ((sms.content.str.contains(overdue_words,case= False,na= False)) | ((sms.content.str.contains(co_overdue_deuda,case= False,na= False)) & (sms.content.str.contains(co_overdue_atrasado,case= False,na= False))))]['content'].count()
    sms_in_d0_overdue_cnt = sms.loc[(sms.borrow_time.dt.date == sms.created_time.dt.date) & (sms.type==1) & ((sms.content.str.contains(overdue_words,case= False,na= False)) | ((sms.content.str.contains(co_overdue_deuda,case= False,na= False)) & (sms.content.str.contains(co_overdue_atrasado,case= False,na= False))))]['content'].count()
    sms_in_d3_overdue_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(3,'D')) & (sms.type==1) & ((sms.content.str.contains(overdue_words,case= False,na= False)) | ((sms.content.str.contains(co_overdue_deuda,case= False,na= False)) & (sms.content.str.contains(co_overdue_atrasado,case= False,na= False))))]['content'].count()
    sms_in_d7_overdue_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(7,'D')) & (sms.type==1) & ((sms.content.str.contains(overdue_words,case= False,na= False)) | ((sms.content.str.contains(co_overdue_deuda,case= False,na= False)) & (sms.content.str.contains(co_overdue_atrasado,case= False,na= False))))]['content'].count()
    sms_in_d14_overdue_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(14,'D')) & (sms.type==1) & ((sms.content.str.contains(overdue_words,case= False,na= False)) | ((sms.content.str.contains(co_overdue_deuda,case= False,na= False)) & (sms.content.str.contains(co_overdue_atrasado,case= False,na= False))))]['content'].count()
    sms_in_d30_overdue_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(30,'D')) & (sms.type==1) & ((sms.content.str.contains(overdue_words,case= False,na= False)) | ((sms.content.str.contains(co_overdue_deuda,case= False,na= False)) & (sms.content.str.contains(co_overdue_atrasado,case= False,na= False))))]['content'].count()
    sms_in_d60_overdue_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(60,'D')) & (sms.type==1) & ((sms.content.str.contains(overdue_words,case= False,na= False)) | ((sms.content.str.contains(co_overdue_deuda,case= False,na= False)) & (sms.content.str.contains(co_overdue_atrasado,case= False,na= False))))]['content'].count()

    sms_in_otp_cnt= sms.loc[(sms.type==1) & (sms.content.str.contains(otp_words,case= False,na= False))]['content'].count()
    sms_in_d0_otp_cnt = sms.loc[(sms.borrow_time.dt.date == sms.created_time.dt.date) & (sms.type==1) & (sms.content.str.contains(otp_words,case= False,na= False))]['content'].count()
    sms_in_d3_otp_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(3,'D')) & (sms.type==1) & (sms.content.str.contains(otp_words,case= False,na= False))]['content'].count()
    sms_in_d7_otp_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(7,'D')) & (sms.type==1) & (sms.content.str.contains(otp_words,case= False,na= False))]['content'].count()
    sms_in_d14_otp_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(14,'D')) & (sms.type==1) & (sms.content.str.contains(otp_words,case= False,na= False))]['content'].count()
    sms_in_d30_otp_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(30,'D')) & (sms.type==1) & (sms.content.str.contains(otp_words,case= False,na= False))]['content'].count()
    sms_in_d60_otp_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(60,'D')) & (sms.type==1) & (sms.content.str.contains(otp_words,case= False,na= False))]['content'].count()

    sms_in_bank_cnt= sms.loc[(sms.type==1) & ((sms.content.str.contains(bank_words,case= False,na= False)) | (sms.phone.str.contains(bankphone_words,case= False,na= False)))]['content'].count()
    sms_in_d0_bank_cnt = sms.loc[(sms.borrow_time.dt.date == sms.created_time.dt.date) & (sms.type==1) & ((sms.content.str.contains(bank_words,case= False,na= False)) | (sms.phone.str.contains(bankphone_words,case= False,na= False)))]['content'].count()
    sms_in_d3_bank_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(3,'D')) & (sms.type==1) & ((sms.content.str.contains(bank_words,case= False,na= False)) | (sms.phone.str.contains(bankphone_words,case= False,na= False)))]['content'].count()
    sms_in_d7_bank_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(7,'D')) & (sms.type==1) & ((sms.content.str.contains(bank_words,case= False,na= False)) | (sms.phone.str.contains(bankphone_words,case= False,na= False)))]['content'].count()
    sms_in_d14_bank_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(14,'D')) & (sms.type==1) & ((sms.content.str.contains(bank_words,case= False,na= False)) | (sms.phone.str.contains(bankphone_words,case= False,na= False)))]['content'].count()
    sms_in_d30_bank_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(30,'D')) & (sms.type==1) & ((sms.content.str.contains(bank_words,case= False,na= False)) | (sms.phone.str.contains(bankphone_words,case= False,na= False)))]['content'].count()
    sms_in_d60_bank_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(60,'D')) & (sms.type==1) & ((sms.content.str.contains(bank_words,case= False,na= False)) | (sms.phone.str.contains(bankphone_words,case= False,na= False)))]['content'].count()

    sms_in_currency_cnt= sms.loc[(sms.type==1) & ((sms.content.str.contains(currency_1,case= False,na= False)) | (sms.content.str.contains(currency_2,case= False,na= False)))]['content'].count()
    sms_in_d0_currency_cnt = sms.loc[(sms.borrow_time.dt.date == sms.created_time.dt.date) & (sms.type==1) & ((sms.content.str.contains(currency_1,case= False,na= False)) | (sms.content.str.contains(currency_2,case= False,na= False)))]['content'].count()
    sms_in_d3_currency_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(3,'D')) & (sms.type==1) & ((sms.content.str.contains(currency_1,case= False,na= False)) | (sms.content.str.contains(currency_2,case= False,na= False)))]['content'].count()
    sms_in_d7_currency_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(7,'D')) & (sms.type==1) & ((sms.content.str.contains(currency_1,case= False,na= False)) | (sms.content.str.contains(currency_2,case= False,na= False)))]['content'].count()
    sms_in_d14_currency_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(14,'D')) & (sms.type==1) & ((sms.content.str.contains(currency_1,case= False,na= False)) | (sms.content.str.contains(currency_2,case= False,na= False)))]['content'].count()
    sms_in_d30_currency_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(30,'D')) & (sms.type==1) & ((sms.content.str.contains(currency_1,case= False,na= False)) | (sms.content.str.contains(currency_2,case= False,na= False)))]['content'].count()
    sms_in_d60_currency_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(60,'D')) & (sms.type==1) & ((sms.content.str.contains(currency_1,case= False,na= False)) | (sms.content.str.contains(currency_2,case= False,na= False)))]['content'].count()

    sms_in_payment_cnt= sms.loc[(sms.type==1) & (sms.content.str.contains(payment_words,case= False,na= False))]['content'].count()
    sms_in_d0_payment_cnt = sms.loc[(sms.borrow_time.dt.date == sms.created_time.dt.date) & (sms.type==1) & (sms.content.str.contains(payment_words,case= False,na= False))]['content'].count()
    sms_in_d3_payment_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(3,'D')) & (sms.type==1) & (sms.content.str.contains(payment_words,case= False,na= False))]['content'].count()
    sms_in_d7_payment_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(7,'D')) & (sms.type==1) & (sms.content.str.contains(payment_words,case= False,na= False))]['content'].count()
    sms_in_d14_payment_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(14,'D')) & (sms.type==1) & (sms.content.str.contains(payment_words,case= False,na= False))]['content'].count()
    sms_in_d30_payment_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(30,'D')) & (sms.type==1) & (sms.content.str.contains(payment_words,case= False,na= False))]['content'].count()
    sms_in_d60_payment_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(60,'D')) & (sms.type==1) & (sms.content.str.contains(payment_words,case= False,na= False))]['content'].count()

    sms_in_blacklist_cnt= sms.loc[(sms.type==1) & (sms.content.str.contains(blacklist_words,case= False,na= False))]['content'].count()
    sms_in_d0_blacklist_cnt = sms.loc[(sms.borrow_time.dt.date == sms.created_time.dt.date) & (sms.type==1) & (sms.content.str.contains(blacklist_words,case= False,na= False))]['content'].count()
    sms_in_d3_blacklist_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(3,'D')) & (sms.type==1) & (sms.content.str.contains(blacklist_words,case= False,na= False))]['content'].count()
    sms_in_d7_blacklist_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(7,'D')) & (sms.type==1) & (sms.content.str.contains(blacklist_words,case= False,na= False))]['content'].count()
    sms_in_d14_blacklist_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(14,'D')) & (sms.type==1) & (sms.content.str.contains(blacklist_words,case= False,na= False))]['content'].count()
    sms_in_d30_blacklist_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(30,'D')) & (sms.type==1) & (sms.content.str.contains(blacklist_words,case= False,na= False))]['content'].count()
    sms_in_d60_blacklist_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(60,'D')) & (sms.type==1) & (sms.content.str.contains(blacklist_words,case= False,na= False))]['content'].count()

    sms_in_account_cnt= sms.loc[(sms.type==1) & (sms.content.str.contains(account_words,case= False,na= False))]['content'].count()
    sms_in_d0_account_cnt = sms.loc[(sms.borrow_time.dt.date == sms.created_time.dt.date) & (sms.type==1) & (sms.content.str.contains(account_words,case= False,na= False))]['content'].count()
    sms_in_d3_account_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(3,'D')) & (sms.type==1) & (sms.content.str.contains(account_words,case= False,na= False))]['content'].count()
    sms_in_d7_account_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(7,'D')) & (sms.type==1) & (sms.content.str.contains(account_words,case= False,na= False))]['content'].count()
    sms_in_d14_account_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(14,'D')) & (sms.type==1) & (sms.content.str.contains(account_words,case= False,na= False))]['content'].count()
    sms_in_d30_account_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(30,'D')) & (sms.type==1) & (sms.content.str.contains(account_words,case= False,na= False))]['content'].count()
    sms_in_d60_account_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(60,'D')) & (sms.type==1) & (sms.content.str.contains(account_words,case= False,na= False))]['content'].count()

    sms_in_casino_cnt= sms.loc[(sms.type==1) & (sms.content.str.contains(casino_words,case= False,na= False))]['content'].count()
    sms_in_d0_casino_cnt = sms.loc[(sms.borrow_time.dt.date == sms.created_time.dt.date) & (sms.type==1) & (sms.content.str.contains(casino_words,case= False,na= False))]['content'].count()
    sms_in_d3_casino_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(3,'D')) & (sms.type==1) & (sms.content.str.contains(casino_words,case= False,na= False))]['content'].count()
    sms_in_d7_casino_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(7,'D')) & (sms.type==1) & (sms.content.str.contains(casino_words,case= False,na= False))]['content'].count()
    sms_in_d14_casino_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(14,'D')) & (sms.type==1) & (sms.content.str.contains(casino_words,case= False,na= False))]['content'].count()
    sms_in_d30_casino_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(30,'D')) & (sms.type==1) & (sms.content.str.contains(casino_words,case= False,na= False))]['content'].count()
    sms_in_d60_casino_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(60,'D')) & (sms.type==1) & (sms.content.str.contains(casino_words,case= False,na= False))]['content'].count()

    sms_in_gcash_cnt= sms.loc[(sms.type==1) & (sms.content.str.contains(gcash_words,case= False,na= False))]['content'].count()
    sms_in_d0_gcash_cnt = sms.loc[(sms.borrow_time.dt.date == sms.created_time.dt.date) & (sms.type==1) & (sms.content.str.contains(gcash_words,case= False,na= False))]['content'].count()
    sms_in_d3_gcash_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(3,'D')) & (sms.type==1) & (sms.content.str.contains(gcash_words,case= False,na= False))]['content'].count()
    sms_in_d7_gcash_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(7,'D')) & (sms.type==1) & (sms.content.str.contains(gcash_words,case= False,na= False))]['content'].count()
    sms_in_d14_gcash_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(14,'D')) & (sms.type==1) & (sms.content.str.contains(gcash_words,case= False,na= False))]['content'].count()
    sms_in_d30_gcash_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(30,'D')) & (sms.type==1) & (sms.content.str.contains(gcash_words,case= False,na= False))]['content'].count()
    sms_in_d60_gcash_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(60,'D')) & (sms.type==1) & (sms.content.str.contains(gcash_words,case= False,na= False))]['content'].count()

    sms_in_whatsapp_cnt= sms.loc[(sms.type==1) & (sms.content.str.contains(whatsapp_words,case= False,na= False))]['content'].count()

    sms_unique_phone_cnt= sms['phone'].nunique()
    
    sms_in_unique_phone_cnt = sms.loc[sms.type == 1]['phone'].nunique()
    sms_in_d0_unique_phone_cnt = sms.loc[(sms.type == 1)& (sms.borrow_time.dt.date == sms.created_time.dt.date)]['phone'].nunique()
    sms_in_d3_unique_phone_cnt = sms.loc[(sms.type == 1)&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(3,'D'))]['phone'].nunique()
    sms_in_d7_unique_phone_cnt = sms.loc[(sms.type == 1)&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(7,'D'))]['phone'].nunique()
    sms_in_d14_unique_phone_cnt = sms.loc[(sms.type == 1)&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(14,'D'))]['phone'].nunique()
    sms_in_d30_unique_phone_cnt = sms.loc[(sms.type == 1)&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(30,'D'))]['phone'].nunique()
    sms_in_d60_unique_phone_cnt = sms.loc[(sms.type == 1)&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(60,'D'))]['phone'].nunique()

    sms_out_unique_phone_cnt = sms.loc[sms.type == 2]['phone'].nunique()
    sms_out_d0_unique_phone_cnt = sms.loc[(sms.type == 2)& (sms.borrow_time.dt.date == sms.created_time.dt.date)]['phone'].nunique()
    sms_out_d3_unique_phone_cnt = sms.loc[(sms.type == 2)&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(3,'D'))]['phone'].nunique()
    sms_out_d7_unique_phone_cnt = sms.loc[(sms.type == 2)&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(7,'D'))]['phone'].nunique()
    sms_out_d14_unique_phone_cnt = sms.loc[(sms.type == 2)&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(14,'D'))]['phone'].nunique()
    sms_out_d30_unique_phone_cnt = sms.loc[(sms.type == 2)&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(30,'D'))]['phone'].nunique()
    sms_out_d60_unique_phone_cnt = sms.loc[(sms.type == 2)&(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(60,'D'))]['phone'].nunique()

    sms_in_xjd_unique_phone_cnt= sms.loc[(sms.type==1) & (sms.content.str.contains(xjd_words,case= False,na= False))]['phone'].nunique()
    sms_in_d0_xjd_unique_phone_cnt = sms.loc[(sms.borrow_time.dt.date == sms.created_time.dt.date) & (sms.type==1) & (sms.content.str.contains(xjd_words,case= False,na= False))]['phone'].nunique()
    sms_in_d3_xjd_unique_phone_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(3,'D')) & (sms.type==1) & (sms.content.str.contains(xjd_words,case= False,na= False))]['phone'].nunique()
    sms_in_d7_xjd_unique_phone_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(7,'D')) & (sms.type==1) & (sms.content.str.contains(xjd_words,case= False,na= False))]['phone'].nunique()
    sms_in_d14_xjd_unique_phone_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(14,'D')) & (sms.type==1) & (sms.content.str.contains(xjd_words,case= False,na= False))]['phone'].nunique()
    sms_in_d30_xjd_unique_phone_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(30,'D')) & (sms.type==1) & (sms.content.str.contains(xjd_words,case= False,na= False))]['phone'].nunique()
    sms_in_d60_xjd_unique_phone_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(60,'D')) & (sms.type==1) & (sms.content.str.contains(xjd_words,case= False,na= False))]['phone'].nunique()
    sms_out_xjd_unique_phone_cnt= sms.loc[(sms.type==2) & (sms.content.str.contains(xjd_words,case= False,na= False))]['phone'].nunique()
   
    sms_in_due_unique_phone_cnt= sms.loc[(sms.type==1) & ((sms.content.str.contains(due_words,case= False,na= False)) | ((sms.content.str.contains(co_due_deuda,case= False,na= False)) & (sms.content.str.contains(co_due_caduca,case= False,na= False)))) & (~sms.content.str.contains(due_exclude_words,case= False,na= False)) & (~sms.content.str.contains(overdue_words,case= False,na= False))]['phone'].nunique()
    sms_in_d0_due_unique_phone_cnt = sms.loc[(sms.borrow_time.dt.date == sms.created_time.dt.date) & (sms.type==1) & ((sms.content.str.contains(due_words,case= False,na= False)) | ((sms.content.str.contains(co_due_deuda,case= False,na= False)) & (sms.content.str.contains(co_due_caduca,case= False,na= False)))) & (~sms.content.str.contains(due_exclude_words,case= False,na= False)) & (~sms.content.str.contains(overdue_words,case= False,na= False))]['phone'].nunique()
    sms_in_d3_due_unique_phone_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(3,'D')) & (sms.type==1) & ((sms.content.str.contains(due_words,case= False,na= False)) | ((sms.content.str.contains(co_due_deuda,case= False,na= False)) & (sms.content.str.contains(co_due_caduca,case= False,na= False)))) & (~sms.content.str.contains(due_exclude_words,case= False,na= False)) & (~sms.content.str.contains(overdue_words,case= False,na= False))]['phone'].nunique()
    sms_in_d7_due_unique_phone_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(7,'D')) & (sms.type==1) & ((sms.content.str.contains(due_words,case= False,na= False)) | ((sms.content.str.contains(co_due_deuda,case= False,na= False)) & (sms.content.str.contains(co_due_caduca,case= False,na= False)))) & (~sms.content.str.contains(due_exclude_words,case= False,na= False)) & (~sms.content.str.contains(overdue_words,case= False,na= False))]['phone'].nunique()
    sms_in_d14_due_unique_phone_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(14,'D')) & (sms.type==1) & ((sms.content.str.contains(due_words,case= False,na= False)) | ((sms.content.str.contains(co_due_deuda,case= False,na= False)) & (sms.content.str.contains(co_due_caduca,case= False,na= False)))) & (~sms.content.str.contains(due_exclude_words,case= False,na= False)) & (~sms.content.str.contains(overdue_words,case= False,na= False))]['phone'].nunique()
    sms_in_d30_due_unique_phone_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(30,'D')) & (sms.type==1) & ((sms.content.str.contains(due_words,case= False,na= False)) | ((sms.content.str.contains(co_due_deuda,case= False,na= False)) & (sms.content.str.contains(co_due_caduca,case= False,na= False)))) & (~sms.content.str.contains(due_exclude_words,case= False,na= False)) & (~sms.content.str.contains(overdue_words,case= False,na= False))]['phone'].nunique()
    sms_in_d60_due_unique_phone_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(60,'D')) & (sms.type==1) & ((sms.content.str.contains(due_words,case= False,na= False)) | ((sms.content.str.contains(co_due_deuda,case= False,na= False)) & (sms.content.str.contains(co_due_caduca,case= False,na= False)))) & (~sms.content.str.contains(due_exclude_words,case= False,na= False)) & (~sms.content.str.contains(overdue_words,case= False,na= False))]['phone'].nunique()
    
    sms_in_overdue_unique_phone_cnt= sms.loc[(sms.type==1) & ((sms.content.str.contains(overdue_words,case= False,na= False)) | ((sms.content.str.contains(co_overdue_deuda,case= False,na= False)) & (sms.content.str.contains(co_overdue_atrasado,case= False,na= False))))]['phone'].nunique()
    sms_in_d0_overdue_unique_phone_cnt = sms.loc[(sms.borrow_time.dt.date == sms.created_time.dt.date) & (sms.type==1) & ((sms.content.str.contains(overdue_words,case= False,na= False)) | ((sms.content.str.contains(co_overdue_deuda,case= False,na= False)) & (sms.content.str.contains(co_overdue_atrasado,case= False,na= False))))]['phone'].nunique()
    sms_in_d3_overdue_unique_phone_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(3,'D')) & (sms.type==1) & ((sms.content.str.contains(overdue_words,case= False,na= False)) | ((sms.content.str.contains(co_overdue_deuda,case= False,na= False)) & (sms.content.str.contains(co_overdue_atrasado,case= False,na= False))))]['phone'].nunique()
    sms_in_d7_overdue_unique_phone_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(7,'D')) & (sms.type==1) & ((sms.content.str.contains(overdue_words,case= False,na= False)) | ((sms.content.str.contains(co_overdue_deuda,case= False,na= False)) & (sms.content.str.contains(co_overdue_atrasado,case= False,na= False))))]['phone'].nunique()
    sms_in_d14_overdue_unique_phone_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(14,'D')) & (sms.type==1) & ((sms.content.str.contains(overdue_words,case= False,na= False)) | ((sms.content.str.contains(co_overdue_deuda,case= False,na= False)) & (sms.content.str.contains(co_overdue_atrasado,case= False,na= False))))]['phone'].nunique()
    sms_in_d30_overdue_unique_phone_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(30,'D')) & (sms.type==1) & ((sms.content.str.contains(overdue_words,case= False,na= False)) | ((sms.content.str.contains(co_overdue_deuda,case= False,na= False)) & (sms.content.str.contains(co_overdue_atrasado,case= False,na= False))))]['phone'].nunique()
    sms_in_d60_overdue_unique_phone_cnt = sms.loc[(sms.borrow_time.dt.date <= sms.created_time.dt.date + np.timedelta64(60,'D')) & (sms.type==1) & ((sms.content.str.contains(overdue_words,case= False,na= False)) | ((sms.content.str.contains(co_overdue_deuda,case= False,na= False)) & (sms.content.str.contains(co_overdue_atrasado,case= False,na= False))))]['phone'].nunique()
    
    sms_in_casino_unique_phone_cnt= sms.loc[(sms.type==1) & (sms.content.str.contains(casino_words,case= False,na= False))]['phone'].nunique()
    
    if sms.empty:
        sms_date_max_gap = sms_in_date_max_gap = sms_out_date_max_gap = sms_in_xjd_date_max_gap = sms_out_xjd_date_max_gap = sms_in_due_date_max_gap = sms_in_overdue_date_max_gap = -1 
        sms_date_min_gap = sms_in_date_min_gap = sms_out_date_min_gap = sms_in_xjd_date_min_gap = sms_out_xjd_date_min_gap = sms_in_due_date_min_gap = sms_in_overdue_date_min_gap = -1
        sms_daily_max = sms_d3_daily_max = sms_d7_daily_max = sms_d14_daily_max = sms_d30_daily_max = sms_d60_daily_max = -1
        sms_daily_min = sms_d3_daily_min = sms_d7_daily_min = sms_d14_daily_min = sms_d30_daily_min = sms_d60_daily_min = -1
        sms_daily_mean = sms_d3_daily_mean = sms_d7_daily_mean = sms_d14_daily_mean = sms_d30_daily_mean = sms_d60_daily_mean = -1
        sms_in_daily_max = sms_in_d3_daily_max = sms_in_d7_daily_max = sms_in_d14_daily_max = sms_in_d30_daily_max = sms_in_d60_daily_max = -1
        sms_in_daily_min = sms_in_d3_daily_min = sms_in_d7_daily_min = sms_in_d14_daily_min = sms_in_d30_daily_min = sms_in_d60_daily_min = -1
        sms_in_daily_mean = sms_in_d3_daily_mean = sms_in_d7_daily_mean = sms_in_d14_daily_mean = sms_in_d30_daily_mean = sms_in_d60_daily_mean = -1
        sms_out_daily_max = sms_out_d3_daily_max = sms_out_d7_daily_max = sms_out_d14_daily_max = sms_out_d30_daily_max = sms_out_d60_daily_max = -1
        sms_out_daily_min = sms_out_d3_daily_min = sms_out_d7_daily_min = sms_out_d14_daily_min = sms_out_d30_daily_min = sms_out_d60_daily_min = -1
        sms_out_daily_mean = sms_out_d3_daily_mean = sms_out_d7_daily_mean = sms_out_d14_daily_mean = sms_out_d30_daily_mean = sms_out_d60_daily_mean = -1
        sms_in_daily_xjd_max = sms_in_d3_daily_xjd_max = sms_in_d7_daily_xjd_max = sms_in_d14_daily_xjd_max = sms_in_d30_daily_xjd_max = sms_in_d60_daily_xjd_max = -1
        sms_in_daily_xjd_min = sms_in_d3_daily_xjd_min = sms_in_d7_daily_xjd_min = sms_in_d14_daily_xjd_min = sms_in_d30_daily_xjd_min = sms_in_d60_daily_xjd_min = -1
        sms_in_daily_xjd_mean = sms_in_d3_daily_xjd_mean = sms_in_d7_daily_xjd_mean = sms_in_d14_daily_xjd_mean = sms_in_d30_daily_xjd_mean = sms_in_d60_daily_xjd_mean = -1
        sms_in_daily_due_max = sms_in_d3_daily_due_max = sms_in_d7_daily_due_max = sms_in_d14_daily_due_max = sms_in_d30_daily_due_max = sms_in_d60_daily_due_max = -1
        sms_in_daily_due_min = sms_in_d3_daily_due_min = sms_in_d7_daily_due_min = sms_in_d14_daily_due_min = sms_in_d30_daily_due_min = sms_in_d60_daily_due_min = -1
        sms_in_daily_due_mean = sms_in_d3_daily_due_mean = sms_in_d7_daily_due_mean = sms_in_d14_daily_due_mean = sms_in_d30_daily_due_mean = sms_in_d60_daily_due_mean = -1
        sms_in_daily_overdue_max = sms_in_d3_daily_overdue_max = sms_in_d7_daily_overdue_max = sms_in_d14_daily_overdue_max = sms_in_d30_daily_overdue_max = sms_in_d60_daily_overdue_max = -1
        sms_in_daily_overdue_min = sms_in_d3_daily_overdue_min = sms_in_d7_daily_overdue_min = sms_in_d14_daily_overdue_min = sms_in_d30_daily_overdue_min = sms_in_d60_daily_overdue_min = -1
        sms_in_daily_overdue_mean = sms_in_d3_daily_overdue_mean = sms_in_d7_daily_overdue_mean = sms_in_d14_daily_overdue_mean = sms_in_d30_daily_overdue_mean = sms_in_d60_daily_overdue_mean = -1

    else:
        sms['date_gap'] = (sms.borrow_time.dt.date - sms.created_time.dt.date).dt.days
        
        sms_date_max_gap = sms.date_gap.max()
        sms_in_date_max_gap = sms.loc[sms.type == 1].date_gap.max()
        sms_out_date_max_gap = sms.loc[sms.type == 2].date_gap.max()
        sms_in_xjd_date_max_gap = sms.loc[(sms.type==1) & (sms.content.str.contains(xjd_words,case= False,na= False))].date_gap.max()
        sms_out_xjd_date_max_gap = sms.loc[(sms.type==2) & (sms.content.str.contains(xjd_words,case= False,na= False))].date_gap.max()
        sms_in_due_date_max_gap = sms.loc[(sms.type==1) & ((sms.content.str.contains(due_words,case= False,na= False)) | ((sms.content.str.contains(co_due_deuda,case= False,na= False)) & (sms.content.str.contains(co_due_caduca,case= False,na= False)))) & (~sms.content.str.contains(due_exclude_words,case= False,na= False)) & (~sms.content.str.contains(overdue_words,case= False,na= False))].date_gap.max()
        sms_in_overdue_date_max_gap = sms.loc[(sms.type==1) & ((sms.content.str.contains(overdue_words,case= False,na= False)) | ((sms.content.str.contains(co_overdue_deuda,case= False,na= False)) & (sms.content.str.contains(co_overdue_atrasado,case= False,na= False))))].date_gap.max()

        sms_date_min_gap = sms.date_gap.min()
        sms_in_date_min_gap = sms.loc[sms.type == 1].date_gap.min()
        sms_out_date_min_gap = sms.loc[sms.type == 2].date_gap.min()
        sms_in_xjd_date_min_gap = sms.loc[(sms.type==1) & (sms.content.str.contains(xjd_words,case= False,na= False))].date_gap.min()
        sms_out_xjd_date_min_gap = sms.loc[(sms.type==2) & (sms.content.str.contains(xjd_words,case= False,na= False))].date_gap.min()
        sms_in_due_date_min_gap = sms.loc[(sms.type==1) & ((sms.content.str.contains(due_words,case= False,na= False)) | ((sms.content.str.contains(co_due_deuda,case= False,na= False)) & (sms.content.str.contains(co_due_caduca,case= False,na= False)))) & (~sms.content.str.contains(due_exclude_words,case= False,na= False)) & (~sms.content.str.contains(overdue_words,case= False,na= False))].date_gap.min()
        sms_in_overdue_date_min_gap = sms.loc[(sms.type==1) & ((sms.content.str.contains(overdue_words,case= False,na= False)) | ((sms.content.str.contains(co_overdue_deuda,case= False,na= False)) & (sms.content.str.contains(co_overdue_atrasado,case= False,na= False))))].date_gap.min()
    
        sms_daily_max = sms.groupby('date_gap')['content'].count().max()
        sms_d3_daily_max = sms.loc[sms.date_gap <= 3].groupby('date_gap')['content'].count().max()
        sms_d7_daily_max = sms.loc[sms.date_gap <= 7].groupby('date_gap')['content'].count().max()
        sms_d14_daily_max = sms.loc[sms.date_gap <= 14].groupby('date_gap')['content'].count().max()
        sms_d30_daily_max = sms.loc[sms.date_gap <= 30].groupby('date_gap')['content'].count().max()
        sms_d60_daily_max = sms.loc[sms.date_gap <= 60].groupby('date_gap')['content'].count().max()

        sms_daily_min = sms.groupby('date_gap')['content'].count().min()
        sms_d3_daily_min = sms.loc[sms.date_gap <= 3].groupby('date_gap')['content'].count().min()
        sms_d7_daily_min = sms.loc[sms.date_gap <= 7].groupby('date_gap')['content'].count().min()
        sms_d14_daily_min = sms.loc[sms.date_gap <= 14].groupby('date_gap')['content'].count().min()
        sms_d30_daily_min = sms.loc[sms.date_gap <= 30].groupby('date_gap')['content'].count().min()
        sms_d60_daily_min = sms.loc[sms.date_gap <= 60].groupby('date_gap')['content'].count().min()

        sms_daily_mean = round(sms.groupby('date_gap')['content'].count().mean(),3)
        sms_d3_daily_mean = round(sms.loc[sms.date_gap <= 3].groupby('date_gap')['content'].count().mean(),3)
        sms_d7_daily_mean = round(sms.loc[sms.date_gap <= 7].groupby('date_gap')['content'].count().mean(),3)
        sms_d14_daily_mean = round(sms.loc[sms.date_gap <= 14].groupby('date_gap')['content'].count().mean(),3)
        sms_d30_daily_mean = round(sms.loc[sms.date_gap <= 30].groupby('date_gap')['content'].count().mean(),3)
        sms_d60_daily_mean = round(sms.loc[sms.date_gap <= 60].groupby('date_gap')['content'].count().mean(),3)
        
        sms_in_daily_max = sms.loc[sms.type == 1].groupby('date_gap')['content'].count().max()
        sms_in_d3_daily_max = sms.loc[(sms.type == 1) & (sms.date_gap <= 3)].groupby('date_gap')['content'].count().max()
        sms_in_d7_daily_max = sms.loc[(sms.type == 1) & (sms.date_gap <= 7)].groupby('date_gap')['content'].count().max()
        sms_in_d14_daily_max = sms.loc[(sms.type == 1) & (sms.date_gap <= 14)].groupby('date_gap')['content'].count().max()
        sms_in_d30_daily_max = sms.loc[(sms.type == 1) & (sms.date_gap <= 30)].groupby('date_gap')['content'].count().max()
        sms_in_d60_daily_max = sms.loc[(sms.type == 1) & (sms.date_gap <= 60)].groupby('date_gap')['content'].count().max()

        sms_in_daily_min = sms.loc[sms.type == 1].groupby('date_gap')['content'].count().min()
        sms_in_d3_daily_min = sms.loc[(sms.type == 1) & (sms.date_gap <= 3)].groupby('date_gap')['content'].count().min()
        sms_in_d7_daily_min = sms.loc[(sms.type == 1) & (sms.date_gap <= 7)].groupby('date_gap')['content'].count().min()
        sms_in_d14_daily_min = sms.loc[(sms.type == 1) & (sms.date_gap <= 14)].groupby('date_gap')['content'].count().min()
        sms_in_d30_daily_min = sms.loc[(sms.type == 1) & (sms.date_gap <= 30)].groupby('date_gap')['content'].count().min()
        sms_in_d60_daily_min = sms.loc[(sms.type == 1) & (sms.date_gap <= 60)].groupby('date_gap')['content'].count().min()

        sms_in_daily_mean = round(sms.loc[sms.type == 1].groupby('date_gap')['content'].count().mean(),3)
        sms_in_d3_daily_mean = round(sms.loc[(sms.type == 1) & (sms.date_gap <= 3)].groupby('date_gap')['content'].count().mean(),3)
        sms_in_d7_daily_mean = round(sms.loc[(sms.type == 1) & (sms.date_gap <= 7)].groupby('date_gap')['content'].count().mean(),3)
        sms_in_d14_daily_mean = round(sms.loc[(sms.type == 1) & (sms.date_gap <= 14)].groupby('date_gap')['content'].count().mean(),3)
        sms_in_d30_daily_mean = round(sms.loc[(sms.type == 1) & (sms.date_gap <= 30)].groupby('date_gap')['content'].count().mean(),3)
        sms_in_d60_daily_mean = round(sms.loc[(sms.type == 1) & (sms.date_gap <= 60)].groupby('date_gap')['content'].count().mean(),3)

        sms_out_daily_max = sms.loc[sms.type == 2].groupby('date_gap')['content'].count().max()
        sms_out_d3_daily_max = sms.loc[(sms.type == 2) & (sms.date_gap <= 3)].groupby('date_gap')['content'].count().max()
        sms_out_d7_daily_max = sms.loc[(sms.type == 2) & (sms.date_gap <= 7)].groupby('date_gap')['content'].count().max()
        sms_out_d14_daily_max = sms.loc[(sms.type == 2) & (sms.date_gap <= 14)].groupby('date_gap')['content'].count().max()
        sms_out_d30_daily_max = sms.loc[(sms.type == 2) & (sms.date_gap <= 30)].groupby('date_gap')['content'].count().max()
        sms_out_d60_daily_max = sms.loc[(sms.type == 2) & (sms.date_gap <= 60)].groupby('date_gap')['content'].count().max()

        sms_out_daily_min = sms.loc[sms.type == 2].groupby('date_gap')['content'].count().min()
        sms_out_d3_daily_min = sms.loc[(sms.type == 2) & (sms.date_gap <= 3)].groupby('date_gap')['content'].count().min()
        sms_out_d7_daily_min = sms.loc[(sms.type == 2) & (sms.date_gap <= 7)].groupby('date_gap')['content'].count().min()
        sms_out_d14_daily_min = sms.loc[(sms.type == 2) & (sms.date_gap <= 14)].groupby('date_gap')['content'].count().min()
        sms_out_d30_daily_min = sms.loc[(sms.type == 2) & (sms.date_gap <= 30)].groupby('date_gap')['content'].count().min()
        sms_out_d60_daily_min = sms.loc[(sms.type == 2) & (sms.date_gap <= 60)].groupby('date_gap')['content'].count().min()

        sms_out_daily_mean = round(sms.loc[sms.type == 2].groupby('date_gap')['content'].count().mean(),3)
        sms_out_d3_daily_mean = round(sms.loc[(sms.type == 2) & (sms.date_gap <= 3)].groupby('date_gap')['content'].count().mean(),3)
        sms_out_d7_daily_mean = round(sms.loc[(sms.type == 2) & (sms.date_gap <= 7)].groupby('date_gap')['content'].count().mean(),3)
        sms_out_d14_daily_mean = round(sms.loc[(sms.type == 2) & (sms.date_gap <= 14)].groupby('date_gap')['content'].count().mean(),3)
        sms_out_d30_daily_mean = round(sms.loc[(sms.type == 2) & (sms.date_gap <= 30)].groupby('date_gap')['content'].count().mean(),3)
        sms_out_d60_daily_mean = round(sms.loc[(sms.type == 2) & (sms.date_gap <= 60)].groupby('date_gap')['content'].count().mean(),3)

        sms_in_daily_xjd_max = sms.loc[(sms.type == 1) & (sms.content.str.contains(xjd_words,case= False,na= False))].groupby('date_gap')['content'].count().max()
        sms_in_d3_daily_xjd_max = sms.loc[(sms.type == 1) & (sms.date_gap <= 3) & (sms.content.str.contains(xjd_words,case= False,na= False))].groupby('date_gap')['content'].count().max()
        sms_in_d7_daily_xjd_max = sms.loc[(sms.type == 1) & (sms.date_gap <= 7) & (sms.content.str.contains(xjd_words,case= False,na= False))].groupby('date_gap')['content'].count().max()
        sms_in_d14_daily_xjd_max = sms.loc[(sms.type == 1) & (sms.date_gap <= 14) & (sms.content.str.contains(xjd_words,case= False,na= False))].groupby('date_gap')['content'].count().max()
        sms_in_d30_daily_xjd_max = sms.loc[(sms.type == 1) & (sms.date_gap <= 30) & (sms.content.str.contains(xjd_words,case= False,na= False))].groupby('date_gap')['content'].count().max()
        sms_in_d60_daily_xjd_max = sms.loc[(sms.type == 1) & (sms.date_gap <= 60) & (sms.content.str.contains(xjd_words,case= False,na= False))].groupby('date_gap')['content'].count().max()

        sms_in_daily_xjd_min = sms.loc[(sms.type == 1) & (sms.content.str.contains(xjd_words,case= False,na= False))].groupby('date_gap')['content'].count().min()
        sms_in_d3_daily_xjd_min = sms.loc[(sms.type == 1) & (sms.date_gap <= 3) & (sms.content.str.contains(xjd_words,case= False,na= False))].groupby('date_gap')['content'].count().min()
        sms_in_d7_daily_xjd_min = sms.loc[(sms.type == 1) & (sms.date_gap <= 7) & (sms.content.str.contains(xjd_words,case= False,na= False))].groupby('date_gap')['content'].count().min()
        sms_in_d14_daily_xjd_min = sms.loc[(sms.type == 1) & (sms.date_gap <= 14) & (sms.content.str.contains(xjd_words,case= False,na= False))].groupby('date_gap')['content'].count().min()
        sms_in_d30_daily_xjd_min = sms.loc[(sms.type == 1) & (sms.date_gap <= 30) & (sms.content.str.contains(xjd_words,case= False,na= False))].groupby('date_gap')['content'].count().min()
        sms_in_d60_daily_xjd_min = sms.loc[(sms.type == 1) & (sms.date_gap <= 60) & (sms.content.str.contains(xjd_words,case= False,na= False))].groupby('date_gap')['content'].count().min()

        sms_in_daily_xjd_mean = round(sms.loc[(sms.type == 1) & (sms.content.str.contains(xjd_words,case= False,na= False))].groupby('date_gap')['content'].count().mean(),3)
        sms_in_d3_daily_xjd_mean = round(sms.loc[(sms.type == 1) & (sms.date_gap <= 3) & (sms.content.str.contains(xjd_words,case= False,na= False))].groupby('date_gap')['content'].count().mean(),3)
        sms_in_d7_daily_xjd_mean = round(sms.loc[(sms.type == 1) & (sms.date_gap <= 7) & (sms.content.str.contains(xjd_words,case= False,na= False))].groupby('date_gap')['content'].count().mean(),3)
        sms_in_d14_daily_xjd_mean = round(sms.loc[(sms.type == 1) & (sms.date_gap <= 14) & (sms.content.str.contains(xjd_words,case= False,na= False))].groupby('date_gap')['content'].count().mean(),3)
        sms_in_d30_daily_xjd_mean = round(sms.loc[(sms.type == 1) & (sms.date_gap <= 30) & (sms.content.str.contains(xjd_words,case= False,na= False))].groupby('date_gap')['content'].count().mean(),3)
        sms_in_d60_daily_xjd_mean = round(sms.loc[(sms.type == 1) & (sms.date_gap <= 60) & (sms.content.str.contains(xjd_words,case= False,na= False))].groupby('date_gap')['content'].count().mean(),3)
    
        sms_in_daily_due_max = sms.loc[(sms.type == 1) & ((sms.content.str.contains(due_words,case= False,na= False)) | ((sms.content.str.contains(co_due_deuda,case= False,na= False)) & (sms.content.str.contains(co_due_caduca,case= False,na= False)))) & (~sms.content.str.contains(due_exclude_words,case= False,na= False)) & (~sms.content.str.contains(overdue_words,case= False,na= False))].groupby('date_gap')['content'].count().max()
        sms_in_d3_daily_due_max = sms.loc[(sms.type == 1) & (sms.date_gap <= 3) & ((sms.content.str.contains(due_words,case= False,na= False)) | ((sms.content.str.contains(co_due_deuda,case= False,na= False)) & (sms.content.str.contains(co_due_caduca,case= False,na= False)))) & (~sms.content.str.contains(due_exclude_words,case= False,na= False)) & (~sms.content.str.contains(overdue_words,case= False,na= False))].groupby('date_gap')['content'].count().max()
        sms_in_d7_daily_due_max = sms.loc[(sms.type == 1) & (sms.date_gap <= 7) & ((sms.content.str.contains(due_words,case= False,na= False)) | ((sms.content.str.contains(co_due_deuda,case= False,na= False)) & (sms.content.str.contains(co_due_caduca,case= False,na= False)))) & (~sms.content.str.contains(due_exclude_words,case= False,na= False)) & (~sms.content.str.contains(overdue_words,case= False,na= False))].groupby('date_gap')['content'].count().max()
        sms_in_d14_daily_due_max = sms.loc[(sms.type == 1) & (sms.date_gap <= 14) & ((sms.content.str.contains(due_words,case= False,na= False)) | ((sms.content.str.contains(co_due_deuda,case= False,na= False)) & (sms.content.str.contains(co_due_caduca,case= False,na= False)))) & (~sms.content.str.contains(due_exclude_words,case= False,na= False)) & (~sms.content.str.contains(overdue_words,case= False,na= False))].groupby('date_gap')['content'].count().max()
        sms_in_d30_daily_due_max = sms.loc[(sms.type == 1) & (sms.date_gap <= 30) & ((sms.content.str.contains(due_words,case= False,na= False)) | ((sms.content.str.contains(co_due_deuda,case= False,na= False)) & (sms.content.str.contains(co_due_caduca,case= False,na= False)))) & (~sms.content.str.contains(due_exclude_words,case= False,na= False)) & (~sms.content.str.contains(overdue_words,case= False,na= False))].groupby('date_gap')['content'].count().max()
        sms_in_d60_daily_due_max = sms.loc[(sms.type == 1) & (sms.date_gap <= 60) & ((sms.content.str.contains(due_words,case= False,na= False)) | ((sms.content.str.contains(co_due_deuda,case= False,na= False)) & (sms.content.str.contains(co_due_caduca,case= False,na= False)))) & (~sms.content.str.contains(due_exclude_words,case= False,na= False)) & (~sms.content.str.contains(overdue_words,case= False,na= False))].groupby('date_gap')['content'].count().max()

        sms_in_daily_due_min = sms.loc[(sms.type == 1) & ((sms.content.str.contains(due_words,case= False,na= False)) | ((sms.content.str.contains(co_due_deuda,case= False,na= False)) & (sms.content.str.contains(co_due_caduca,case= False,na= False)))) & (~sms.content.str.contains(due_exclude_words,case= False,na= False)) & (~sms.content.str.contains(overdue_words,case= False,na= False))].groupby('date_gap')['content'].count().min()
        sms_in_d3_daily_due_min = sms.loc[(sms.type == 1) & (sms.date_gap <= 3) & ((sms.content.str.contains(due_words,case= False,na= False)) | ((sms.content.str.contains(co_due_deuda,case= False,na= False)) & (sms.content.str.contains(co_due_caduca,case= False,na= False)))) & (~sms.content.str.contains(due_exclude_words,case= False,na= False)) & (~sms.content.str.contains(overdue_words,case= False,na= False))].groupby('date_gap')['content'].count().min()
        sms_in_d7_daily_due_min = sms.loc[(sms.type == 1) & (sms.date_gap <= 7) & ((sms.content.str.contains(due_words,case= False,na= False)) | ((sms.content.str.contains(co_due_deuda,case= False,na= False)) & (sms.content.str.contains(co_due_caduca,case= False,na= False)))) & (~sms.content.str.contains(due_exclude_words,case= False,na= False)) & (~sms.content.str.contains(overdue_words,case= False,na= False))].groupby('date_gap')['content'].count().min()
        sms_in_d14_daily_due_min = sms.loc[(sms.type == 1) & (sms.date_gap <= 14) & ((sms.content.str.contains(due_words,case= False,na= False)) | ((sms.content.str.contains(co_due_deuda,case= False,na= False)) & (sms.content.str.contains(co_due_caduca,case= False,na= False)))) & (~sms.content.str.contains(due_exclude_words,case= False,na= False)) & (~sms.content.str.contains(overdue_words,case= False,na= False))].groupby('date_gap')['content'].count().min()
        sms_in_d30_daily_due_min = sms.loc[(sms.type == 1) & (sms.date_gap <= 30) & ((sms.content.str.contains(due_words,case= False,na= False)) | ((sms.content.str.contains(co_due_deuda,case= False,na= False)) & (sms.content.str.contains(co_due_caduca,case= False,na= False)))) & (~sms.content.str.contains(due_exclude_words,case= False,na= False)) & (~sms.content.str.contains(overdue_words,case= False,na= False))].groupby('date_gap')['content'].count().min()
        sms_in_d60_daily_due_min = sms.loc[(sms.type == 1) & (sms.date_gap <= 60) & ((sms.content.str.contains(due_words,case= False,na= False)) | ((sms.content.str.contains(co_due_deuda,case= False,na= False)) & (sms.content.str.contains(co_due_caduca,case= False,na= False)))) & (~sms.content.str.contains(due_exclude_words,case= False,na= False)) & (~sms.content.str.contains(overdue_words,case= False,na= False))].groupby('date_gap')['content'].count().min()

        sms_in_daily_due_mean = round(sms.loc[(sms.type == 1) & ((sms.content.str.contains(due_words,case= False,na= False)) | ((sms.content.str.contains(co_due_deuda,case= False,na= False)) & (sms.content.str.contains(co_due_caduca,case= False,na= False)))) & (~sms.content.str.contains(due_exclude_words,case= False,na= False)) & (~sms.content.str.contains(overdue_words,case= False,na= False))].groupby('date_gap')['content'].count().mean(),3)
        sms_in_d3_daily_due_mean = round(sms.loc[(sms.type == 1) & (sms.date_gap <= 3) & ((sms.content.str.contains(due_words,case= False,na= False)) | ((sms.content.str.contains(co_due_deuda,case= False,na= False)) & (sms.content.str.contains(co_due_caduca,case= False,na= False)))) & (~sms.content.str.contains(due_exclude_words,case= False,na= False)) & (~sms.content.str.contains(overdue_words,case= False,na= False))].groupby('date_gap')['content'].count().mean(),3)
        sms_in_d7_daily_due_mean = round(sms.loc[(sms.type == 1) & (sms.date_gap <= 7) & ((sms.content.str.contains(due_words,case= False,na= False)) | ((sms.content.str.contains(co_due_deuda,case= False,na= False)) & (sms.content.str.contains(co_due_caduca,case= False,na= False)))) & (~sms.content.str.contains(due_exclude_words,case= False,na= False)) & (~sms.content.str.contains(overdue_words,case= False,na= False))].groupby('date_gap')['content'].count().mean(),3)
        sms_in_d14_daily_due_mean = round(sms.loc[(sms.type == 1) & (sms.date_gap <= 14) & ((sms.content.str.contains(due_words,case= False,na= False)) | ((sms.content.str.contains(co_due_deuda,case= False,na= False)) & (sms.content.str.contains(co_due_caduca,case= False,na= False)))) & (~sms.content.str.contains(due_exclude_words,case= False,na= False)) & (~sms.content.str.contains(overdue_words,case= False,na= False))].groupby('date_gap')['content'].count().mean(),3)
        sms_in_d30_daily_due_mean = round(sms.loc[(sms.type == 1) & (sms.date_gap <= 30) & ((sms.content.str.contains(due_words,case= False,na= False)) | ((sms.content.str.contains(co_due_deuda,case= False,na= False)) & (sms.content.str.contains(co_due_caduca,case= False,na= False)))) & (~sms.content.str.contains(due_exclude_words,case= False,na= False)) & (~sms.content.str.contains(overdue_words,case= False,na= False))].groupby('date_gap')['content'].count().mean(),3)
        sms_in_d60_daily_due_mean = round(sms.loc[(sms.type == 1) & (sms.date_gap <= 60) & ((sms.content.str.contains(due_words,case= False,na= False)) | ((sms.content.str.contains(co_due_deuda,case= False,na= False)) & (sms.content.str.contains(co_due_caduca,case= False,na= False)))) & (~sms.content.str.contains(due_exclude_words,case= False,na= False)) & (~sms.content.str.contains(overdue_words,case= False,na= False))].groupby('date_gap')['content'].count().mean(),3)
        
        sms_in_daily_overdue_max = sms.loc[(sms.type == 1) & ((sms.content.str.contains(overdue_words,case= False,na= False)) | ((sms.content.str.contains(co_overdue_deuda,case= False,na= False)) & (sms.content.str.contains(co_overdue_atrasado,case= False,na= False))))].groupby('date_gap')['content'].count().max()
        sms_in_d3_daily_overdue_max = sms.loc[(sms.type == 1) & (sms.date_gap <= 3) & ((sms.content.str.contains(overdue_words,case= False,na= False)) | ((sms.content.str.contains(co_overdue_deuda,case= False,na= False)) & (sms.content.str.contains(co_overdue_atrasado,case= False,na= False))))].groupby('date_gap')['content'].count().max()
        sms_in_d7_daily_overdue_max = sms.loc[(sms.type == 1) & (sms.date_gap <= 7) & ((sms.content.str.contains(overdue_words,case= False,na= False)) | ((sms.content.str.contains(co_overdue_deuda,case= False,na= False)) & (sms.content.str.contains(co_overdue_atrasado,case= False,na= False))))].groupby('date_gap')['content'].count().max()
        sms_in_d14_daily_overdue_max = sms.loc[(sms.type == 1) & (sms.date_gap <= 14) & ((sms.content.str.contains(overdue_words,case= False,na= False)) | ((sms.content.str.contains(co_overdue_deuda,case= False,na= False)) & (sms.content.str.contains(co_overdue_atrasado,case= False,na= False))))].groupby('date_gap')['content'].count().max()
        sms_in_d30_daily_overdue_max = sms.loc[(sms.type == 1) & (sms.date_gap <= 30) & ((sms.content.str.contains(overdue_words,case= False,na= False)) | ((sms.content.str.contains(co_overdue_deuda,case= False,na= False)) & (sms.content.str.contains(co_overdue_atrasado,case= False,na= False))))].groupby('date_gap')['content'].count().max()
        sms_in_d60_daily_overdue_max = sms.loc[(sms.type == 1) & (sms.date_gap <= 60) & ((sms.content.str.contains(overdue_words,case= False,na= False)) | ((sms.content.str.contains(co_overdue_deuda,case= False,na= False)) & (sms.content.str.contains(co_overdue_atrasado,case= False,na= False))))].groupby('date_gap')['content'].count().max()

        sms_in_daily_overdue_min = sms.loc[(sms.type == 1) & ((sms.content.str.contains(overdue_words,case= False,na= False)) | ((sms.content.str.contains(co_overdue_deuda,case= False,na= False)) & (sms.content.str.contains(co_overdue_atrasado,case= False,na= False))))].groupby('date_gap')['content'].count().min()
        sms_in_d3_daily_overdue_min = sms.loc[(sms.type == 1) & (sms.date_gap <= 3) & ((sms.content.str.contains(overdue_words,case= False,na= False)) | ((sms.content.str.contains(co_overdue_deuda,case= False,na= False)) & (sms.content.str.contains(co_overdue_atrasado,case= False,na= False))))].groupby('date_gap')['content'].count().min()
        sms_in_d7_daily_overdue_min = sms.loc[(sms.type == 1) & (sms.date_gap <= 7) & ((sms.content.str.contains(overdue_words,case= False,na= False)) | ((sms.content.str.contains(co_overdue_deuda,case= False,na= False)) & (sms.content.str.contains(co_overdue_atrasado,case= False,na= False))))].groupby('date_gap')['content'].count().min()
        sms_in_d14_daily_overdue_min = sms.loc[(sms.type == 1) & (sms.date_gap <= 14) & ((sms.content.str.contains(overdue_words,case= False,na= False)) | ((sms.content.str.contains(co_overdue_deuda,case= False,na= False)) & (sms.content.str.contains(co_overdue_atrasado,case= False,na= False))))].groupby('date_gap')['content'].count().min()
        sms_in_d30_daily_overdue_min = sms.loc[(sms.type == 1) & (sms.date_gap <= 30) & ((sms.content.str.contains(overdue_words,case= False,na= False)) | ((sms.content.str.contains(co_overdue_deuda,case= False,na= False)) & (sms.content.str.contains(co_overdue_atrasado,case= False,na= False))))].groupby('date_gap')['content'].count().min()
        sms_in_d60_daily_overdue_min = sms.loc[(sms.type == 1) & (sms.date_gap <= 60) & ((sms.content.str.contains(overdue_words,case= False,na= False)) | ((sms.content.str.contains(co_overdue_deuda,case= False,na= False)) & (sms.content.str.contains(co_overdue_atrasado,case= False,na= False))))].groupby('date_gap')['content'].count().min()

        sms_in_daily_overdue_mean = round(sms.loc[(sms.type == 1) & ((sms.content.str.contains(overdue_words,case= False,na= False)) | ((sms.content.str.contains(co_overdue_deuda,case= False,na= False)) & (sms.content.str.contains(co_overdue_atrasado,case= False,na= False))))].groupby('date_gap')['content'].count().mean(),3)
        sms_in_d3_daily_overdue_mean = round(sms.loc[(sms.type == 1) & (sms.date_gap <= 3) & ((sms.content.str.contains(overdue_words,case= False,na= False)) | ((sms.content.str.contains(co_overdue_deuda,case= False,na= False)) & (sms.content.str.contains(co_overdue_atrasado,case= False,na= False))))].groupby('date_gap')['content'].count().mean(),3)
        sms_in_d7_daily_overdue_mean = round(sms.loc[(sms.type == 1) & (sms.date_gap <= 7) & ((sms.content.str.contains(overdue_words,case= False,na= False)) | ((sms.content.str.contains(co_overdue_deuda,case= False,na= False)) & (sms.content.str.contains(co_overdue_atrasado,case= False,na= False))))].groupby('date_gap')['content'].count().mean(),3)
        sms_in_d14_daily_overdue_mean = round(sms.loc[(sms.type == 1) & (sms.date_gap <= 14) & ((sms.content.str.contains(overdue_words,case= False,na= False)) | ((sms.content.str.contains(co_overdue_deuda,case= False,na= False)) & (sms.content.str.contains(co_overdue_atrasado,case= False,na= False))))].groupby('date_gap')['content'].count().mean(),3)
        sms_in_d30_daily_overdue_mean = round(sms.loc[(sms.type == 1) & (sms.date_gap <= 30) & ((sms.content.str.contains(overdue_words,case= False,na= False)) | ((sms.content.str.contains(co_overdue_deuda,case= False,na= False)) & (sms.content.str.contains(co_overdue_atrasado,case= False,na= False))))].groupby('date_gap')['content'].count().mean(),3)
        sms_in_d60_daily_overdue_mean = round(sms.loc[(sms.type == 1) & (sms.date_gap <= 60) & ((sms.content.str.contains(overdue_words,case= False,na= False)) | ((sms.content.str.contains(co_overdue_deuda,case= False,na= False)) & (sms.content.str.contains(co_overdue_atrasado,case= False,na= False))))].groupby('date_gap')['content'].count().mean(),3)

    data = {'order_id':order_id,
            'sms_cnt':sms_cnt,'sms_d0_cnt':sms_d0_cnt,'sms_d3_cnt':sms_d3_cnt,'sms_d7_cnt':sms_d7_cnt,'sms_d14_cnt':sms_d14_cnt,'sms_d30_cnt':sms_d30_cnt,'sms_d60_cnt':sms_d60_cnt,
            'sms_in_cnt':sms_in_cnt,'sms_in_d0_cnt':sms_in_d0_cnt,'sms_in_d3_cnt':sms_in_d3_cnt,'sms_in_d7_cnt':sms_in_d7_cnt,'sms_in_d14_cnt':sms_in_d14_cnt,'sms_in_d30_cnt':sms_in_d30_cnt,'sms_in_d60_cnt':sms_in_d60_cnt,
            'sms_out_cnt':sms_out_cnt,'sms_out_d0_cnt':sms_out_d0_cnt,'sms_out_d3_cnt':sms_out_d3_cnt,'sms_out_d7_cnt':sms_out_d7_cnt,'sms_out_d14_cnt':sms_out_d14_cnt,'sms_out_d30_cnt':sms_out_d30_cnt,'sms_out_d60_cnt':sms_out_d60_cnt,
            'sms_in_xjd_cnt':sms_in_xjd_cnt,'sms_in_d0_xjd_cnt':sms_in_d0_xjd_cnt,'sms_in_d3_xjd_cnt':sms_in_d3_xjd_cnt,'sms_in_d7_xjd_cnt':sms_in_d7_xjd_cnt,'sms_in_d14_xjd_cnt':sms_in_d14_xjd_cnt,'sms_in_d30_xjd_cnt':sms_in_d30_xjd_cnt,'sms_in_d60_xjd_cnt':sms_in_d60_xjd_cnt,
            'sms_out_xjd_cnt':sms_out_xjd_cnt,'sms_out_d0_xjd_cnt':sms_out_d0_xjd_cnt,'sms_out_d3_xjd_cnt':sms_out_d3_xjd_cnt,'sms_out_d7_xjd_cnt':sms_out_d7_xjd_cnt,'sms_out_d14_xjd_cnt':sms_out_d14_xjd_cnt,'sms_out_d30_xjd_cnt':sms_out_d30_xjd_cnt,'sms_out_d60_xjd_cnt':sms_out_d60_xjd_cnt,
            'sms_in_due_cnt':sms_in_due_cnt,'sms_in_d0_due_cnt':sms_in_d0_due_cnt,'sms_in_d3_due_cnt':sms_in_d3_due_cnt,'sms_in_d7_due_cnt':sms_in_d7_due_cnt,'sms_in_d14_due_cnt':sms_in_d14_due_cnt,'sms_in_d30_due_cnt':sms_in_d30_due_cnt,'sms_in_d60_due_cnt':sms_in_d60_due_cnt,
            'sms_in_overdue_cnt':sms_in_overdue_cnt,'sms_in_d0_overdue_cnt':sms_in_d0_overdue_cnt,'sms_in_d3_overdue_cnt':sms_in_d3_overdue_cnt,'sms_in_d7_overdue_cnt':sms_in_d7_overdue_cnt,'sms_in_d14_overdue_cnt':sms_in_d14_overdue_cnt,'sms_in_d30_overdue_cnt':sms_in_d30_overdue_cnt,'sms_in_d60_overdue_cnt':sms_in_d60_overdue_cnt,
            'sms_in_otp_cnt':sms_in_otp_cnt,'sms_in_d0_otp_cnt':sms_in_d0_otp_cnt,'sms_in_d3_otp_cnt':sms_in_d3_otp_cnt,'sms_in_d7_otp_cnt':sms_in_d7_otp_cnt,'sms_in_d14_otp_cnt':sms_in_d14_otp_cnt,'sms_in_d30_otp_cnt':sms_in_d30_otp_cnt,'sms_in_d60_otp_cnt':sms_in_d60_otp_cnt,
            'sms_in_bank_cnt':sms_in_bank_cnt,'sms_in_d0_bank_cnt':sms_in_d0_bank_cnt,'sms_in_d3_bank_cnt':sms_in_d3_bank_cnt,'sms_in_d7_bank_cnt':sms_in_d7_bank_cnt,'sms_in_d14_bank_cnt':sms_in_d14_bank_cnt,'sms_in_d30_bank_cnt':sms_in_d30_bank_cnt,'sms_in_d60_bank_cnt':sms_in_d60_bank_cnt,
            'sms_in_currency_cnt':sms_in_currency_cnt,'sms_in_d0_currency_cnt':sms_in_d0_currency_cnt,'sms_in_d3_currency_cnt':sms_in_d3_currency_cnt,'sms_in_d7_currency_cnt':sms_in_d7_currency_cnt,'sms_in_d14_currency_cnt':sms_in_d14_currency_cnt,'sms_in_d30_currency_cnt':sms_in_d30_currency_cnt,'sms_in_d60_currency_cnt':sms_in_d60_currency_cnt,
            'sms_in_payment_cnt':sms_in_payment_cnt,'sms_in_d0_payment_cnt':sms_in_d0_payment_cnt,'sms_in_d3_payment_cnt':sms_in_d3_payment_cnt,'sms_in_d7_payment_cnt':sms_in_d7_payment_cnt,'sms_in_d14_payment_cnt':sms_in_d14_payment_cnt,'sms_in_d30_payment_cnt':sms_in_d30_payment_cnt,'sms_in_d60_payment_cnt':sms_in_d60_payment_cnt,
            'sms_in_blacklist_cnt':sms_in_blacklist_cnt,'sms_in_d0_blacklist_cnt':sms_in_d0_blacklist_cnt,'sms_in_d3_blacklist_cnt':sms_in_d3_blacklist_cnt,'sms_in_d7_blacklist_cnt':sms_in_d7_blacklist_cnt,'sms_in_d14_blacklist_cnt':sms_in_d14_blacklist_cnt,'sms_in_d30_blacklist_cnt':sms_in_d30_blacklist_cnt,'sms_in_d60_blacklist_cnt':sms_in_d60_blacklist_cnt,
            'sms_in_account_cnt':sms_in_account_cnt,'sms_in_d0_account_cnt':sms_in_d0_account_cnt,'sms_in_d3_account_cnt':sms_in_d3_account_cnt,'sms_in_d7_account_cnt':sms_in_d7_account_cnt,'sms_in_d14_account_cnt':sms_in_d14_account_cnt,'sms_in_d30_account_cnt':sms_in_d30_account_cnt,'sms_in_d60_account_cnt':sms_in_d60_account_cnt,
            'sms_in_casino_cnt':sms_in_casino_cnt,'sms_in_d0_casino_cnt':sms_in_d0_casino_cnt,'sms_in_d3_casino_cnt':sms_in_d3_casino_cnt,'sms_in_d7_casino_cnt':sms_in_d7_casino_cnt,'sms_in_d14_casino_cnt':sms_in_d14_casino_cnt,'sms_in_d30_casino_cnt':sms_in_d30_casino_cnt,'sms_in_d60_casino_cnt':sms_in_d60_casino_cnt,
            'sms_in_gcash_cnt':sms_in_gcash_cnt,'sms_in_d0_gcash_cnt':sms_in_d0_gcash_cnt,'sms_in_d3_gcash_cnt':sms_in_d3_gcash_cnt,'sms_in_d7_gcash_cnt':sms_in_d7_gcash_cnt,'sms_in_d14_gcash_cnt':sms_in_d14_gcash_cnt,'sms_in_d30_gcash_cnt':sms_in_d30_gcash_cnt,'sms_in_d60_gcash_cnt':sms_in_d60_gcash_cnt,
            'sms_in_whatsapp_cnt':sms_in_whatsapp_cnt,
            'sms_unique_phone_cnt':sms_unique_phone_cnt,
            'sms_in_unique_phone_cnt':sms_in_unique_phone_cnt,'sms_in_d0_unique_phone_cnt':sms_in_d0_unique_phone_cnt,'sms_in_d3_unique_phone_cnt':sms_in_d3_unique_phone_cnt,'sms_in_d7_unique_phone_cnt':sms_in_d7_unique_phone_cnt,'sms_in_d14_unique_phone_cnt':sms_in_d14_unique_phone_cnt,'sms_in_d30_unique_phone_cnt':sms_in_d30_unique_phone_cnt,'sms_in_d60_unique_phone_cnt':sms_in_d60_unique_phone_cnt,
            'sms_out_unique_phone_cnt':sms_out_unique_phone_cnt,'sms_out_d0_unique_phone_cnt':sms_out_d0_unique_phone_cnt,'sms_out_d3_unique_phone_cnt':sms_out_d3_unique_phone_cnt,'sms_out_d7_unique_phone_cnt':sms_out_d7_unique_phone_cnt,'sms_out_d14_unique_phone_cnt':sms_out_d14_unique_phone_cnt,'sms_out_d30_unique_phone_cnt':sms_out_d30_unique_phone_cnt,'sms_out_d60_unique_phone_cnt':sms_out_d60_unique_phone_cnt,
            'sms_in_xjd_unique_phone_cnt':sms_in_xjd_unique_phone_cnt,'sms_in_d0_xjd_unique_phone_cnt':sms_in_d0_xjd_unique_phone_cnt,'sms_in_d3_xjd_unique_phone_cnt':sms_in_d3_xjd_unique_phone_cnt,'sms_in_d7_xjd_unique_phone_cnt':sms_in_d7_xjd_unique_phone_cnt,'sms_in_d14_xjd_unique_phone_cnt':sms_in_d14_xjd_unique_phone_cnt,'sms_in_d30_xjd_unique_phone_cnt':sms_in_d30_xjd_unique_phone_cnt,'sms_in_d60_xjd_unique_phone_cnt':sms_in_d60_xjd_unique_phone_cnt,
            'sms_out_xjd_unique_phone_cnt':sms_out_xjd_unique_phone_cnt,
            'sms_in_due_unique_phone_cnt':sms_in_due_unique_phone_cnt,'sms_in_d0_due_uniabsque_phone_cnt':sms_in_d0_due_unique_phone_cnt,'sms_in_d3_due_unique_phone_cnt':sms_in_d3_due_unique_phone_cnt,'sms_in_d7_due_unique_phone_cnt':sms_in_d7_due_unique_phone_cnt,'sms_in_d14_due_unique_phone_cnt':sms_in_d14_due_unique_phone_cnt,'sms_in_d30_due_unique_phone_cnt':sms_in_d30_due_unique_phone_cnt,'sms_in_d60_due_unique_phone_cnt':sms_in_d60_due_unique_phone_cnt,
            'sms_in_overdue_unique_phone_cnt':sms_in_overdue_unique_phone_cnt,'sms_iabsn_d0_overdue_unique_phone_cnt':sms_in_d0_overdue_unique_phone_cnt,'sms_in_d3_overdue_unique_phone_cnt':sms_in_d3_overdue_unique_phone_cnt,'sms_in_d7_overdue_unique_phone_cnt':sms_in_d7_overdue_unique_phone_cnt,'sms_in_d14_overdue_unique_phone_cnt':sms_in_d14_overdue_unique_phone_cnt,'sms_in_d30_overdue_unique_phone_cnt':sms_in_d30_overdue_unique_phone_cnt,'sms_in_d60_overdue_unique_phone_cnt':sms_in_d60_overdue_unique_phone_cnt,
            'sms_in_casino_unique_phone_cnt':sms_in_casino_unique_phone_cnt,
            'sms_date_max_gap':sms_date_max_gap,'sms_in_date_max_gap':sms_in_date_max_gap,'sms_out_date_max_gap':sms_out_date_max_gap,'sms_in_xjd_date_max_gap':sms_in_xjd_date_max_gap,'sms_out_xjd_date_max_gap':sms_out_xjd_date_max_gap,'sms_in_due_date_max_gap':sms_in_due_date_max_gap,'sms_in_overdue_date_max_gap':sms_in_overdue_date_max_gap,
            'sms_date_min_gap':sms_date_min_gap,'sms_in_date_min_gap':sms_in_date_min_gap,'sms_out_date_min_gap':sms_out_date_min_gap,'sms_in_xjd_date_min_gap':sms_in_xjd_date_min_gap,'sms_out_xjd_date_min_gap':sms_out_xjd_date_min_gap,'sms_in_due_date_min_gap':sms_in_due_date_min_gap,'sms_in_overdue_date_min_gap':sms_in_overdue_date_min_gap,
            'sms_daily_max':sms_daily_max,'sms_d3_daily_max':sms_d3_daily_max,'sms_d7_daily_max':sms_d7_daily_max,'sms_d14_daily_max':sms_d14_daily_max,'sms_d30_daily_max':sms_d30_daily_max,'sms_d60_daily_max':sms_d60_daily_max,
            'sms_daily_min':sms_daily_min,'sms_d3_daily_min':sms_d3_daily_min,'sms_d7_daily_min':sms_d7_daily_min,'sms_d14_daily_min':sms_d14_daily_min,'sms_d30_daily_min':sms_d30_daily_min,'sms_d60_daily_min':sms_d60_daily_min,
            'sms_daily_mean':sms_daily_mean,'sms_d3_daily_mean':sms_d3_daily_mean,'sms_d7_daily_mean':sms_d7_daily_mean,'sms_d14_daily_mean':sms_d14_daily_mean,'sms_d30_daily_mean':sms_d30_daily_mean,'sms_d60_daily_mean':sms_d60_daily_mean,
            'sms_in_daily_max':sms_in_daily_max,'sms_in_d3_daily_max':sms_in_d3_daily_max,'sms_in_d7_daily_max':sms_in_d7_daily_max,'sms_in_d14_daily_max':sms_in_d14_daily_max,'sms_in_d30_daily_max':sms_in_d30_daily_max,'sms_in_d60_daily_max':sms_in_d60_daily_max,
            'sms_in_daily_min':sms_in_daily_min,'sms_in_d3_daily_min':sms_in_d3_daily_min,'sms_in_d7_daily_min':sms_in_d7_daily_min,'sms_in_d14_daily_min':sms_in_d14_daily_min,'sms_in_d30_daily_min':sms_in_d30_daily_min,'sms_in_d60_daily_min':sms_in_d60_daily_min,
            'sms_in_daily_mean':sms_in_daily_mean,'sms_in_d3_daily_mean':sms_in_d3_daily_mean,'sms_in_d7_daily_mean':sms_in_d7_daily_mean,'sms_in_d14_daily_mean':sms_in_d14_daily_mean,'sms_in_d30_daily_mean':sms_in_d30_daily_mean,'sms_in_d60_daily_mean':sms_in_d60_daily_mean,
            'sms_out_daily_max':sms_out_daily_max,'sms_out_d3_daily_max':sms_out_d3_daily_max,'sms_out_d7_daily_max':sms_out_d7_daily_max,'sms_out_d14_daily_max':sms_out_d14_daily_max,'sms_out_d30_daily_max':sms_out_d30_daily_max,'sms_out_d60_daily_max':sms_out_d60_daily_max,
            'sms_out_daily_min':sms_out_daily_min,'sms_out_d3_daily_min':sms_out_d3_daily_min,'sms_out_d7_daily_min':sms_out_d7_daily_min,'sms_out_d14_daily_min':sms_out_d14_daily_min,'sms_out_d30_daily_min':sms_out_d30_daily_min,'sms_out_d60_daily_min':sms_out_d60_daily_min,
            'sms_out_daily_mean':sms_out_daily_mean,'sms_out_d3_daily_mean':sms_out_d3_daily_mean,'sms_out_d7_daily_mean':sms_out_d7_daily_mean,'sms_out_d14_daily_mean':sms_out_d14_daily_mean,'sms_out_d30_daily_mean':sms_out_d30_daily_mean,'sms_out_d60_daily_mean':sms_out_d60_daily_mean,
            'sms_in_daily_xjd_max':sms_in_daily_xjd_max,'sms_in_d3_daily_xjd_max':sms_in_d3_daily_xjd_max,'sms_in_d7_daily_xjd_max':sms_in_d7_daily_xjd_max,'sms_in_d14_daily_xjd_max':sms_in_d14_daily_xjd_max,'sms_in_d30_daily_xjd_max':sms_in_d30_daily_xjd_max,'sms_in_d60_daily_xjd_max':sms_in_d60_daily_xjd_max,
            'sms_in_daily_xjd_min':sms_in_daily_xjd_min,'sms_in_d3_daily_xjd_min':sms_in_d3_daily_xjd_min,'sms_in_d7_daily_xjd_min':sms_in_d7_daily_xjd_min,'sms_in_d14_daily_xjd_min':sms_in_d14_daily_xjd_min,'sms_in_d30_daily_xjd_min':sms_in_d30_daily_xjd_min,'sms_in_d60_daily_xjd_min':sms_in_d60_daily_xjd_min,
            'sms_in_daily_xjd_mean':sms_in_daily_xjd_mean,'sms_in_d3_daily_xjd_mean':sms_in_d3_daily_xjd_mean,'sms_in_d7_daily_xjd_mean':sms_in_d7_daily_xjd_mean,'sms_in_d14_daily_xjd_mean':sms_in_d14_daily_xjd_mean,'sms_in_d30_daily_xjd_mean':sms_in_d30_daily_xjd_mean,'sms_in_d60_daily_xjd_mean':sms_in_d60_daily_xjd_mean,
            'sms_in_daily_due_max':sms_in_daily_due_max,'sms_in_d3_daily_due_max':sms_in_d3_daily_due_max,'sms_in_d7_daily_due_max':sms_in_d7_daily_due_max,'sms_in_d14_daily_due_max':sms_in_d14_daily_due_max,'sms_in_d30_daily_due_max':sms_in_d30_daily_due_max,'sms_in_d60_daily_due_max':sms_in_d60_daily_due_max,
            'sms_in_daily_due_min':sms_in_daily_due_min,'sms_in_d3_daily_due_min':sms_in_d3_daily_due_min,'sms_in_d7_daily_due_min':sms_in_d7_daily_due_min,'sms_in_d14_daily_due_min':sms_in_d14_daily_due_min,'sms_in_d30_daily_due_min':sms_in_d30_daily_due_min,'sms_in_d60_daily_due_min':sms_in_d60_daily_due_min,
            'sms_in_daily_due_mean':sms_in_daily_due_mean,'sms_in_d3_daily_due_mean':sms_in_d3_daily_due_mean,'sms_in_d7_daily_due_mean':sms_in_d7_daily_due_mean,'sms_in_d14_daily_due_mean':sms_in_d14_daily_due_mean,'sms_in_d30_daily_due_mean':sms_in_d30_daily_due_mean,'sms_in_d60_daily_due_mean':sms_in_d60_daily_due_mean,
            'sms_in_daily_overdue_max':sms_in_daily_overdue_max,'sms_in_d3_daily_overdue_max':sms_in_d3_daily_overdue_max,'sms_in_d7_daily_overdue_max':sms_in_d7_daily_overdue_max,'sms_in_d14_daily_overdue_max':sms_in_d14_daily_overdue_max,'sms_in_d30_daily_overdue_max':sms_in_d30_daily_overdue_max,'sms_in_d60_daily_overdue_max':sms_in_d60_daily_overdue_max,
            'sms_in_daily_overdue_min':sms_in_daily_overdue_min,'sms_in_d3_daily_overdue_min':sms_in_d3_daily_overdue_min,'sms_in_d7_daily_overdue_min':sms_in_d7_daily_overdue_min,'sms_in_d14_daily_overdue_min':sms_in_d14_daily_overdue_min,'sms_in_d30_daily_overdue_min':sms_in_d30_daily_overdue_min,'sms_in_d60_daily_overdue_min':sms_in_d60_daily_overdue_min,
            'sms_in_daily_overdue_mean':sms_in_daily_overdue_mean,'sms_in_d3_daily_overdue_mean':sms_in_d3_daily_overdue_mean,'sms_in_d7_daily_overdue_mean':sms_in_d7_daily_overdue_mean,'sms_in_d14_daily_overdue_mean':sms_in_d14_daily_overdue_mean,'sms_in_d30_daily_overdue_mean':sms_in_d30_daily_overdue_mean,'sms_in_d60_daily_overdue_mean':sms_in_d60_daily_overdue_mean,
           }
    sms_df = pd.DataFrame([data])
    sms_df.fillna(-1,inplace=True)
    return sms_df


