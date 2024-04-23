from r_codes.red_module import red_main
from r_codes.yellow_module import yellow_main
from r_codes.navy_module import navy_main
from r_codes.green_module import green_main
from r_codes.spread import spread_data
import pandas as pd
import threading
import time

#정보 파싱 모듈
class r_main:
    def __init__(self, appkey, appsec, acckey):
        self.app_key=appkey
        self.app_sec=appsec
        self.acckey=acckey
    
    def __main__(self,tickers):
        self.action(ticker_df=tickers)
        return self.ynb_df, self.red_q_df, self.red_y_df, self.green_df
        
    def action(self, ticker_df):
        savecsv=True #################csv 저장 여부
        
        #데이터프레임 기본 생성 (비공개)
        
        ####멀티스레딩
        procs=10 #스레드 한도 10 넘어갈 경우 초당 거래건수 초과!
        for i in range(1+(len(ticker_df)//procs)):
            print('='*50, (i+1)*procs, '='*5)
            ticker_df_split=ticker_df.iloc[i*procs:(i+1)*procs]
            threads=[]
            for i in range(procs):
                t=threading.Thread(target=self.parsingaction, args=(i, ticker_df_split))
                t.start()
                threads.append(t)
            for t in threads:
                t.join()
            time.sleep(0.5)
            print(self.ynb_df.shape)
        ####
        spread_data().__main__() #스프레드 데이터 확보
        if savecsv:
            self.ynb_df.to_csv('./test_ynb_df.csv')
            self.red_y_df.to_csv('./test_redy_df.csv')
            self.red_q_df.to_csv('./test_redq_df.csv')
            self.green_df.to_csv('./test_green_df.csv')
            
    def parsingaction(self, i, ticker_df):
        try:
            #print(ticker_df.iloc[i]['ticker'], ticker_df.iloc[i]['name']) #############JUST FOR TEST!!!!
            yellow_row, red_a_row, red_q_row, navy_row, green_row, botong =self.iteration(
                code=ticker_df.iloc[i]['ticker'], name=ticker_df.iloc[i]['name'], appkey=self.app_key, appsec=self.app_sec, acckey=self.acckey)
            self.concatdf(yellow_row, red_a_row, red_q_row, navy_row, green_row, botong)
        except: pass
         
    def iteration(self, code, name, appkey, appsec, acckey):
        yellow_sheet, stocktype, reitsny=yellow_main(appkey=self.app_key, appsec=self.app_sec, acckey=self.acckey).__main__(code=code, name=name)
        botong=True if ##보통주 판단 로직 비공개
        if botong:
            navy_sheet, date =navy_main().__main__(code=code)
            red_sheet_a, red_sheet_q=red_main().__main__(code=code, date=date, appkey=appkey, appsec=appsec, acckey=acckey)
            green_sheet=green_main().__main__(code=code)
            return yellow_sheet, red_sheet_a, red_sheet_q, navy_sheet, green_sheet, botong
        else:
            return yellow_sheet, None, None, None, None, botong
        
            
    def concatdf(self, y, ra, rq, n, g, botong):
        if botong:
            row=pd.concat([y, n],axis=1)
        else:
            row=y
        self.ynb_df=pd.concat([self.ynb_df, row])
        self.red_y_df=pd.concat([self.red_y_df, ra])
        self.red_q_df=pd.concat([self.red_q_df, rq])
        self.green_df=pd.concat([self.green_df, g])