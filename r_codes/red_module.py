import requests
import numpy as np
import pandas as pd
import datetime
from pykrx import stock
from dateutil.relativedelta import relativedelta
from rf_module import f_main
import re

class red_main:
    def __init__(self):
        pass
    def __main__(self, code, date, appkey, appsec, acckey):
        try:
            red_y, red_q=self.action(code, date, appkey, appsec, acckey)
        except:
            red_y=pd.DataFrame(columns=['컬럼 비공개'])
            red_q=pd.DataFrame(columns=['컬럼 비공개'])

        return red_y, red_q
    
    def action(self, code, date, appkey, appsec, acckey):
        #정보 정리 로직 비공개: getfnsheet 활성화시키는 부분

    def getfnsheet(self, code, appkey, appsec, acckey): #손익계산서
        rf_y, rf_q=f_main(appkey, appsec, acckey).__main__(code=code)
        return rf_y, rf_q
    
    
    def getfindata(self, code, freqtyp): #손익계산서 제외한 나머지 전부
        url=f""
        res=requests.get(url).text
        if freqtyp=="Q":
            self.position=12
            fin=pd.read_html(res)[self.position] #IFRS 연결 분기
            if fin.loc[0].isna().sum()+1==len(fin.loc[0]):
                self.position=15
                fin=pd.read_html(res)[self.position] #IFRS 별도 분기
        elif freqtyp=="Y":
            if self.position==12:
                fin=pd.read_html(res)[11]
            elif self.position==15:
                fin=pd.read_html(res)[14] #IFRS 별도 연간
        fin=fin.set_index(fin.columns[0]).T.droplevel(0)
        fin.reset_index(names='stac_yymm', inplace=True)
        fin['컨센서스']=fin['stac_yymm'].apply(lambda x: 'E' if "(E)" in x else 'P' if "(P)" in x else 'A')
        fin['stac_yymm']=fin['stac_yymm'].apply(lambda x: re.sub(r'[^0-9.]', '', x))
        
        #매출액, 영업이익, 당기순이익
        fin_temp=pd.DataFrame(index=fin.index)
        temp_target=['stac_yymm','매출액', '영업이익', '당기순이익']
        for col in temp_target:
            if col in fin.columns:
                fin_temp[col]=fin[col]
            else:
                fin_temp[col]="XXXX"
        fin_temp.set_index('stac_yymm', inplace=True)
                
        fin1=fin[['컬럼 비공개']].copy()
        #이후 전처리 로직 비공개
        return fin1, fin_temp, list(fin['stac_yymm'])[0], list(fin['stac_yymm'])[-1]
    
    def getfindata_2(self, code): #당좌비율
        #로직 비공개
    
    def add_ratio(self, df):
        #로직 비공개
        return df
    
    def close_price(self, startdate, enddate, code, freq): #PER 추정 위한 종가 확보
        startdate=startdate+'01'
        enddate=enddate+'01'
        startdate=datetime.datetime.strptime(startdate,'%Y%m%d').strftime('%Y%m%d')
        enddate=(datetime.datetime.strptime(enddate,'%Y%m%d')+relativedelta(months=1)-relativedelta(days=1)).strftime('%Y%m%d')
        if freq=='q':
            price=stock.get_market_ohlcv(startdate, enddate, code, "m")
        elif freq=='y':
            price=stock.get_market_ohlcv(startdate, enddate, code, "y")
        price=price.reset_index()[['날짜','종가']]
        price['날짜']=price['날짜'].apply(lambda x: str(x)[:7].replace('-',''))
        price=price.set_index('날짜')
        price.index.name=None
        return price
        
    
    def clear_temp(self, tx, rx):
        #소스 두 개의 내용을 모두 참조하여 비어있지 않은 재무제표 만드는 로직
        return cx