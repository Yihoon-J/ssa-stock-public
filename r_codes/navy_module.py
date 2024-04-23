import requests
import pandas as pd
import bs4
import re
import datetime
from dateutil.relativedelta import relativedelta

class navy_main:
    def __init__(self):
        pass
    def __main__(self, code):
        source=self.parsing_base(code=code)
        navy_sheet, date=self.build_tables(source=source, code=code)
        return navy_sheet, date
        
    def parsing_base(self, code):
        url=f''
        source=requests.get(url).text
        return source
    
    def build_tables(self, source, code):
        url=f""
        soup = bs4.BeautifulSoup(requests.get(url).content, 'html.parser')
        dt=soup.select('---')[0].text #기준일자
        dt=re.sub(r'[^0-9.]', '', dt)
        try:
            dstr=int(soup.find_all('tbody')[0].find_all('tr')[-1].find_all('td')[1].text.split(sep="/")[0].replace(",",""))
        except:
            dstr=None
        cp=pd.read_html(source)[0][0].iloc[0].split(sep="  ")[-1].replace(",","")
        
        for i in range(7,0,-1):
            t1v=pd.read_html(source)[i].T
            if '시가총액' in list(pd.read_html(source)[i].T.iloc[0]):
                t1=t1v.copy()
        t1.columns=t1.iloc[0]
        t1=t1.drop(0)
        t1['시가총액']=t1['시가총액'].apply(lambda x: re.sub(r'[^0-9.]', '', x))
        try:
            t1['시가총액순위']=t1['시가총액순위'].apply(lambda x: x.replace("위",""))
        except:
            t1['시가총액순위']=''
            
        try:
            t1.columns=['---']
            t1['액면가 | 매매단위']=t1['액면가 | 매매단위'].apply(lambda x: x.split("  l  ")[0].replace("원","").replace(",",""))
        except:
            try:
                t1.columns=['---']
                t1['액면가 | 매매단위']=None
            except:
                
        t1['기준일자']=dt
        t1['종가']=cp
        t1['유통주식수']=dstr
        t1.index=[code]
        
        "t2~t6 확보 방법 비공개"
        
        tn=pd.concat([t1, t2, t3, t4, t5, t6],axis=1)
        return tn, dt
    
# task=navy_main()
# task.__main__(code='000020')