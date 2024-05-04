import requests
from rds_link import RDS_Link
import pandas as pd
import datetime

#금리스프레드 확보
class spread_data():
    def __main__(self):
        parsing, cm=self.load_decide()
        if parsing:
            self.parse(date=cm)
        
    
    def load_decide(self):
        #데이터 조회해서 업데이트 여부 결정
        parsing=False
        current_maxdate=RDS_Link().mysql_load("SELECT MAX(CRI_YMD) FROM  xxx").iloc[0][0]
        if current_maxdate==None:
            current_maxdate='xxx'
        today=datetime.date.today()
        if int(today.year)>=int(current_maxdate[:3])+2: #돌리는 날짜가 마지막 기재 연도보다 2 큰 경우
            parsing=True
        return parsing, int(current_maxdate)
            
    def parse(self, date):
        url='---'
        res=requests.get(url).text
        df=pd.read_html(res)[0]
        df=df[df.columns[:4]]
        df=pd.DataFrame(df.set_index('등급').stack()).reset_index()
        df.columns=['xxx','xxx', 'xxx']
        df['xxx']=df['xxx'].apply(lambda x: x.replace(".",""))
        df['xxx']=df['xxx'].apply(lambda x: int(x))
        df=df.loc[df['xxx']>date]
        sql="INSERT INTO xxx VALUES (%s, %s, %s)"
        for i in range(len(df)):
            row=df.iloc[i]
            RDS_Link().mysql_execute(sql=sql, val= (row['xxx'], row['xxx'], row['xxx']))

# if __name__ == '__main__':   
    # task=spread_data() 
    # task.__main__()
