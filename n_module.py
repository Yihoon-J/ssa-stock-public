import datetime
import json
from rds_link import RDS_Link
from dateutil.relativedelta import relativedelta
import urllib.request
import pandas as pd
import numpy as np
import boto3
import pickle

#검색량 데이터 확보

class n_main():
    def __init__(self):
        self.client_id = "";
        self.client_secret = "";
        self.url = "";
        
    def __main__(self):
        self.read_tickers()
        self.actions()
    
    def read_tickers(self): #오늘 돌려야 하는 종목명 확보
        s3=boto3.client('s3')
        s3.download_file('---', 'tickers.pkl', 'tickers.pkl')
        with open('tickers.pkl', 'rb') as f:
            tickers = pickle.load(f)
        todaydate=str(datetime.date.today().weekday()) #Monday is 0 and Sunday is 6
        if todaydate==4: #금요일 첫 실행 전 검색어 리셋
            RDS_Link().mysql_execute("TRUNCATE SEARCH;") #검색어 테이블은 사이클마다 비워야 함
        self.tickers=tickers.loc[tickers['lmda']==todaydate]
        #columns=ticker, name, lmda
    
    def actions(self): #각 종목명에 대해 검색량 집계 및 적재 액션 수행
        self.set_date()
        for i in range(len(self.tickers)):
            # for i in range(5): #FOR TEST ONLY
            name=self.tickers.iloc[i]['name']
            self.build_body(kwd=name)
            self.get_request()
            self.agg_values()
            self.saving(kwd=name)

    def set_date(self): # 검색 대상 및 집계 구분 날짜값 정보 확보
        
        
    def build_body(self, kwd): #json body 생성
        self.body = json.dumps({
            "startDate": self.startdate,
            "endDate": self.enddate,
            "timeUnit": 'date',
            "keywordGroups": [{'groupName':'g1','keywords':[kwd]}]
            }, ensure_ascii=False)

    def get_request(self): #요청 전송하고 response 확보
        request = urllib.request.Request(self.url)
        request.add_header("",self.client_id)
        request.add_header("",self.client_secret)
        request.add_header("Content-Type","application/json")
        response = urllib.request.urlopen(request, data=self.body.encode("utf-8"))
        self.response_json=json.loads(response.read())
        
    def agg_values(self): #월 단위로 평균 검색량 집계
        
    
    def saving(self, kwd): #집계된 값을 RDS로 저장
        sql="INSERT INTO SEARCH VALUES(%s, %s, %s, %s)"
        RDS_Link().mysql_execute(sql, (??????))

# if __name__ == '__main__':   
    # task=n_main()
    # task.__main__()