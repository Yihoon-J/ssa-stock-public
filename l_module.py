# from rds_link import RDS_Link
from pykrx import stock
import datetime
import pandas as pd
import boto3
from rds_link import RDS_Link

#종목 정보 S3 저장 모듈


class l_main:
    def __init__(self):
        self.s3_bucket='---'
        
    def __main__(self):
        self.get_cri_ymd()
        self.get_ticker()
        
    def get_cri_ymd(self):#티커 조회 기준 일자 생성
        
                
    def get_ticker(self): #티커 및 종목명 dataframe 생성
        
        tickers_df['lmda'].iloc[:1000]='4' #금요일 실행
        tickers_df['lmda'].iloc[1000:2000]='5' #토요일 실행
        tickers_df['lmda'].iloc[2000:]='6' #일요일 실행
        
        if len(tickers_df)>2990:
            print('종목 개수 2990개 이상, 검색량 확보 재작업 필요!!')
        else:
            print("종목 개수:", len(tickers_df))
            self.s3_saving(tickers_df)
    
    def s3_saving(self, data):
        bucket=self.s3_bucket
        s3=boto3.client('s3')
        data.to_pickle('tickers.pkl')
        s3.upload_file("tickers.pkl", bucket, "tickers.pkl")
        print('s3 save success')
# if __name__ == '__main__':
    # task=l_main()
    # task.__main__()