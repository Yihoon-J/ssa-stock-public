from s_module import s_main
from r_module import r_main
from v_module import v_main
from e_module import e_main
from x_module import x_main
from m_module import m_main
import boto3
import pickle


class ssa_stock():
    def __init__(self):
        pass
    
    def __main__(self): #lambda handler function
        
        # 01 s_module: 증권사 API 사용 기본 세팅
        self.app_key, self.app_sec, self.acc_key= s_main().__main__(newkey=newkey)
        
        # 02 l_module에서 생성된 티커 목록을 s3에서 로드
        s3=boto3.client('s3')
        s3.download_file('---', 'tickers.pkl', 'tickers.pkl')
        with open('tickers.pkl', 'rb') as f:
            tickers = pickle.load(f)
        self.tickers_df=tickers
        
        #03 r_module: 종목별 정보 확보
        ynb, rq, ry, g= r_main(self.app_key, self.app_sec, self.acc_key).__main__(tickers=self.tickers_df)
       
        #04 v_module: 완성된 데이터를 DB 적재
        v_main(ynb, rq, ry, g).__main__()
        
        #05 e_module: RDS에서 기본 데이터 포맷 생성
        e_main().__main__()
        
        #06 x_module: 임시 저장된 엑셀 파일 가공한 후 S3 저장
        savetime=x_main().__main__()
        
        #07 m_module: S3에서 파일 내려받아서 이메일 전송
        m_main().__main__(ts=savetime)

if __name__ == '__main__':
    task=ssa_stock()
    task.__main__()