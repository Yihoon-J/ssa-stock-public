import requests
import pandas as pd

class yellow_main:
    def __init__(self, appkey=None, appsec=None, acckey=None):
        self.base_domain="비공개"
        self.app_key=appkey
        self.app_sec=appsec
        self.acckey=acckey

    def __main__(self, code, name):
        info_sheet,stocktype, reitsny=self.get_stock_info(code=code)
        price_sheet=self.get_price2_sheet(code=code, type=stocktype, reits_ny=reitsny)
        concat_sheet=self.concat_df(info=info_sheet, price=price_sheet, code=code, name=name)
        return concat_sheet, stocktype, reitsny
    
    def api_base_structure(self, url, trid, code, info=False): #국내주식 시세조회 기본 구조
        addr=self.base_domain+url
        headers = {}
        if info: #info=True 즉 정보 조회 시
            params = {}
        else: #정보 외 (시세 등)조회 시
            params = {}
        try:
            res = requests.get(addr, headers=headers, params=params).json()['output']
        except:
            print("토큰 재발급 필요!")
        result=pd.json_normalize(res)
        result.index=[code]
        return result
    
    def get_stock_info(self, code):  #기본조회
        #상장주수, 주식종류
        trid='---'
        url='---'
        info_df=self.api_base_structure(trid=trid, url=url, code=code, info=True)
        target_col=['---']
        info_df=info_df[target_col]
        #리턴값 비공개

    def get_price2_sheet(self, code, type, reits_ny):  #주식시세(2)
        '로직 비공개'
    
    def concat_df(self, info, price, code, name):
        concatdf= pd.concat([info, price],axis=1)
        concatdf['종목명']=name
        concatdf['종목코드']=code
        return concatdf