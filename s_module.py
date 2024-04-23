import json
import requests
import sys

#증권사 API 키 발급 로직
class s_main:
    def __init__(self):
        self.app_key="---" #App Key
        self.app_sec="--" #App Secret
        self.base_domain="--"
        
    def __main__(self,newkey):
        if newkey:
            self.acc_key=self.get_token()
        else: #가장 마지막 키
            self.acc_key="키 수동 입력시 사용"
        return self.app_key, self.app_sec, self.acc_key
        
    def get_token(self): #key 요청할 경우 접속 키 발급
        token_url=""
        addr=self.base_domain+token_url
        headers = {"content-type":"application/json"}
        body = {"grant_type":"client_credentials",
                "appkey":self.app_key,
                "appsecret":self.app_sec}
        res = requests.post(addr, headers=headers, data=json.dumps(body))
        try:
            self.acc_key = res.json()["access_token"]
            print("신규 키 발급\n{}".format(self.acc_key))
        except:
            print('신규 키 발급 실패')
            print(res.json())
            self.acc_key=None
        return self.acc_key

# if __name__ == '__main__':   
    # task=s_main()
    # task.__main__(newkey=True)