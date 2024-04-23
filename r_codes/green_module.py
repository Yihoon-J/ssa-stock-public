import bs4
import requests
import pandas as pd
import re

class green_main():
    def __main__(self, code):
        green_sheet=self.create_table(code=code)
        # print(green_sheet)
        return green_sheet
            
    def create_table(self, code):
        #일단 연결 기준
        url=f"---"
        self.soup = bs4.BeautifulSoup(requests.get(url).content, 'html.parser')
        
        bd, hd, tg = self.give_selector(type='1')
        table1=self.parse_table(hd, bd, tg)
        
        bd, hd, tg = self.give_selector(type='2')
        table2=self.parse_table(hd, bd, tg)
        
        bd, hd, tg = self.give_selector(type='3')
        table3=self.parse_table(hd, bd, tg)
        
        table=pd.concat([table1, table2, table3], axis=1)
        table['종목코드']=code
        
        table=table.reset_index(names='기준일자')
        table['기준일자']=table['기준일자'].apply(lambda x: re.sub(r'[^0-9.]', '', x))
        # table=table.loc[table[['기준일자']!='']]
        
        
        table=table.rename({'---':'---'},
                           axis=1)
        return table
        
        
    def give_selector(self, type):
        if type=='1': #포괄손익계산서
            #연결 포괄손익계산서
            
        elif type=='2': #재무상태표

        elif type=="3": #현금흐름표
            
        return bd, hd, tg
        
        
    def parse_table(self, header, body, targetcol):
        try:
            thead=self.soup.select(header)[0].text.split(sep="\n")
            thead=[x for x in thead if x != '']
            tbody=self.soup.select(body)[0]
            result=pd.DataFrame()
            for j in targetcol:
                i=0
                find=False
                cand=tbody.find_all('tr')
                try:
                    #상세 전처리 로직 비공개
                except: pass
            if len(result)!=0:
                result.columns=result.iloc[0]
                result.index=thead
                result=result.drop(result.index[0])
                try:
                    result=result.drop(['전년동기', '전년동기(%)'])
                except: pass
        except:
            result=pd.DataFrame(columns=[], index=['200000'])
        return result


# if __name__ == '__main__':          
    # task=green_main()
    # task.__main__(code='000120')