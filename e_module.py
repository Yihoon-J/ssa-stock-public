from rds_link import RDS_Link
import datetime
import pandas as pd

#RDS에 저장된 테이블 엑셀 가공 위해 로드하는 로직

class e_main():
    def __main__(self):
        criym=self.load_tables()
        self.build_tables(criym)


    def load_tables(self):
        
        #YELLOW
        sql1=\
            "SELECT xxx,xxx,xxx,xxx FROM xxx WHERE xxx NOT LIKE 'xx' GROUP BY xx HAVING MAX(xx);"

        #NAVY
        sql2=\
            "WITH BASE AS(\
                SELECT xx,xxx,xx,xx,x,xx,xx,xx,xx\
                FROM YNB\
                    LEFT JOIN xxx\
                    ON YNB.xxxx = SEARCH.xx)\
            SELECT xxx xxx,\
                    xxx,\
                    xxx,\
                    CAST(xxx AS SIGNED) 'xxx',\
                    SUBSTR(xxx,1,3) xxx,\
                    CAST(SUBSTR(xxx,5,11) AS UNSIGNED) 'xxx',\
                    CAST(xxx AS UNSIGNED) 'xxx',\
                    CAST(xxx AS UNSIGNED) 'xxx',\
                    CAST(xxx AS UNSIGNED) xxx,\
                    CAST(xxx AS UNSIGNED) 'xxx',\
                    CAST(xxx AS UNSIGNED) 'xxxx',\
                    CAST(xxx AS UNSIGNED) 'xxx',\
                    CAST(xxx AS DECIMAL(5,2)) 'xxx',\
                    CAST(xxx AS UNSIGNED) 'xxx',\
                    CAST(xxx AS UNSIGNED) 'xxx)',\
                    CAST(xxx AS DECIMAL(6,2)) 'xxx',\
                    xxx 'xxx',\
                    xxx 'xxx',\
                    xxx 'xxx'\
                FROM BASE\
                GROUP BY xxx\
                HAVING MAX(xxx);\
            "

        #RED_Y
        sql3=\
            "WITH T0 AS(\
                SELECT * FROM xxx WHERE xxx='A' GROUP BY xxx, xxx HAVING MAX(xxx) AND xxx<xxx\
                            AND xxx>xxx)\
            SELECT\
                CONCAT(\
                    'FY',\
                    CASE WHEN SUBSTR(xxx,5,2)<7 THEN SUBSTR(xxx,3,2)-1 ELSE SUBSTR(xxx,3,2) END\
                ) CRI_YM,\
                xxx,\
                xxx xxx,\
                CASE WHEN xxx='' THEN NULL ELSE CAST(xxx AS SIGNED) END 'xxx',\
                CASE WHEN xxx='' THEN NULL ELSE CAST(xxx AS SIGNED) END 'xx',\
                CASE WHEN xxx='' THEN NULL ELSE CAST(xxx AS SIGNED) END 'xxx',\
                CASE WHEN xxx='' THEN NULL ELSE CAST(xxx AS DECIMAL(11,2)) END 'xxx',\
                CASE WHEN xxx='' THEN NULL ELSE CAST(xxx AS DECIMAL(11,2)) END 'xxx',\
                CASE WHEN xxx='' THEN NULL ELSE CAST(xxx AS DECIMAL(11,2)) END 'xxx',\
                CASE WHEN xxx='' THEN NULL ELSE CAST(xxx AS DECIMAL(11,2)) END 'xxx',\
                xxx - LAG(xxx,1)OVER(PARTITION BY xxx ORDER BY xxx) 'xxx',\
                ROUND(100*(xxx - LAG(xxx,1)OVER(PARTITION BY xxx ORDER BY xxx)) / LAG(xxx,1) OVER(PARTITION BY xxx ORDER BY xxx),2)  AS 'xxx',\
                xxx - LAG(xxx,1) OVER(PARTITION BY xxx ORDER BY xxx) 'xxx',\
                ROUND(100*(xxx - LAG(xxx,1) OVER(PARTITION BY xxx ORDER BY xxx)) / LAG(xxx,1) OVER(PARTITION BY xxx ORDER BY xxx),2)  AS 'xxx',\
                CASE WHEN xxx='' THEN NULL ELSE CAST(xxx AS SIGNED) END 'xxx)',\
                CASE WHEN xxx='' THEN NULL ELSE CAST(xxx AS SIGNED) END 'xxx',\
                CASE WHEN xxx='' THEN ROUND(xxx/xxx,2) ELSE CAST(xxx AS DECIMAL(6,2)) END 'xxx',\
                CASE WHEN xxx='' THEN NULL ELSE CAST(xxx AS DECIMAL(5,2)) END 'xxx',\
                CASE WHEN xxx='' THEN NULL ELSE CAST(xxx AS DECIMAL(5,2)) END 'xxx',\
                CASE WHEN xxx='' THEN NULL ELSE CAST(xxx AS DECIMAL(8,2)) END 'xxx'\
            FROM T0\
            ORDER BY xxx, xxx;\
            "    
        #RED_Q
        sql4=\
            "WITH T0 AS(\
                SELECT * FROM xxx GROUP BY xxx, xxx HAVING MAX(xxx) AND xxx>xxx)\
            SELECT\
                CASE WHEN SUBSTR(xxx, 5,2) IN(2,3) THEN CONCAT(SUBSTR(xxx, 3,2),'/1Q')\
                    WHEN SUBSTR(xxx, 5,2) IN(5,6) THEN CONCAT(SUBSTR(xxx, 3,2),'/2Q')\
                    WHEN SUBSTR(xxx, 5,2) IN (8,9) THEN CONCAT(SUBSTR(xxx, 3,2),'/3Q')\
                    WHEN SUBSTR(xxx, 5,2) IN (11,12) THEN CONCAT(SUBSTR(xxx, 3,2),'/4Q')\
                ELSE xxx END 'xxx',\
                xxx xxx,\
                CASE WHEN xxx='' THEN NULL ELSE CAST(xxx AS SIGNED) END 'xxx',\
                CASE WHEN xxx='' THEN NULL ELSE CAST(xxx AS SIGNED) END 'xxx',\
                CASE WHEN xxx='' THEN NULL ELSE CAST(xxx AS SIGNED) END 'xxx',\
                CASE WHEN xxx='' THEN NULL ELSE CAST(xxx AS DECIMAL(11,2)) END 'xxx',\
                CASE WHEN xxx='' THEN NULL ELSE CAST(xxx AS DECIMAL(11,2)) END 'xxx',\
                CASE WHEN xxx='' THEN NULL ELSE CAST(xxx AS DECIMAL(11,2)) END 'xxx',\
                SALE_ACNT - LAG(xxx,1)OVER(PARTITION BY xxx ORDER BY xxx) 'xxx',\
                ROUND(100*(xxx - LAG(xxx,1)OVER(PARTITION BY xxx ORDER BY xxx)) / LAG(xxx,1) OVER(PARTITION BY xxx ORDER BY xxx),2)  AS 'xxx',\
                xxx - LAG(xxx,1) OVER(PARTITION BY xxx ORDER BY CRI_YM) 'xxx',\
                ROUND(100*(xxx - LAG(xxx,1) OVER(PARTITION BY xxx ORDER BY xxx)) / LAG(xxx,1) OVER(PARTITION BY xxx ORDER BY xxx),2)  AS 'xxx',\
                CASE WHEN xxx='' THEN NULL ELSE CAST(xxx AS SIGNED) END 'xxx',\
                CASE WHEN xxx='' THEN NULL ELSE CAST(xxx AS SIGNED) END 'xxx',\
                CASE WHEN xxx='' THEN NULL ELSE CAST(xxx AS SIGNED) END 'xxx',\
                ROUND(xxx/(xxx+LAG(xxx,1)OVER(PARTITION BY xxx ORDER BY xxx)\
                    +LAG(xxx,2)OVER(PARTITION BY xxx ORDER BY xxx)\
                        +LAG(xxx,3)OVER(PARTITION BY xxx ORDER BY xxx)\
                            +LAG(xxx,4)OVER(PARTITION BY xxx ORDER BY xxx)),2) 'xxx',\
                CASE WHEN xxx='' THEN NULL ELSE CAST(xxx AS DECIMAL(5,2)) END 'xxx',\
                CASE WHEN xxx='' THEN NULL ELSE CAST(xxx AS DECIMAL(5,2)) END 'xxx',\
                CASE WHEN xxx='' THEN NULL ELSE CAST(xxx AS DECIMAL(8,2)) END 'xxx'\
            FROM T0\
            ORDER BY xxx, xxx;\
            "
        
        #GREEN
        criym=str(int(datetime.date.today().year)-1)+'12'    
        sql5=f"\
            WITH GREEN_RAW AS(\
                SELECT xxx,\
                    xxx,\
                    CAST(xxx AS SIGNED) xxx,\
                    CAST(xxx AS SIGNED) xxx,\
                    CAST(xxx AS SIGNED) xxx,\
                    CAST(xxx AS SIGNED) xxx,\
                    CASE WHEN xxx=0 THEN NULL ELSE CAST(xxx AS SIGNED) END xxx,\
                    CASE WHEN xxx=0 THEN NULL ELSE CAST(xxx AS SIGNED) END xxx,\
                    CAST(xxx AS SIGNED) xxx,\
                    CAST(xxx AS SIGNED) xxx,\
                    CAST(xxx AS SIGNED) xxx\
                    FROM xxx WHERE xxx={criym}),\
            ROE_TABLE AS(\
                WITH ROE_RAW AS(\
                    SELECT xxx, xxx,\
                            xxx 'xxx',\
                            LAG(xxx,1) OVER(PARTITION BY xxx ORDER BY xxx) 'xxx',\
                            LAG(xxx,2) OVER(PARTITION BY xxx ORDER BY xxx) 'xxx'\
                            FROM xxx)\
                SELECT xxx,\
                CAST(ROUND((3*xxx+2*xxx+1*xxx)/6,2)\
                    AS DECIMAL(8,2)) xxx,\
                xxx\
                FROM xxx\
                WHERE xxx={criym})\
            SELECT G.xxx xxx,\
                    xxx 'xxx',\
                    xxx 'xxx',\
                    xxx'xxx',\
                    xxx 'xxx',\
                    xxx 'xxx',\
                    xxx 'xxx',\
                    xxx 'xxx',\
                    xxx 'xxx',\
                    xxx 'xxx',\
                    xxx 'xxx'\
                FROM GREEN_RAW G\
                LEFT JOIN ROE_TABLE R\
                ON G.xxx = R.xxx;"
        
        self.t1=RDS_Link().mysql_load(sql1).set_index('xxx')
        self.t2=RDS_Link().mysql_load(sql2).set_index('xxx')
        self.t3=RDS_Link().mysql_load(sql3).set_index(['xxx', 'xxx'])
        self.t4=RDS_Link().mysql_load(sql4).set_index(['xxx', 'xxx'])
        self.t5=RDS_Link().mysql_load(sql5).set_index('xxx')
        return criym

    def build_tables(self, criym):
        #로드된 테이블 가공 로직 비공개
        #결과 concat
  
        result=pd.concat([self.t1[self.t1.columns[0]],
                          self.t3,
                          self.t4,
                          self.t2,
                          self.t1[self.t1.columns[1:]],
                          self.t5],
                         axis=1)
        result=result.loc[result[result.columns[1]].notna().index] #종목명 안 나오는 것 제외
        result=result.loc[result[result.columns[-37]]!='창업투자']
        result['xxx']=result['xxx'].apply(lambda x: x.zfill(6))
        result.set_index('xxx', inplace=True)
        result.to_excel('./rds_result.xlsx')
        del result

# if __name__ == '__main__':         
    # task=e_main()
    # task.__main__()
