from rds_link import RDS_Link
import numpy as np
import time
import threading

#완성된 dataframe을 저장
class v_main:
    def __init__(self, ynb, rq, ry, g):
        self.ynb=ynb.reset_index(names='STCK_CODE').replace({np.nan: '', np.inf: '', -np.inf:'', 'nan':''})
        self.rq=rq.reset_index(names='CRI_YM').replace({np.nan: '', np.inf:'', -np.inf:'', 'nan':''})
        self.ry=ry.reset_index(names='CRI_YM').replace({np.nan: '', np.inf: '', -np.inf:'', 'nan':''})
        self.g=g.reset_index(names='CRI_YM').replace({np.nan: '', np.inf: '', -np.inf:'', 'nan':''})
        
    def __main__(self):
        time.sleep(3)
        threads=[]
        for tablecolor in ['YNB', 'RQ', 'RY', 'G']:
            t=threading.Thread(target=self.insert_db, args=(tablecolor,))
            t.start()
            threads.append(t)
        for t in threads:
            t.join()
        print('RDS 적재 성공')
        
    def insert_db(self, color):
        if color=='YNB':
            table=self.ynb
            db='yn'
        elif color=='RQ':
            table=self.rq
            db='rq'
        elif color=='RY':
            table=self.ry
            db='ry'
        elif color=='G':
            table=self.g
            db='g'
        print(f"{color} 적재 시작")
        for i in range(len(table)):
            rw=table.iloc[i]
            self.insert_row(rw, table=db)
        
        
    def insert_row(self, row, table):
        ynb_insert="INSERT INTO YNB VALUES\
            (%s, %s, %s, %s, %s, %s, %s, %s,\
            %s, %s, %s, %s, %s, %s, %s, %s,\
            %s, %s, %s, %s, %s, %s, %s, %s,\
            %s, %s, %s, %s)"
        red_q_insert="INSERT INTO RED_Q VALUES\
            (%s, %s, %s, %s, %s, %s, %s, %s,\
            %s, %s, %s, %s, %s, %s, %s, %s,\
            %s, %s)"
        red_y_insert="INSERT INTO RED_Y VALUES\
            (%s, %s, %s, %s, %s, %s, %s, %s,\
            %s, %s, %s, %s, %s, %s, %s, %s,\
            %s, %s, %s)"
        green_insert="INSERT INTO GREEN VALUES\
            (%s, %s, %s, %s, %s, %s, %s, %s,\
            %s, %s, %s)"
                    
        if table=="yn":
            #중복적재 방지: 동일 날짜로 두번 쌓는 것만 막으면 괜찮음. 로그 위해 중복적재 필요
            #보통주 아니면 적재 x
            cri_ym=row['기준일자']
            stck_code=str(row['STCK_CODE'])
            stck_type=str(row['stck_kind_cd'])
            try:
                current_maxdate=RDS_Link.mysql_load(f"SELECT MAX(CRI_YMD) FROM YNB WHERE STCK_CD={stck_code}")
                current_maxdate=current_maxdate[current_maxdate.columns[0]].iloc[0]
            except:
                #테이블 아예 비어 있으면...
                current_maxdate='20240401'
            if (cri_ym!='' and int(current_maxdate)<int(cri_ym)): #테이블의 현재 최대일자보다 불러오는 일자 값이 더 큰 경우에만 적재
                RDS_Link().mysql_execute(sql=ynb_insert, val=
                (row['기준일자'], '적재 컬럼 비공개'))
        elif table=="rq":
            # (종목코드, ----) 조합에 대해 unique 보장 필요. 최신 업데이트 값만 쓸 거니까, 해당 조합에 대해 겹치는 것 있으면 지워준 후에 append
            cri_ym=str(row['CRI_YM'])
            stck_code=str(row['종목코드'])
            RDS_Link().mysql_execute(sql="DELETE FROM RED_Q WHERE CRI_YM=%s AND STCK_CODE=%s", val=(cri_ym, stck_code))

            RDS_Link().mysql_execute(sql=red_q_insert, val=
            (row['UPDT_YMD'], '적재 컬럼 비공개']))
        
        elif table=="ry": 
            # (종목코드, ----) 조합에 대해 unique 보장 필요. 최신 업데이트 값만 쓸 거니까, 해당 조합에 대해 겹치는 것 있으면 지워준 후에 append
            cri_ym=str(row['CRI_YM'])
            stck_code=str(row['종목코드'])
            RDS_Link().mysql_execute(sql="DELETE FROM RED_Y WHERE CRI_YM=%s AND STCK_CODE=%s", val=(cri_ym, stck_code))
            
            RDS_Link().mysql_execute(sql=red_y_insert, val=
            (row['UPDT_YMD'], '적재 컬럼 비공개'))
        
        elif table=="g":
            #현재 row의 기준일자와 종목 확인한 후 적재
            try: cri_ym=int(row['기준일자']) #input row의 일자
            except: cri_ym=200000 #정상 로드 안 되는 경우에는 적재 X
            stck_code=row['종목코드'] #input row의 종목코드
            
            current_maxdate=RDS_Link().mysql_load(f"SELECT MAX(CRI_YM) FROM GREEN WHERE STCK_CODE={stck_code}")
            current_maxdate=current_maxdate[current_maxdate.columns[0]].iloc[0]
            if current_maxdate==None:
                current_maxdate='202212'
                
            if int(current_maxdate)<cri_ym:
                RDS_Link().mysql_execute(sql=green_insert, val=
                    (row['종목코드'], '적재 컬럼 비공개'))
