import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import boto3

'''
완성된 파일 최종 메일 발송 로직
'''

class m_main():
    def __init__(self):
        smtp_addr="smtp.gmail.com"
        port=00000000
        self.smtp=smtplib.SMTP_SSL(smtp_addr, port)
        
    def __main__(self, ts):
        self.login()
        self.msg=self.prep()
        self.write(msg=self.msg)
        self.getfile(savetime=ts)
        self.attach(msg=self.msg)
        self.complete(id=self.id, msg=self.msg)
    
    def login(self):
        self.smtp.login(id, pw)
        self.id=id
    
    def prep(self): #제목, 수신인, 발신인 세팅
        msg=MIMEMultipart()
        msg['Subject']=f"주가 RPA 데이터 전달"
        msg['From']=""
        msg['To']=""
        return msg
    
    def write(self, msg):
        content="..."
        content_part=MIMEText(content, "plain")
        msg.attach(content_part)
        return msg
    
    def getfile(self,savetime):
        s3=boto3.client('s3')
        #download from s3
        s3.download_file('---', f'result_{savetime}.xlsx', 'RPA_RESULT.xlsx') #S3 파일명 - 저장할 파일명 순
        
    def attach(self, msg): #파일 첨부
        fnm='RPA_RESULT.xlsx'
        with open(fnm, 'rb') as xf:
            att=MIMEApplication(xf.read())
            att.add_header('Content-Disposition','attachment',filename=fnm)
            msg.attach(att)
        return msg
    
    def complete(self, id, msg):
        #메일 전송
        self.smtp.sendmail(id, to_mail, msg.as_string())
        self.smtp.quit()
        print('메일 발송 완료')

# if __name__ == '__main__':    
    # task=m_main()
    # task.__main__(savetime="2024-04-02 18:27:55.923053")