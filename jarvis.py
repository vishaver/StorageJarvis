import splunklib.client as client
import sys
from time import sleep
import splunklib.results as results
import pandas as pd
from collections import OrderedDict, Counter
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from datetime import datetime
import boto3
from boto3.s3.transfer import S3Transfer
from boto3.session import Session
import random, string
sns.set(rc={'figure.figsize':(8, 4)})

class splunk:
    sessid = ''.join(random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits) for _ in range(16))
    HOST = "splunkserver"
    PORT = "9443"
    USERNAME =  "username"
    PASSWORD = "passwrod"
    OWNER="username"
    APP="Isilon_app"
    kwargs_normalsearch = {"exec_mode": "normal"}
    def __init__(self,stgName,alertFrom,alertType):
        self.stgName = stgName
        self.alertType = alertType
        self.alertFrom = alertFrom

    def queryMapping(self,stgName):
        print("I am in queryMapping")
        if self.alertType == "CPU" and self.alertFrom == "SAN":
            cacheQuery = "search source=/apps/python3/hitachi/1m/cache_cliper1_" + stgName + " earliest=-2h latest=now| timechart span=2m latest(CACHE_MEMORY_USAGE_RATE) as CACHE_MEMORY_USAGE_RATE latest(CACHE_WRITE_PENDING_RATE) as CACHE_WRITE_PENDING_RATE by CLPR_NUMBER"
            cpuQuery = "search source=/apps/python3/hitachi/1m/processor_2_" + stgName + " earliest=-2h latest=now | timechart span=2m latest(PROCESSOR_BUSY_RATE) by ADAPTOR_ID"
            topLdevIOPS = 'search source=/apps/python3/hitachi/5m/ldev_stat3_' + stgName + ' earliest=-2h latest=now | eval READ_IO_RATE=tonumber(READ_IO_RATE) | eval WRITE_IO_RATE=tonumber(WRITE_IO_RATE) | eval TOTAL_IO_RATE=READ_IO_RATE+WRITE_IO_RATE | table _time LDEV_NUMBER READ_IO_RATE WRITE_IO_RATE TOTAL_IO_RATE |  dedup _time LDEV_NUMBER | sort 0 -num(TOTAL_IO_RATE) | fields - READ_IO_RATE,WRITE_IO_RATE | timechart span=5m latest(TOTAL_IO_RATE) by LDEV_NUMBER limit=1000000 | stats  avg(*) | transpose 0 | sort -num("row 1") | rename column as LDEV | rename "row 1" as IO_RATE | rex field=LDEV mode=sed "s/avg\(//g" | rex field=LDEV mode=sed "s/\)//g" | head 10'
            topLdevTranferRate = 'search source=/apps/python3/hitachi/5m/ldev_stat3_' + stgName + ' earliest=-2h latest=now | eval READ_XFER_RATE=tonumber(READ_XFER_RATE) | eval WRITE_XFER_RATE=tonumber(WRITE_XFER_RATE) | eval TOTAL_XFER_RATE=READ_XFER_RATE+WRITE_XFER_RATE | table _time LDEV_NUMBER READ_XFER_RATE WRITE_XFER_RATE TOTAL_XFER_RATE | dedup _time LDEV_NUMBER  | sort 0 -num(TOTAL_XFER_RATE) | fields - READ_XFER_RATE,WRITE_XFER_RATE | timechart span=5m latest(TOTAL_XFER_RATE) by LDEV_NUMBER limit=1000000 | stats  avg(*) | transpose 0 | sort -num("row 1") | rename column as LDEV | rename "row 1" as XferRate | rex field=LDEV mode=sed "s/avg\(//g" | rex field=LDEV mode=sed "s/\)//g" | head 10'
            system_IO_Rate = 'search source=/apps/python3/hitachi/5m/storage_system_stats1_' + stgName + ' earliest=-2h latest=now | timechart span=5m latest(READ_IO_RATE) as READ_IO_RATE latest(WRITE_IO_RATE) as WRITE_IO_RATE | eval READ_IO_RATE=round(READ_IO_RATE,2) | eval WRITE_IO_RATE=round(WRITE_IO_RATE,2) | rename READ_IO_RATE as Sytem_Read_IO_RATE,WRITE_IO_RATE as System_Write_IO_Rate'
            system_Tranfer_Rate = 'search source=/apps/python3/hitachi/5m/storage_system_stats1_' + stgName + ' earliest=-2h latest=now | timechart span=5m latest(READ_XFER_RATE) as READ_XFER_RATE latest(WRITE_XFER_RATE) as WRITE_XFER_RATE | eval READ_XFER_RATE=round(READ_XFER_RATE,2) | eval  WRITE_XFER_RATE= round(WRITE_XFER_RATE,2) | rename READ_XFER_RATE as System_Read_TranferRate | rename WRITE_XFER_RATE as System_Write_TransferRate'

            query = {"cache":[cacheQuery,"CACHE_WRITE_PENDING_RATE: 0","CACHE_MEMORY_USAGE_RATE: 0","Cache Stats","Scatter"],"CPU":[cpuQuery,"MPU-10","MPU-11","MPU-20","MPU-21","CPU Stats","Scatter"],"topLdevIOPS":[topLdevIOPS,"LDEV","IO_RATE","Top ldev IO_Rate","bar"],"topLdevTranferRate":[topLdevTranferRate,"LDEV","XferRate","Top ldev TranferRate","bar"],"system_IO_Rate":[system_IO_Rate,"Sytem_Read_IO_RATE","System_Write_IO_Rate","System wide IO_Rate","Scatter"],"system_Tranfer_Rate":[system_Tranfer_Rate,"System_Read_TranferRate","System_Write_TransferRate","Sytem wide Transfer Rate","Scatter"]}
            #query = {"cache":[cacheQuery,"CACHE_WRITE_PENDING_RATE: 0","CACHE_MEMORY_USAGE_RATE: 0","Cache Stats","Scatter"],"CPU":[cpuQuery,"MPU-10","MPU-11","MPU-20","MPU-21","CPU Stats","Scatter"],"topLdevIOPS":[topLdevIOPS,"LDEV","IO_RATE","Top ldev IO_Rate","bar"],"topLdevTranferRate":[topLdevTranferRate,"LDEV","XferRate","Top ldev TranferRate","bar"]}
            self.myQuery(**query)

    def runQurey(self,keys,*queryname):
        print("I am in runQurey")
        self.service = client.connect(host=self.HOST,port=self.PORT,username=self.USERNAME,password=self.PASSWORD,owner=self.OWNER,app=self.APP)
        job = self.service.jobs.create(queryname[0],**self.kwargs_normalsearch)
        while True:
            if job.is_done() == True:
                break
        output=results.ResultsReader(job.results(count=0))
        job.cancel()
        self.myDataframe(output,keys,*queryname)

    def myDataframe(self,output,keys,*queryname):
        print("I am in myDataframe")
        l = []
        for j in output:
            l.append(j)
        df = pd.DataFrame(l)
        print(df)
        if queryname[-1] == "Scatter":
            df._time = pd.to_datetime(df._time)
            jk1 = df.dropna().set_index('_time').astype(float)
            self.myGraph(jk1,keys,*queryname)
        elif queryname[-1] == "bar":
            jk1 = df.dropna()
            self.myGraph(jk1,keys,*queryname)

    def myGraph(self,jk1,keys,*queryname):
        print("I am in myGraph")
        if keys == "cache" or keys == "system_IO_Rate" or keys == "system_Tranfer_Rate":
            t=[queryname[1],queryname[2]]
            jk1[t].plot(marker='o', linestyle='-')
            plt.savefig(keys)
            self.objectUpload(keys)
        elif keys == "CPU":
            t=[queryname[1],queryname[2],queryname[3],queryname[4]]
            jk1[t].plot(marker='o', linestyle='-')
            plt.savefig(keys)
            self.objectUpload(keys)
        elif keys == "topLdevIOPS" or keys == "topLdevTranferRate":
            print("topldev")
            jk1[queryname[2]] = jk1[queryname[2]].astype(float)
            print(jk1.dtypes)
            jk1.plot.bar(x=jk1.columns[0],y=jk1.columns[1],rot=20)
            plt.savefig(keys)
            self.objectUpload(keys)

    def objectUpload(self,keys):
        print("I am in objectUpload")
        session = Session(aws_access_key_id = '*************',aws_secret_access_key = '*****************')
        s3 = session.resource('s3')
        client = session.client('s3',endpoint_url = 'https://s3.domain.com')
        transfer = S3Transfer(client)
        transfer.upload_file(keys + ".png",'jarvis',keys + "_" + self.sessid,extra_args={'ACL':'public-read'})


    def collectImageName(self,sessid):
        print("I am in collectImageName")
        session = Session(aws_access_key_id = '*****************',aws_secret_access_key = '************************')
        s3 = session.resource('s3')
        client = session.client('s3',endpoint_url = 'https://s3.domain.com')
        test = client.list_objects(Bucket='jarvis')['Contents']
        graphList = [j['Key'] for j in test if self.sessid in j['Key']]
        self.mailData(graphList)

    def myQuery(self,**kwargs):
        print("I am in myQuery")
        for keys,values in kwargs.items():
            self.runQurey(keys,*values)
            sleep(5)

    def mailData(self,graphList):
        print("I am in mailData")
        graphs = ["https://jarvis.s3.domain.com/" + jk for jk in graphList]
        template = (''
        '<a href="{graph_url}" target="_blank">' # Open the interactive graph when you click on the image
        '<img src="{graph_url}">'        # Use the ".png" magic url so that the latest, most-up-to-date image is included
        '</a>'
        '{caption}'                              # Optional caption to include below the graph
        '<br>'                                   # Line break
        '<br>'
        '<hr>'                                   # horizontal line
        '')

        email_body = ''
        for graph in graphs:
            _ = template
            _ = _.format(graph_url=graph, caption='')
            email_body += _
        me  = 'xyz@domain.com'
        recipient = 'user@domain.com'
        subject = 'Graph Report'

        email_server_host = 'relay1.domain.com'
        port = 25
        import smtplib
        from email.mime.multipart import MIMEMultipart
        from email.mime.text import MIMEText
        import os

        msg = MIMEMultipart('alternative')
        msg['From'] = me
        msg['To'] = recipient
        msg['Subject'] = subject

        msg.attach(MIMEText(email_body, 'html'))

        server = smtplib.SMTP(email_server_host, port)
        server.sendmail(me, recipient, msg.as_string())
        server.close()
