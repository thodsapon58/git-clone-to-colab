import datetime, pytz
tz = pytz.timezone('Asia/Bangkok')

def now():
    now1 = datetime.datetime.now(tz)
    month_name = 'x มกราคม กุมภาพันธ์ มีนาคม เมษายน พฤษภาคม มิถุนายน กรกฎาคม สิงหาคม กันยายน ตุลาคม พฤศจิกายน ธันวาคม'.split()[now1.month]
    thai_year = now1.year + 543
    time_str = now1.strftime('%H:%M:%S')
    return "%d %s %d %s"%(now1.day, month_name, thai_year, time_str) # 30 ตุลาคม 2560 20:45:30
    

     
!ls drive
!pip install pydrive
!pip install xlsxwriter
import pandas as pd
import xlsxwriter
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive 
from google.colab import auth 
from oauth2client.client import GoogleCredentials
auth.authenticate_user()
gauth = GoogleAuth()
gauth.credentials = GoogleCredentials.get_application_default()
drive = GoogleDrive(gauth)
