import datetime, pytz
tz = pytz.timezone('Asia/Bangkok')

import pandas as pd
def now():
    now1 = datetime.datetime.now(tz)
    month_name = 'x มกราคม กุมภาพันธ์ มีนาคม เมษายน พฤษภาคม มิถุนายน กรกฎาคม สิงหาคม กันยายน ตุลาคม พฤศจิกายน ธันวาคม'.split()[now1.month]
    thai_year = now1.year + 543
    time_str = now1.strftime('%H:%M:%S')
    return "%d %s %d %s"%(now1.day, month_name, thai_year, time_str) # 30 ตุลาคม 2560 20:45:30
  
   

def import_gg_xlsx(file_id, file_name):
    from pydrive.auth import GoogleAuth
    from pydrive.drive import GoogleDrive 
    from google.colab import auth 
    from oauth2client.client import GoogleCredentials
    auth.authenticate_user()
    gauth = GoogleAuth()
    gauth.credentials = GoogleCredentials.get_application_default()
    drive = GoogleDrive(gauth)

    downloaded = drive.CreateFile({'id': file_id})
    downloaded.GetContentFile(file_name)
    df_new          = pd.read_excel(file_name)
    return df_new



def import_gg_csv(file_id, file_name):
    from pydrive.auth import GoogleAuth
    from pydrive.drive import GoogleDrive 
    from google.colab import auth 
    from oauth2client.client import GoogleCredentials
    auth.authenticate_user()
    gauth = GoogleAuth()
    gauth.credentials = GoogleCredentials.get_application_default()
    drive = GoogleDrive(gauth)

    downloaded = drive.CreateFile({'id': file_id})
    downloaded.GetContentFile(file_name)
    df_new = pd.read_csv(file_name ,  sep='\^'       )
    return df_new

  

#พ่นออกมาที่ชีทนี้
#https://docs.google.com/spreadsheets/d/1EaVFbZrNsiE9fT_DXrCoSN9PNZmzXte5WgB0SVT72PI/edit#gid=1020847031
def next_available_row(worksheet):
    str_list = list(filter(None, worksheet.col_values(1)))
    return str(len(str_list)+1)


def add_log(
    link_log,
    cell1,
    cell2,
    cell3,
    cell4,
    cell5,
    cell6

):
    
    from pydrive.auth import GoogleAuth
    from pydrive.drive import GoogleDrive 
    from google.colab import auth 
    from oauth2client.client import GoogleCredentials

    auth.authenticate_user()
    gauth = GoogleAuth()
    gauth.credentials = GoogleCredentials.get_application_default()
    drive = GoogleDrive(gauth)

    import gspread
    from gspread_dataframe import set_with_dataframe
    from gspread_dataframe import get_as_dataframe

    gc = gspread.authorize(GoogleCredentials.get_application_default())


    wb_logfile= gc.open_by_url(link_log)
    sheet1 = wb_logfile.worksheet('Logfile Report')
    next_row = next_available_row(sheet1)

    sheet1.update_acell("A{}".format(next_row), cell1)
    sheet1.update_acell("B{}".format(next_row), cell2)
    sheet1.update_acell("C{}".format(next_row), cell3)
    sheet1.update_acell("D{}".format(next_row), cell4)
    sheet1.update_acell("E{}".format(next_row), cell5)
    sheet1.update_acell("F{}".format(next_row), cell6)

    
def df_rename_column(df, col_index, col_name ):
    df         = df.rename(columns={df.columns[col_index] : col_name})      
    return df

def df_cutheader_secondrow(df, col_index, col_name, cut_name ):
    #     df         = df.rename(columns={df.columns[col_index] : col_name})      
    cut_name_list = [cut_name]
    df         = df[  (~df[col_name].isin(cut_name_list))   ]
    return df


def convert_field_type_int(df, col_name):
    df[col_name] = df[col_name].astype(int)
    return df
