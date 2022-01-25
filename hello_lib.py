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
    df_new = df_new.fillna("")
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
    df_new = df_new.fillna("")
    return df_new

def import_gg_csv_comma(file_id, file_name):
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
    df_new = pd.read_csv(file_name ,  sep=','       )
    df_new = df_new.fillna("")
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


# def convert_field_type_int(df, col_name):
#     df[col_name] = df[col_name].astype(int)
#     return df


# def convert_field_type_int(col_value):
#     return str(col_value)

def mapping_base_info_from_main(df_m, df, col_name):
    df['empl_status'] = df[col_name].map(df_m.set_index(col_name)['empl_status'])
    df['empl_class'] = df[col_name].map(df_m.set_index(col_name)['empl_class'])
    df['descr_c'] = df[col_name].map(df_m.set_index(col_name)['descr_c'])
    df['descr_m'] = df[col_name].map(df_m.set_index(col_name)['descr_m'])
    df['effdt_en'] = df[col_name].map(df_m.set_index(col_name)['effdt_en'])
    df['effdt_en_yyyymm'] = df[col_name].map(df_m.set_index(col_name)['effdt_en_yyyymm'])
    df['merge_hiredate_en'] = df[col_name].map(df_m.set_index(col_name)['merge_hiredate_en'])
    df['merge_hiredate_en_yyyymm'] = df[col_name].map(df_m.set_index(col_name)['merge_hiredate_en_yyyymm'])
    return df



def mapping_ey_from_main(df_m, df, col_name, today_date):
    
    df_m['today_date'] = today_date

    df_m['พนักงานเข้าใหม่ยังไม่ถึงปี'] = ''
    df_m.loc[(df_m['empl_status'].isin(['A'])  ) &
                 (pd.to_datetime(df_m['today_date'], format='%d/%m/%Y') - pd.to_datetime(df_m['hire_date_en'], format='%d/%m/%Y')  < '365 days' ), 'พนักงานเข้าใหม่ยังไม่ถึงปี']  = 'พนักงานเข้าใหม่ยังไม่ถึงปี'

    df_m.loc[(df_m['action'] == 'REH') , 'พนักงานเข้าใหม่ยังไม่ถึงปี']  = ''


#     df_m['วันสิ้นสภาพ'] = ''
    df_m.loc[(df_m['empl_status'].isin(['T','D'])  ), 'วันสิ้นสภาพ']  = df_m['effdt_en']
    
    df['rc_code']                         = df[col_name].map(df_m.set_index(col_name)['rc_code'] )
    df['descr_rc_code']                   = df[col_name].map(df_m.set_index(col_name)['descr_rc_code'] )
    df['descr_c']                         = df[col_name].map(df_m.set_index(col_name)['descr_c'] )
    df['1. กำกับสายงาน']                   = df[col_name].map(df_m.set_index(col_name)['1. กำกับสายงาน'] )
    df['2. สาย']                          = df[col_name].map(df_m.set_index(col_name)['2. สาย'] )
    df['3. กลุ่ม']                          = df[col_name].map(df_m.set_index(col_name)['3. กลุ่ม'] )
    df['merge_hiredate_en']               = df[col_name].map(df_m.set_index(col_name)['merge_hiredate_en'] )
    df['age']                             = df[col_name].map(df_m.set_index(col_name)['age'] )
    df['sex']                             = df[col_name].map(df_m.set_index(col_name)['sex'] )
    df['sys_retireyear']                  = df[col_name].map(df_m.set_index(col_name)['sys_retireyear'] )
    df['empl_class']                      = df[col_name].map(df_m.set_index(col_name)['empl_class'] )
    df['effdt']                           = df[col_name].map(df_m.set_index(col_name)['effdt'] )
    df['action']                          = df[col_name].map(df_m.set_index(col_name)['action'] )
    df['action_reason']                   = df[col_name].map(df_m.set_index(col_name)['action_reason'] )
    df['action_reason_descr']             = df[col_name].map(df_m.set_index(col_name)['action_reason_descr'] )
    df['พนักงานเข้าใหม่ยังไม่ถึงปี']             = df[col_name].map(df_m.set_index(col_name)['พนักงานเข้าใหม่ยังไม่ถึงปี'] )
    df['วันสิ้นสภาพ']                         = df[col_name].map(df_m.set_index(col_name)['วันสิ้นสภาพ'] )
    


    return df










def fnc_senddata_to_googlesheet(df , googlesheet_url, sheet_name, column_list, sheet_range):

#     !ls drive
#     !pip install pydrive
#     !pip install xlsxwriter
    import pandas as pd

    #add column
    if (len(column_list)>0):
        df = df[column_list]
        

    # these classes allow you to request the Google drive API
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

    #wb2 = gc.open_by_url('https://docs.google.com/spreadsheets/d/{}/edit#gid=0'.format(val_4_2))
    wb2 = gc.open_by_url(googlesheet_url)

    sheet_destination_sheet = wb2.worksheet(sheet_name)
    wb2.values_clear("'{}'!{}".format(sheet_name,sheet_range))
    set_with_dataframe(sheet_destination_sheet, df, row=10, include_column_header=True) 
