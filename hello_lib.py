


import datetime, pytz
tz = pytz.timezone('Asia/Bangkok')

import pandas as pd
import numpy as np
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
    df['วันสิ้นสภาพ']                        = df[col_name].map(df_m.set_index(col_name)['วันสิ้นสภาพ'] )
    df['เรียงลำดับสายงาน']                   = df[col_name].map(df_m.set_index(col_name)['เรียงลำดับสายงาน'] )
    df['สายงานภายใต้การรับผิดชอบของ BP ณ ปัจจุบัน (Fix Code กรณีมีการแก้ไขต้องปรับเปลี่ยนช่องด้วย)'] = df[col_name].map(df_m.set_index(col_name)['สายงานภายใต้การรับผิดชอบของ BP ณ ปัจจุบัน (Fix Code กรณีมีการแก้ไขต้องปรับเปลี่ยนช่องด้วย)'] )
    
    
    a = ['CON',  'RLS',  'WHI']
    df = df[~df.action_reason.isin(a)]


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

    
    
    try:
            sheet1 = wb2.worksheet(sheet_name)
#             wb2.del_worksheet(sheet1)

#             worksheet1 = wb2.add_worksheet(title=sheet_name, rows="100", cols="20")
    except:
            worksheet1 = wb2.add_worksheet(title=sheet_name, rows="100", cols="20")
    
    
    #worksheet1 เสมือน sheet_destination_sheet
    sheet_destination_sheet = wb2.worksheet(sheet_name)
    wb2.values_clear("'{}'!{}".format(sheet_name,sheet_range))
    set_with_dataframe(sheet_destination_sheet, df, row=10, include_column_header=True) 
    
    
    
    
    
    
    
    
def grouptake1_12month(data_ey, key_mapping, key_type, key_type_value,key_sum, key_m):
    listofmonth_int = ['01','02','03','04','05','06','07','08','09','10','11','12']
    newlist = []
    count = 1
    # for x in range(1, 10):
    #     field_rename = 'สิทธิลาพักผ่อนประจำปี : รวมวันลาใช้ไป ณ เดือน ' + str(x)
    for x1 in listofmonth_int:
        newlist.append(x1)
        field_rename = key_type_value + ": รวมวันลาใช้ไป ณ เดือน " + str(count)
#         print(newlist)
#         print(field_rename)


        data_ey2SUM_group           = data_ey[  (data_ey[key_type]).isin([key_type_value]) & (data_ey[key_m]).isin(newlist)  ].groupby(key_mapping).agg({key_sum: 'sum'}).reset_index().rename(columns={key_sum:field_rename})
        data_ey[field_rename]       = data_ey[key_mapping].map(data_ey2SUM_group.set_index(key_mapping)[field_rename])
        data_ey.loc[(      data_ey[field_rename].isnull() ), field_rename]  = 0.0
        count                       = count +1

def assign_field_to_float(df, f):
    df[f] = df[f].fillna('0.0')
    df.loc[(df[f] == '' ),f]='0.0'
    df[f] = df[f].astype(float)
    return df



def parameter_this_year(this_year):

    return this_year


def upload_file_split_sheet(df, id_googlesheet, f1_value, f2, f3, f3_value, my_sheet, my_clean_range, this_year ):
    
    this_year_str = str(this_year)
    this_year_before = str(this_year-1)
    
    df_new = df[   df['หน่วยงานที่ดูแล'].isin([f1_value])  & df[f2].isin(['A'])  &  df[f3].isin([f3_value])          ]

    
    
    field11 = '1.1 สิทธิลาพักผ่อนยกมาปี ' + this_year_before + ' ใช้ภายใน 31/03/'+ this_year_str
    field12 = '1.2 ยอดใช้ไปลาพักผ่อนยกจากปี ' + this_year_before + ' ใช้ภายใน 31/03/'+ this_year_str
    field13 = '1.3 คงเหลือลาพักผ่อนยกจากปี ' + this_year_before + ' ใช้ภายใน 31/03/'+ this_year_str
    
    field21 = '2.1 สิทธิลาพักผ่อนประจำปี ' + this_year_str
    field22 = '2.2 ยอดใช้ไปลาพักผ่อนประจำปี ' + this_year_str
    field23 = '2.3 คงเหลือลาพักผ่อนปี ' + this_year_str
    
    field31 = '3.1 สิทธิลาพักผ่อนสะสม'
    field32 = '3.2 ยอดใช้ไปลาพักผ่อนสะสม'
    field33 = '3.3 คงเหลือลาพักผ่อนสะสม'
    
        
    df_new = df_new.rename(columns={    '3. กลุ่ม' : 'กลุ่ม',
                                        'rc_code' : 'รหัสสังกัด',
                                        'descr_rc_code' : 'สังกัด',
                                        '1.1 สิทธิการลาพักผ่อนประจำปียกมาจากปีที่แล้ว' : field11,
                                        '1.2 ยอดใช้ไป - สิทธิการลาพักผ่อนประจำปียกมาจากปีที่แล้ว' : field12,                          
                                        '1.3 คงเหลือ - สิทธิการลาพักผ่อนประจำปียกมาจากปีที่แล้ว' : field13, 
                                    
                                    
                                        '2.1 สิทธิการลาพักผ่อนประจำปีปัจจุบัน' : field21,
                                        '2.2 ยอดใช้ไป - สิทธิการลาพักผ่อนประจำปีปัจจุบัน' : field22,                          
                                        '2.3 คงเหลือ - สิทธิการลาพักผ่อนประจำปีปัจจุบัน' : field23,     
                                    
                                        '3.1 สิทธิการลาพักผ่อนสะสมใช้ได้ 3 ปี' : field31,
                                        '3.2 ยอดใช้ไป - สิทธิการลาพักผ่อนสะสมใช้ได้ 3 ปี' : field32,                          
                                        '3.3 คงเหลือ - สิทธิการลาพักผ่อนสะสมใช้ได้ 3 ปี' : field33,     
                                    
    })


    df_new.sort_values([field33], ascending=[False],  na_position ='first', inplace=True)
    column_BP = [                
                                'emplid',
                                'name',
                                'ตำแหน่ง',
                                'กลุ่ม',
                                'รหัสสังกัด',
                                'สังกัด',
                                'กลุ่มที่ต้อง adjust วันลา',
                                field11,
                                field12,
                                field13,
             
                                field21,
                                field22,
                                field23,

                                field31,
                                field32,
                                field33,

#                                 'หน่วยงานที่ดูแล'
                              ]
    df_new = df_new.drop(['หน่วยงานที่ดูแล'],axis =1)
    fnc_senddata_to_googlesheet(df_new, id_googlesheet, my_sheet  , column_BP, my_clean_range) 
    
    
    
    
def run_e_status(df,e):
    df[e] = 0
    df.loc[(df[e] == 'A' ),e]=1
    df.loc[(df[e] == 'L' ),e]=2
    df.loc[(df[e] == 'S' ),e]=3
    df.loc[(df[e] == 'T' ),e]=4
    df.loc[(df[e] == 'D' ),e]=5
    return df
    
    
def fnc_find_this_year_match_function(today_date_month, my_text):
    my_column_name = ''
    if(today_date_month =='01'):
        my_column_name = my_text + '1'
    elif(today_date_month =='02'):
        my_column_name = my_text + '2'
    elif(today_date_month =='03'):
        my_column_name = my_text + '3'
    elif(today_date_month =='04'):
        my_column_name = my_text + '4'
    elif(today_date_month =='05'):
        my_column_name = my_text + '5'
    elif(today_date_month =='06'):
        my_column_name = my_text + '6'
    elif(today_date_month =='07'):
        my_column_name = my_text + '7'
    elif(today_date_month =='08'):
        my_column_name = my_text + '8'
    elif(today_date_month =='09'):
        my_column_name = my_text + '9'
    elif(today_date_month =='10'):
        my_column_name = my_text + '10'
    elif(today_date_month =='11'):
        my_column_name = my_text + '11'
    elif(today_date_month =='12'):
        my_column_name = my_text + '12'

     
    return my_column_name
    
    
    
def fnc_find_this_year_match_function_before_month(today_date_month, my_text):

    my_column_name = ''
    if(today_date_month =='01'):
        my_column_name = my_text + '1'
    elif(today_date_month =='02'):
        my_column_name = my_text + '1'
    elif(today_date_month =='03'):
        my_column_name = my_text + '2'
    elif(today_date_month =='04'):
        my_column_name = my_text + '3'
    elif(today_date_month =='05'):
        my_column_name = my_text + '4'
    elif(today_date_month =='06'):
        my_column_name = my_text + '5'
    elif(today_date_month =='07'):
        my_column_name = my_text + '6'
    elif(today_date_month =='08'):
        my_column_name = my_text + '7'
    elif(today_date_month =='09'):
        my_column_name = my_text + '8'
    elif(today_date_month =='10'):
        my_column_name = my_text + '9'
    elif(today_date_month =='11'):
        my_column_name = my_text + '10'
    elif(today_date_month =='12'):
        my_column_name = my_text + '11'

     
    return my_column_name
    
    
    

def leave_convert_ceiling05(df, field1, field2, field3 ):
    listofmonth_int = ['01','02','03','04','05','06','07','08','09','10','11','12']
    for x in listofmonth_int:
        field_name_avg = field1 + str(x) + ')'
        field_name = field2 + str(int(x))
        df[field_name_avg] = np.ceil((df[field_name] / df[field3])  *2)/2

    return df
    
