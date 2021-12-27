import pandas as pd
from sqlalchemy.sql.elements import Null
from tqdm import tqdm
from dump_data import get_client
import re

date_pattern = re.compile(r'\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}')

def clean_dates(val):
    print(val)
    if val == '0' or len(val)<=20:
        return val
    else:
        return re.findall(date_pattern,val)[0]

grouper = ['teachers','class_names','session_ids','session_names','start_times','num_students']
def is_present(row):
    if row['wrongs']==0 and row['corrects']==0 and row['totals']==0 and row['dontknow']==0:
        return 0
    else:
        return 1
def create_present_indicator(table,is_present):
    table['present'] = table.apply(is_present,axis=1)
    return table
def prepare_teacher_summary(table):
    table1=table.groupby(grouper)[['present']].sum()
    table1=table1.reset_index()
    table2 = table[table['present']==1]
    table2 = table2.drop(['present','num_questions'],axis=1).groupby(grouper).sum()
    table2 = table2.reset_index()
    table3 = table[table['present']==1]
    table3 = table3.drop(['present','users','corrects','wrongs','dontknow','totals'],axis=1)
    table3 = table3.drop_duplicates()
    table4 = pd.merge(table1,table2,how='inner',on=['teachers','class_names','session_ids',
                                       'session_names','start_times','num_students'])
    report = pd.merge(table3,table4,how='inner',on=['teachers','class_names','session_ids',
                                       'session_names','start_times','num_students'])
    for col in report.columns:
        if report[col].dtype!='object':
            report[col].fillna(0,inplace=True)
        else:
            report[col].fillna('0',inplace=True)
    return report

def get_summary_df(collection):
    ## fetch class_ids from class collection
    client = get_client()
    teachers = []
    class_names = [] 
    num_students = []
    users = []
    totals = []
    corrects = []
    wrongs = []
    start_times = []
    session_names = []
    dontknows = []
    num_questions = []
    session_ids = []
    for doc in tqdm(client[collection]['classes'].find()):
        teacher = doc['primaryteacher']
        class_id = doc['_id']
        name = doc['name']
        num_student = len(doc['students'])
        students = doc['students']
        q1 = {'classid':class_id}
        present_students = []
        for stats in client[collection]['class_stats'].find(q1):
            user_id = stats['userid']
            stat = [v for v in stats['sessions'].values()][0]
            totalanswered = stat['stats'].get('totalanswered')
            wrong = stat['stats'].get('wrong')
            correct = stat['stats'].get('correct')
            dontknow = stat['stats'].get('dontknow')
            session_id = stat['id']
            q2 = {'class_id':class_id}
            for sessions in client[collection]['sessions'].find(q2):
                for session in sessions['sessions']:
                    if session['id'] == session_id:
                        start_time = session['starttime']
                        session_name = session['sessionname']
                        present_students.append(user_id)
                        num_questions.append(len(session['questions']))
                        teachers.append(teacher)
                        class_names.append(name)
                        num_students.append(num_student)
                        users.append(user_id)
                        totals.append(totalanswered)
                        corrects.append(correct)
                        wrongs.append(wrong)
                        start_times.append(start_time)
                        session_names.append(session_name)
                        dontknows.append(dontknow)
                        session_ids.append(session_id)
        absent_students=list(set(students)-set(present_students))
        for absent in absent_students:
            users.append(absent)
            num_questions.append(0)
            teachers.append(teacher)
            class_names.append(name)
            num_students.append(num_student)
            totals.append(0)
            corrects.append(0)
            wrongs.append(0)
            start_times.append('0')
            session_names.append(0)
            dontknows.append(0)
            session_ids.append(session_id)   
    table = pd.DataFrame({'teachers':teachers,
                            'class_names':class_names,
                            'num_students':num_students,
                            'users':users,
                            'totals':totals,
                            'corrects':corrects,
                            'wrongs':wrongs,
                            'start_times':start_times,
                            'session_names':session_names,
                            'dontknow':dontknows,
                            'session_ids':session_ids,
                            'num_questions':num_questions})
    table['start_times'] = table['start_times'].map(clean_dates)
    for col in table.columns:
        if table[col].dtype!='object':
            table[col].fillna(0,inplace=True)
        else:
            table[col].fillna('0',inplace=True)
    return table