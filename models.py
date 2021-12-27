from sqlalchemy import Column, Integer, Numeric, String, DateTime ## for defining schema
from sqlalchemy.ext.declarative import declarative_base ## defining tables
from sqlalchemy import create_engine ## db path etc
from sqlalchemy.orm import Session
from sqlalchemy.sql.elements import Null
from sqlalchemy.sql.expression import null ## for querying/inserting in db
from tqdm import tqdm
from dateutil import parser
from zoneinfo import ZoneInfo

Base = declarative_base()
utc = ZoneInfo('UTC')

class Student(Base):
    __tablename__='student_report'
    id = Column(Integer(),autoincrement=True,primary_key=True)
    teacher = Column(String())
    class_name = Column(String())
    num_students = Column(Numeric())
    user = Column(String())
    total = Column(Numeric())
    correct = Column(Numeric())
    wrong = Column(Numeric())
    start_time = Column(DateTime())
    session_name = Column(String())
    dontknow = Column(Numeric())
    session_id = Column(String())
    num_questions = Column(Numeric())

class Teacher(Base):
    __tablename__="teacher_report"
    id = Column(Integer(),autoincrement=True,primary_key=True)
    teacher = Column(String())
    class_name = Column(String())
    num_students = Column(Numeric())
    start_time = Column(DateTime())
    session_name = Column(String())
    session_id = Column(String())
    num_question = Column(Numeric())
    present_students = Column(Numeric())
    total = Column(Numeric())
    correct = Column(Numeric())
    wrong = Column(Numeric())
    dontknow = Column(Numeric())

def create_uri(config,base=False):
    if base==False:
        uri = f"postgresql://{config['db_user']}:{config['db_pw']}@{config['db_host']}:{config['db_port']}/{config['db_name']}"
    else:
        uri = f"postgresql://{config['db_user']}:{config['db_pw']}@{config['db_host']}:{config['db_port']}"
    return uri

def create_db(engine,config):
    con = engine.connect()
    con.execute('commit')
    con.execute(f"create database {config['db_name']}")
    print(f"Data base {config['db_name']} successfully created")
    con.close()

def create_tables(engine):
    Base.metadata.create_all(engine)

def check_if_row_exists(session,item,**kwargs):
    if session.query(item).filter_by(**kwargs).count()==0:
        return False
    else:
        return True

def get_or_create(session, model, **kwargs):
    instance = session.query(model).filter_by(**kwargs).first()
    if instance:
        return instance
    else:
        instance = model(**kwargs)
        session.add(instance)
        session.commit()
        return instance

def check_zero(val):
     if val==0 or val=='0':
        return None
     else:
        return val

def check_date(val):
    if val=='0':
        return None
    else:
        date = parser.parse(val,ignoretz=True)
        utc_date = date.replace(tzinfo=utc)
        return utc_date

def insert_table_student(df,engine):
    session = Session(engine)
    for idx,row in tqdm(df.iterrows()):
        keywords = {'teacher':check_zero(row['teachers']),
                    'class_name':check_zero(row['class_names']),
                    'num_students':check_zero(row['num_students']),
                    'user':check_zero(row['users']),
                    'total':check_zero(row['totals']),
                    'correct':check_zero(row['corrects']),
                    'wrong':check_zero(row['wrongs']),
                    'start_time':check_date(row['start_times']),
                    'session_name':check_zero(row['session_names']),
                    'dontknow':check_zero(row['dontknow']),
                    'session_id':check_zero(row['session_ids']),
                    'num_questions':check_zero(row['num_questions'])}
        if not check_if_row_exists(session=session,item=Student,**keywords):
            db_row = Student(**keywords)
            session.add(db_row)
            try:
                session.commit()
                print(f'Row number {idx} of table added to db')
            except:
                session.rollback()
    session.close()

def insert_table_teacher(df,engine):
    session = Session(engine)
    for idx,row in tqdm(df.iterrows()):
        keywords = {'teacher':check_zero(row['teachers']),
                    'class_name':check_zero(row['class_names']),
                    'num_students':check_zero(row['num_students']),
                    'total':check_zero(row['totals']),
                    'correct':check_zero(row['corrects']),
                    'wrong':check_zero(row['wrongs']),
                    'start_time':check_date(row['start_times']),
                    'session_name':check_zero(row['session_names']),
                    'session_id':check_zero(row['session_ids']),
                    'dontknow':check_zero(row['dontknow']),
                    'num_question':check_zero(row['num_questions']),
                    'present_students':check_zero(row['present'])}
        if not check_if_row_exists(session=session,item=Teacher,**keywords):
            db_row = Teacher(**keywords)
            session.add(db_row)
            try:
                session.commit()
                print(f'Row number {idx} of table added to db')
            except:
                session.rollback()
    session.close()


