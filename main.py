import pymongo 
import pandas as pd
from summary import is_present, create_present_indicator, prepare_teacher_summary,get_summary_df
import json
from tqdm import tqdm
from datetime import datetime
from models import create_uri,create_engine, create_db, create_tables,insert_table_student,insert_table_teacher

def get_config():
    with open('config.json','r') as reader:
        config = json.loads(reader.read())
    return config

def get_time_stamp():
    now = datetime.now()
    year,month,day,hour,minute,second = now.year,now.month,now.day,now.hour,now.minute,now.second
    return year,month,day,hour,minute,second


if __name__=="__main__":
    config = get_config()
    collection = config['collection']
    table = get_summary_df(collection=collection)
    year,month,day,hour,minute,second = get_time_stamp()
    table.to_csv(f"./report_dumps/sample_student_{collection}_{year}:{month}:{day}:{hour}:{minute}:{second}.csv",index=False)
    table = create_present_indicator(table,is_present)
    report = prepare_teacher_summary(table)
    report.to_csv(f"./report_dumps/sample_teacher_{collection}_{year}:{month}:{day}:{hour}:{minute}:{second}.csv",index=False)
    engine = create_engine(create_uri(config,base=True),echo=True)
    if  (config['db_name'],) not in engine.execute('SELECT datname FROM pg_database;').fetchall():
        create_db(engine,config)
    engine = create_engine(create_uri(config,base=False),echo=True)
    create_tables(engine)
    insert_table_student(table,engine)
    insert_table_teacher(report,engine)







