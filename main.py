from bottle import Bottle
from bottle import route, request, response, template
import os
import MySQLdb
import time
import cloudstorage as gcs
from test.assignment2 import *
from google.appengine.api import app_identity
import re
import datetime
from collections import defaultdict
# Note: We don't need to call run() since our application is embedded within
# the App Engine WSGI application server.

bottle = Bottle()

@bottle.route('/')
def hello():
    """Return a friendly HTTP greeting."""
    env = os.getenv('SERVER_SOFTWARE')
    main_string = "Time to Insert data ::"
    try:
        bucket_name = os.environ.get('BUCKET_NAME',app_identity.get_default_gcs_bucket_name())
        #file_name = "/"+bucket_name+"/all_month.csv"
        file_name = "/"+bucket_name+"/demo-testfile.csv"
        #content = read_file(file_name)
        db = get_connection(env)
        cursor = db.cursor()
        cursor = initialise_db(cursor)
        s_time = time.time()
        insert_into_table(cursor,file_name)
        db.commit()
        e_time = time.time()
        t_time = str(e_time-s_time)
        mag_5 = len(get_eq_gr_mag(cursor ,5))
        mag_4 = len(get_eq_gr_mag(cursor ,4))
        mag_3 = len(get_eq_gr_mag(cursor, 3))
        mag_2 = len(get_eq_gr_mag(cursor, 2))
        m_5 = len(get_eq_equal_mag(cursor,5))
        m_4 = len(get_eq_equal_mag(cursor,4))
        m_3 = len(get_eq_equal_mag(cursor,3))
        m_2 = len(get_eq_equal_mag(cursor,2))
        rows = get_eq_gr_mag(cursor ,5)
        result_string= filter_result(rows)
        output = ""
        output += "<h3>"+main_string+str(t_time)+"</h3>"
        output += "<h3>Number of earthquakes greater than maginitude 5: "+str(mag_5)+"</h3>"
        output += "<h3>Number of earthquakes greater than maginitude 4: "+str(mag_4)+"</h3>"
        output += "<h3>Number of earthquakes greater than maginitude 3: "+str(mag_3)+"</h3>"
        output += "<h3>Number of earthquakes greater than maginitude 2: "+str(mag_2)+"</h3>"
        output += "<h3>Number of earthquakes equal to maginitude 5: "+str(m_5)+"</h3>"
        output += "<h3>Number of earthquakes equal to maginitude 4: "+str(m_4)+"</h3>"
        output += "<h3>Number of earthquakes equal to maginitude 3: "+str(m_3)+"</h3>"
        output += "<h3>Number of earthquakes equal to maginitude 2: "+str(m_2)+"</h3>"
        return output+"<h3>Number of Earthquakes per each week of maginitude greater than 2<br>"+result_string+"</h3>"
    except Exception as e:
        return str(e)

def get_connection(type_of_con):
    try:
        if (type_of_con and type_of_con.startswith('Google App Engine/')):
            db = MySQLdb.connect(unix_socket='/cloudsql/cloudcomputing1-970:mysql-server-1',user='root')
            return db
        else:
            con = mdb.connect('localhost', 'root', 'root', 'test');
            return con
    except Exception as e:
        print "Connection to the database failed : "+str(e)

def initialise_db(cursor):
    try:
        result2 = cursor.execute('CREATE DATABASE IF NOT EXISTS test')
        result2 = cursor.execute('use test')
        result2 = cursor.execute('CREATE TABLE IF NOT EXISTS EARTHQUAKES (time TIMESTAMP,latitude DOUBLE PRECISION(15,7),longitude DOUBLE PRECISION(15,7),depth DOUBLE ,mag DOUBLE,magType varchar(255),nst VARCHAR(255),gap VARCHAR(255),dmin VARCHAR(255),rms VARCHAR(255),net varchar(255), id varchar(255),updated TIMESTAMP,place varchar(255),type varchar(255))')
        result2 = cursor.execute("truncate EARTHQUAKES");
        result2 = cursor.execute("ALTER TABLE EARTHQUAKES ADD PRIMARY KEY (id)");
    except Exception as e:
        print "Reinitialisation of the tables :: "+str(e)
    return cursor

def read_file(filename):
    gcs_file = gcs.open(filename)
    content = ""
    for line in gcs_file:
        content += line
    return content    

def filter_result(result):
    weeks_dic = defaultdict(int)
    result_string = ""
    for row in result:
        date = row[0]
        week = get_week(date.year,date.month,date.day)
        month_week = "Month:"+str(date.month)+":week:"+str(week)
        weeks_dic[month_week]+=1
        print date,week
    for key in sorted(weeks_dic):
        result_string += key+": "+str(weeks_dic[key])+"<br>"
    return result_string


def get_week(year, month, day):
    first_week_month = datetime.datetime(year, month, 1).isocalendar()[1]
    if month == 1 and first_week_month > 10:
        first_week_month = 0
    user_date = datetime.datetime(year, month, day).isocalendar()[1]
    if month == 1 and user_date > 10:
        user_date = 0
    return user_date - first_week_month+1

def insert_into_table(cursor,filename):
    to_insert={}
    gcs_file = gcs.open(filename)
    try:
        for line in gcs_file:
            fields = line.split(",")
            if(fields[0]=="time"):
                continue
            else:
                to_insert['time'] = fields[0]
                to_insert['latitude'] = float(fields[1])
                to_insert['longitude'] = float(fields[2])
                to_insert['depth'] = float(fields[3])
                to_insert['mag'] = float(fields[4])
                to_insert['magtype'] = fields[5]
                to_insert['nst'] = fields[6]
                to_insert['gap'] = fields[7]
                to_insert['dmin'] = fields[8]
                to_insert['rms'] = fields[9]
                to_insert['net'] = fields[10]
                to_insert['idn'] = fields[11]
                to_insert['updated'] = fields[12]
                to_insert['place'] = fields[13].replace("'","").replace("\"","")
                to_insert['type_of'] = fields[14].strip("\n")
                query = create_insert(to_insert)
                #print query
                cur_r = cursor.execute(query)
    except Exception as e:
        print "Duplicate entries "+str(e)

def create_insert(to_insert):
    formated_time = format_time(to_insert['time'])
    formated_updated = format_time(to_insert['updated'])
    query = "INSERT INTO EARTHQUAKES(time,latitude,longitude,depth,mag,magtype,nst,gap,dmin,rms,net,id,updated,place,type) VALUES("
    query += "'"+formated_time+"'"+","
    query += str(to_insert['latitude'])+","
    query += str(to_insert['longitude'])+","
    query += str(to_insert['depth'])+","
    query += str(to_insert['mag'])+","
    query += "'"+to_insert['magtype']+"'"+","
    query += "'"+to_insert['nst']+"'"+","
    query += "'"+to_insert['gap']+"'"+","
    query += "'"+to_insert['dmin']+"'"+","
    query += "'"+to_insert['rms']+"'"+","
    query += "'"+to_insert['net']+"'"+","
    query += "'"+to_insert['idn']+"'"+","
    query += "'"+formated_updated+"'"+","
    query += "'"+to_insert['place'].rstrip("\"")+"'"+","
    query += "'"+to_insert['type_of']+"'"+")"
    return query
    
def format_time(time_string):
    date = time_string.split("T")[0]
    time = time_string.split("T")[1]
    #2008-01-01 00:00:01
    return date+" "+time.split(".")[0]

def get_eq_gr_mag(cursor ,magnitude):
    cursor.execute("SELECT * FROM EARTHQUAKES WHERE mag >"+str(magnitude))
    rows = cursor.fetchall()
    return rows

def get_eq_equal_mag(cursor,magnitude):
    cursor.execute("SELECT * FROM EARTHQUAKES WHERE mag ="+str(magnitude))
    rows = cursor.fetchall()
    return rows

@bottle.route('/upload')
def upload(name="User"):
    return template('upload_template', name=name)

@bottle.route('/doupload',  method='POST')
def doupload(time_taken=0):
    start_time = time.time()
    bucket_name = os.environ.get('BUCKET_NAME',app_identity.get_default_gcs_bucket_name())
    filename = '/'+bucket_name+ '/demo-testfile.csv'
    #upload = request.files.get('upload')
    data = request.files.get('data')
    raw = data.file.read()
    write_retry_params = gcs.RetryParams(backoff_factor=1.1)
    gcs_file = gcs.open(filename,'w',content_type='text/plain',options={'x-goog-meta-foo': 'foo','x-goog-meta-bar': 'bar'},retry_params=write_retry_params)
    gcs_file.write(raw)
    gcs_file.close()
    end_time = time.time()
    time_taken = end_time-start_time
    #print len(data)
    return template('doupload_template',time_taken=time_taken)

# Define an handler for 404 errors.
@bottle.error(404)
def error_404(error):
    """Return a custom 404 error."""
    return 'Sorry, nothing at this URL.'
