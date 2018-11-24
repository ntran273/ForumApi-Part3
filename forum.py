import flask
import uuid
from flask import Flask,request, _app_ctx_stack, session, jsonify, render_template, json, abort, Response, flash, g, current_app, make_response
from flask_basicauth import BasicAuth
from flask.cli import AppGroup
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from flask_cassandra import CassandraCluster
import click
import sqlite3
import os

app = flask.Flask(__name__)
cassandra = CassandraCluster()
app.config['CASSANDRA_NODES'] = ['172.17.0.2']
# app.config['DEBUG'] = True
CASSANDRA_NODES = '127.17.0.2'
DATABASE = 'forum_api'

class Authentication(BasicAuth):
    def check_credentials(self, username, password):
        print('check_credentials')
        # query from database
        query = "SELECT * from users where username ='{}'".format(username)
        user = query_db(query)
        if user == []:
            return False
        if user[0]['password'] == password:
            current_app.config['BASIC_AUTH_USERNAME'] = username
            current_app.config['BASIC_AUTH_PASSWORD'] = password
            return True
        else:
            return False


basic_auth = Authentication(app)

#Function Connect Database
#https://github.com/TerbiumLabs/flask-cassandra/blob/master/flask_cassandra.py
def get_db():
    top = _app_ctx_stack.top
    if not hasattr(top, 'cassandra_cluster'):
        ctx = stack.top
        ctx.cassandra_cluster = cassandra.connect()
        ctx.cassandra_cluster.set_keyspace(DATABASE)
        ctx.cassandra_cluster.row_factory = dict_factory
    return ctx.cassandra_cluster

@app.teardown_appcontext
def close_connection(exception):
     ctx = _app_ctx_stack.top
     if hasattr(ctx, 'cassandra_cluster'):
        ctx.cassandra_cluster.shutdown()

#Function  execute script
def init_db():
    os.system("docker exec -it scylla cqlsh -k forum_api -f schema.cql")
    
#Create Command initdb
#To use command, run in terminal export FLASK_APP = appname, flask initdb
@app.cli.command('init_db')
def initdb_command():
    init_db()
    print('Initialize the database.')

# Function that formats the date
def getTimeStamp(threadOrPost):
    timeType = ''
    if threadOrPost == 'thread':
        timeType = 'thread_time'
    else:
        timeType = 'post_time'
    # timestamp = "strftime('%m', datetime(thread_time, 'unixepoch')) as month"
    timestampDay = '''case cast (strftime('%w', {}) as integer)
        when 0 then 'Sun, '
        when 1 then 'Mon, '
        when 2 then 'Tues, '
        when 3 then 'Wed, '
        when 4 then 'Thurs, '
        when 5 then 'Fri, '
        else 'Sat, ' end'''.format(timeType)
    timestampDate = "strftime('%d', {})".format(timeType)
    timestampMonth = '''case cast(strftime('%m', {}) as integer)
        when 1 then ' Jan '
        when 2 then ' Feb '
        when 3 then ' Mar '
        when 4 then ' Apr '
        when 5 then ' May '
        when 6 then ' Jun '
        when 7 then ' July '
        when 8 then ' Aug '
        when 9 then ' Sept '
        when 10 then ' Oct '
        when 11 then ' Nov '
        when 12 then ' Dec '
        else '' end'''.format(timeType)
    timestampYear = "strftime('%Y', {})".format(timeType)
    timestampTime = "strftime('%H:%M:%S', {})".format(timeType)
    timestamp = '''{} || {} || {} || {} || ' ' ||  {} || ' ' ||'GMT' '''.format(timestampDay, timestampDate, timestampMonth, timestampYear, timestampTime)
    return timestamp

#Function using for query database
#Fetch each data one by one based on the query provided
def query_db(query, args=(), one=False):
    conn = get_db()
    # for row in conn.execute(query):
    #     return row[0] #CHECK THIS ONE NOT SURE
    query = conn.prepare(query)
    cur = conn.execute(query, args)
    rv = cur[:]
    return (rv[0] if rv else None) if one else rv


#Create User
@app.route('/users', methods=['POST'])
def create_user():
    data = request.get_json(force=True)
    username = data['username']
    password = data['password']

    query = 'SELECT username FROM users'
    listusername = query_db(query)
    for user_name in listusername:
        if user_name['username'] == username:
            error = '409 A username already exists'
            return make_response(jsonify({'error': error}), 409)
    db = get_db()
    db.execute('insert into users(username, password) values (?,?)', (username,password))
    db.commit()

    response = make_response('Success: account created')
    response.status_code = 201
    return response

#Still working on it.
#Change User Password
@app.route('/users/<string:user>', methods=['PUT'])
@basic_auth.required
def change_password(user):
    data = request.get_json(force=True)
    newpassword = data['password']
    creator = current_app.config['BASIC_AUTH_USERNAME']

    #check if username is in database
    query = "SELECT Id FROM USERS WHERE username = '{}'".format(str(user))
    useracc = query_db(query)
    if not useracc:
       error = '404 No user exists with the user of ' + str(user)
       return make_response(jsonify({'error': error}), 404)

    #Check if the username is the same with account authenticated
    if(creator == str(user)):

        db = get_db()
        db.execute("UPDATE users SET password= ? WHERE username= ?",(newpassword, str(user)))
        db.commit()
        response = make_response("Success: User password Changed")
        response.status_code = 201
        return response
    error = '409 CONFLICT Authenticated Account does not match user ' + str(user)
    return make_response(jsonify({'error': error}), 409)

#List available discussion forums
@app.route('/forums', methods = ['GET'])
def api_forums():
    query = "SELECT * FROM forums;"
    forums = query_db(query)

    # all_forums = query_db('SELECT forums.Id, forums.forum_name, user.username FROM  forums INNER JOIN user ON forums.Id = user.Id ;')
    return jsonify(forums)

#List threads in the specified forum
@app.route('/forums/<int:forum_id>', methods = ['GET'])
def api_threads(forum_id):
    query = 'SELECT Id FROM forums WHERE Id = ' + str(forum_id) +';'
    forum = query_db(query)
    if not forum :
        error = '404 No forum exists with the forum id of ' + str(forum_id)
        return make_response(jsonify({'error': error}), 404)
    else:

        timestamp = getTimeStamp('thread')
        query = 'SELECT Id, thread_creator as creator, {} as timestamp, thread_title as title FROM threads WHERE forum_id = {} ORDER BY thread_time DESC'.format(timestamp,str(forum_id))
        threads = query_db(query)
        return jsonify(threads)


#List posts in the specified thread
@app.route('/forums/<int:forum_id>/<int:thread_id>', methods=['GET'])
def get_post(forum_id, thread_id):
    print(forum_id, thread_id)
     # Select from forums on forum id to make sure that the forum exists
    query = 'SELECT * FROM forums WHERE id = ' + str(forum_id)
    forum = query_db(query)
    if not forum:
        error = '404 No forum exists with the forum id of ' + str(forum_id)
        return make_response(jsonify({'error': error}), 404)
    # Select from threads on thread_id to make sure thread exists
    query = 'SELECT * FROM threads WHERE id = ' + str(thread_id)
    thread = query_db(query)
    if not thread:
        error = '404 No thread exists with the thread id of ' + str(thread_id)
        return make_response(jsonify({'error': error}), 404)
    timestamp = getTimeStamp('post')
    query = "SELECT post_authorId as author, post_text as text, {} as timestamp FROM posts WHERE post_threadId = {} AND post_forumid = {}".format(timestamp, str(thread_id), str(forum_id))
    post = query_db(query)
    return jsonify(post)

#POST FORUM
@app.route('/forums', methods=['POST'])
@basic_auth.required
def post_forums():

    data = request.get_json(force=True)
    name = data['forum_name']

    creator = current_app.config['BASIC_AUTH_USERNAME']
    query = 'SELECT forum_name FROM forums'
    forum_names = query_db(query)
    for forum_name in forum_names:
        if forum_name['forum_name'] == name:
            error = '409 A forum already exists with the name ' + name
            return make_response(jsonify({'error': error}), 409)

    db = get_db()
    db.execute('insert into forums (forum_name, forum_creator) values (?, ?)',(name, creator))
    db.commit()

    query = "select Id from forums where forum_name ='{}'".format(name)
    new_forum = query_db(query)
    response = make_response('Success: forum created')
    response.headers['location'] = '/forums/{}'.format(new_forum[0]['Id'])
    response.status_code = 201

    return response

#POST THREAD
@app.route('/forums/<int:forum_id>', methods=['POST'])
@basic_auth.required
def post_thread(forum_id):

    data = request.get_json(force=True)
    title = data['thread_title']
    text = data['text']
    creator = current_app.config['BASIC_AUTH_USERNAME']

     # Select from forums on forum id to make sure that the forum exists
    query = 'SELECT * FROM forums WHERE id = ' + str(forum_id)
    forum = query_db(query)
    print(forum)
    if len(forum) == 0:
        error = '404 No forum exists with the forum id of ' + str(forum_id)
        return make_response(jsonify({'error': error}), 404)
    # If forum exist, insert into threads table
    db = get_db()
    db.execute('insert into threads (thread_title, thread_creator, forum_Id) values (?, ?, ?)',(title, creator, str(forum_id)))
    db.commit()
    # Get the thread_id from the new thread to put into post's thread_id
    file_entry = query_db('SELECT last_insert_rowid()')
    thread_id = file_entry[0]['last_insert_rowid()']
    # Insert text as a new post
    db.execute('insert into posts (post_text, post_authorid , post_threadId, post_forumid) values (?, ?, ?, ?)',(text, creator, str(thread_id), str(forum_id)))
    db.commit()

    response = make_response("Success: Thread and Post created")
    response.headers['location'] = '/forums/{}/{}'.format(str(forum_id), thread_id)
    response.status_code = 201
    return response

#POST POST
@app.route('/forums/<int:forum_id>/<int:thread_id>', methods=['POST'])
@basic_auth.required
def post_post(forum_id, thread_id):
    # Select from forums on forum id to make sure that the forum exists
    query = 'SELECT * FROM forums WHERE id = ' + str(forum_id)
    forum = query_db(query)
    print(forum)
    if len(forum) == 0:
        error = '404 No forum exists with the forum id of ' + str(forum_id)
        return make_response(jsonify({'error': error}), 404)
    # Select from threads on thread_id to make sure thread exists
    query = 'SELECT * FROM threads WHERE id = ' + str(thread_id)
    thread = query_db(query)
    print(thread)
    if len(thread) == 0:
        error = '404 No thread exists with the thread id of ' + str(thread_id)
        return make_response(jsonify({'error': error}), 404)

    data = request.get_json(force=True)
    creator = current_app.config['BASIC_AUTH_USERNAME']
    text = data['text']

    # Insert text as a new post
    db = get_db()
    db.execute('insert into posts (post_text, post_authorid , post_threadId, post_forumid) values (?, ?, ?, ?)',(text, creator, str(thread_id), str(forum_id)))
    db.commit()

    response = make_response("Success: Post created")
    response.headers['location'] = '/forums/{}/{}'.format(str(forum_id), thread_id)
    response.status_code = 201
    return response


if __name__ == "__main__":
    app.run(debug=True)
