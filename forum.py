import flask
import uuid
from flask import Flask,request, _app_ctx_stack, session, jsonify, render_template, json, abort, Response, flash, g, current_app, make_response
from flask_basicauth import BasicAuth
from flask.cli import AppGroup
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from flask_cassandra import CassandraCluster
from datetime import datetime
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
    ctx = _app_ctx_stack.top
    if not hasattr(ctx, 'cassandra_cluster'):
        # ctx = top
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
    os.system("docker cp schema.cql scylla:/schema.cql")
    os.system("docker exec -it scylla cqlsh -f schema.cql")

#Create Command initdb
#To use command, run in terminal export FLASK_APP = appname, flask initdb
@app.cli.command('init_db')
def initdb_command():
    init_db()
    print('Initialize the database.')

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
    generateUserId = uuid.uuid4();

    query = 'SELECT username FROM forum_api.users;'
    listusername = query_db(query)
    for user_name in listusername:
        if user_name['username'] == username:
            error = '409 A username already exists'
            return make_response(jsonify({'error': error}), 409)
    db = get_db()
    db.execute("""insert into forum_api.users (username, password, user_id) values (%s, %s, %s)""",(username, password, generateUserId))

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

    #check if username is in databases
    query = "SELECT user_id FROM forum_api.USERS WHERE username = '{}'".format(str(user))
    useracc = query_db(query)
    if not useracc:
       error = '404 No user exists with the user of ' + str(user)
       return make_response(jsonify({'error': error}), 404)

    if(creator == str(user)):
        db = get_db()
        db.execute("""update forum_api.users set password = %s where username= %s and user_id in (%s)""",(newpassword, creator, useracc[0]['user_id']))
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
    return jsonify(forums)

#List threads in the specified forum
@app.route('/forums/<uuid:forum_id>', methods = ['GET'])
def api_threads(forum_id):
    query = 'SELECT forum_id FROM forum_api.forums WHERE forum_id = {}'.format(str(forum_id)) + ' ALLOW FILTERING;'
    forum = query_db(query)
    if not forum :
        error = '404 No forum exists with the forum id of ' + str(forum_id)
        return make_response(jsonify({'error': error}), 404)
    else:
        query2 = 'SELECT thread_id, username as creator, thread_title as title, thread_time as timestamp FROM forum_api.threads WHERE forum_id = {}'.format(str(forum_id)) + ' ALLOW FILTERING;'
        threads = query_db(query2)
        return jsonify(threads)


#List posts in the specified thread
@app.route('/forums/<uuid:forum_id>/<uuid:thread_id>', methods=['GET'])
def get_post(forum_id, thread_id):
    print(forum_id, thread_id)
     # Select from forums on forum id to make sure that the forum exists
    query = 'SELECT * FROM forum_api.forums WHERE forum_id = {}'.format(str(forum_id)) + ' ALLOW FILTERING;'
    forum = query_db(query)
    if not forum:
        error = '404 No forum exists with the forum id of ' + str(forum_id)
        return make_response(jsonify({'error': error}), 404)
    # Select from threads on thread_id to make sure thread exists
    query = 'SELECT * FROM forum_api.threads WHERE thread_id = {}'.format(str(thread_id)) + ' ALLOW FILTERING;'
    thread = query_db(query)
    if not thread:
        error = '404 No thread exists with the thread id of ' + str(thread_id)
        return make_response(jsonify({'error': error}), 404)
    # query2 = 'SELECT thread_id, username as creator, thread_title as title, thread_time as timestamp FROM forum_api.threads WHERE forum_id = {}'.format(str(forum_id)) + ' ALLOW FILTERING;'

    query = "SELECT username as author, post_text as text, post_time as timestamp FROM forum_api.posts WHERE thread_id = {} AND forum_id = {}".format(thread_id, forum_id)
    post = query_db(query)
    return jsonify(post)

#POST FORUM
@app.route('/forums', methods=['POST'])
@basic_auth.required
def post_forums():

    data = request.get_json(force=True)
    name = data['forum_name']

    creator = current_app.config['BASIC_AUTH_USERNAME']
    query = 'SELECT forum_name FROM forum_api.forums'
    forum_names = query_db(query)
    for forum_name in forum_names:
        if forum_name['forum_name'] == name:
            error = '409 A forum already exists with the name ' + name
            return make_response(jsonify({'error': error}), 409)
    generateForumId = uuid.uuid4()
    db = get_db()
    db.execute("""insert into forum_api.forums (forum_name, username, forum_id) values (%s, %s, %s)""",(name, creator, generateForumId))
    response = make_response('Success: forum created')
    response.headers['location'] = '/forums/{}'.format(str(generateForumId))
    response.status_code = 201

    return response

#POST THREAD
@app.route('/forums/<uuid:forum_id>', methods=['POST'])
@basic_auth.required
def post_thread(forum_id):

    data = request.get_json(force=True)
    title = data['thread_title']
    text = data['text']
    creator = current_app.config['BASIC_AUTH_USERNAME']

     # Select from forums on forum id to make sure that the forum exists
    query = 'SELECT * FROM forum_api.forums WHERE forum_id = {}'.format(str(forum_id)) + ' ALLOW FILTERING;'
    forum = query_db(query)
    print(forum)
    if len(forum) == 0:
        error = '404 No forum exists with the forum id of ' + str(forum_id)
        return make_response(jsonify({'error': error}), 404)
    # If forum exist, insert into threads table
    generateThreadId = uuid.uuid4()
    generatePostId = uuid.uuid4()

    db = get_db()
    db.execute("""insert into forum_api.threads (thread_title, username, forum_id, thread_time, thread_id) values (%s, %s, %s, toTimestamp(now()), %s)""",(title, creator, forum_id, generateThreadId))
    # Insert text as a new post
    db.execute("""insert into forum_api.posts (post_text, username, forum_id, thread_id, post_id, post_time) values (%s, %s, %s, %s, %s, toTimestamp(now()))""",(text, creator, forum_id, generateThreadId, generatePostId))

    response = make_response("Success: Thread and Post created")
    response.headers['location'] = '/forums/{}/{}'.format(forum_id, generateThreadId)
    response.status_code = 201
    return response

#POST POST
@app.route('/forums/<uuid:forum_id>/<uuid:thread_id>', methods=['POST'])
@basic_auth.required
def post_post(forum_id, thread_id):
    # Select from forums on forum id to make sure that the forum exists
    query = 'SELECT * FROM forum_api.forums WHERE forum_id = {}'.format(str(forum_id)) + ' ALLOW FILTERING;'
    forum = query_db(query)
    if len(forum) == 0:
        error = '404 No forum exists with the forum id of ' + str(forum_id)
        return make_response(jsonify({'error': error}), 404)
    # Select from threads on thread_id to make sure thread exists
    query = 'SELECT * FROM forum_api.threads WHERE thread_id = {}'.format(str(thread_id)) + ' ALLOW FILTERING;'

    # query = 'SELECT * FROM threads WHERE id = ' + str(thread_id)
    thread = query_db(query)
    print(thread)
    if len(thread) == 0:
        error = '404 No thread exists with the thread id of ' + str(thread_id)
        return make_response(jsonify({'error': error}), 404)

    data = request.get_json(force=True)
    creator = current_app.config['BASIC_AUTH_USERNAME']
    text = data['text']

    #Generate POST uuid4
    generatePostId = uuid.uuid4()

    # Insert text as a new post
    db = get_db()
    db.execute("""insert into forum_api.posts (post_text, username, forum_id, thread_id, post_id, post_time) values (%s, %s, %s, %s, %s, toTimestamp(now()))""",(text, creator, forum_id, thread_id, generatePostId))

    response = make_response("Success: Post created")
    response.headers['location'] = '/forums/{}/{}'.format(str(forum_id), thread_id)
    response.status_code = 201
    return response


if __name__ == "__main__":
    app.run(debug=True)
