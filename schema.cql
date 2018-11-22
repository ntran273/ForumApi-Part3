CREATE KEYSPACE forum_api
  WITH replication = {
    'class':'SimpleStrategy',
    'replication_factor' : 3}
  };

USE forum_api;
DROP TABLE IF EXISTS USERS;
DROP TABLE IF EXISTS FORUMS;
DROP TABLE IF EXISTS THREADS;
DROP TABLE IF EXISTS POSTS;
DROP INDEX IF EXISTS user_idx;

CREATE TABLE IF NOT EXISTS USERS{
  username text,
  password text,
  user_id uuid,

  PRIMARY KEY (username, user_id)
};

CREATE TABLE IF NOT EXISTS FORUMS{
  forum_name text,
  username text,
  forum_id uuid,

  PRIMARY KEY (username, forum_id)
};

CREATE TABLE IF NOT EXISTS THREADS{
  thread_id uuid,
  thread_title text,
  username text,
  thread_time timestamp,
  forum_id uuid,

  PRIMARY KEY ((username, thread_id), thread_time)
};

CREATE TABLE IF NOT EXISTS POSTS{
  username text,
  post_text text,
  post_time timestamp,
  forum_id uuid,
  thread_id uuid,
  post_id uuid,

  PRIMARY KEY ((username, forum_id, thread_id, post_id), post_time)
};

CREATE INDEX user_idx ON users(user_id);

insert into threads (thread_id, thread_title, username, thread_time, forum_id) values ('e8c3c68f-4557-450d-a4ad-f3d20cb50c10', 'Does anyone know how to start Redis?!!!!', 'bob', '2017-04-01T11:21:59.001+0000', 'a8b18bea-02cd-40be-a97a-54926db8c75c');
insert into threads (thread_id, thread_title, username, thread_time, forum_id) values ('417ef8c4-0bb1-450c-8bbb-e49b7656cbec', 'When is a good time for me to use Redis?', 'charlie', '2017-04-03T11:21:59.001+0000', 'a8b18bea-02cd-40be-a97a-54926db8c75c');
insert into threads (thread_id, thread_title, username, thread_time, forum_id) values ('ec93faf8-1bb7-4c3a-aa39-c8d6e909e93d', 'Why should I use mongodb over cassandra?', 'charlie', '2017-05-04T11:21:59.001+0000', '29d71465-d020-4e4e-bce4-3bb68f270f92');
insert into threads (thread_id, thread_title, username, thread_time, forum_id) values ('7f308475-f1e4-46b4-8914-df347d6884ef', 'Why should I use Redis over other databases?', 'charlie', '2017-04-06T11:21:59.001+0000', '00fe6541-203c-4a78-b5b7-fa44006f9110');

insert into forums (forum_name, username, forum_id) values ('redis', 'alice', 'a8b18bea-02cd-40be-a97a-54926db8c75c');
insert into forums (forum_name, username, forum_id) values ('mongodb', 'bob', '29d71465-d020-4e4e-bce4-3bb68f270f92');
insert into forums (forum_name, username, forum_id) values ('python', 'bob', 'bb8b19a8-08de-4483-a148-60ab46fafaec');
insert into forums (forum_name, username, forum_id) values ('flask', 'bob', '00fe6541-203c-4a78-b5b7-fa44006f9110');

insert into users (username, password, user_id) values ('holly', 'password', '45847660-c154-4b29-b7aa-6c02c44c7850');
insert into users (username, password, user_id) values ('nguyen', 'password', '8136d793-ecde-4adf-ad18-dcb87b6c94ba');
insert into users (username, password, user_id) values ('bob', 'password', 5d886db9-e9b4-4709-921e-71fe0a2a1948);
insert into users (username, password, user_id) values ('alice', 'password', c9cedf3f-aca0-4aa1-8829-ac04c1543e44);
insert into users (username, password, user_id) values ('bob', 'password', 3242a193-ce2b-46c8-a1fd-dd9000c37461);
insert into users (username, password, user_id) values ('charlie', 'password', 00b38632-91ca-41b2-8a09-01f585b4c3d1);

insert into posts (username, post_text, post_time, forum_id, thread_id, post_id) values ('charlie', 'Why should I use Redis over other databases?', '2017-04-01T11:21:59.001+0000', '00fe6541-203c-4a78-b5b7-fa44006f9110' , '7f308475-f1e4-46b4-8914-df347d6884ef', '6978d64a-c481-420b-8b49-96d8c8f4f050');
insert into posts (username, post_text, post_time, forum_id, thread_id, post_id) values ('bob', It is a key-value database that has very quick access ability', '2017-06-01T11:21:59.001+0000', '00fe6541-203c-4a78-b5b7-fa44006f9110', '7f308475-f1e4-46b4-8914-df347d6884ef', '08601c12-b2a6-4c44-a9a6-49677596ee92');
insert into posts (username, post_text, post_time, forum_id, thread_id, post_id) values ('charlie', When is a good time for me to use Redis?', '2017-08-01T11:21:59.001+0000', 'a8b18bea-02cd-40be-a97a-54926db8c75c', '417ef8c4-0bb1-450c-8bbb-e49b7656cbec', 'f58edf43-a6d6-4a88-9cea-a2714813364c');
