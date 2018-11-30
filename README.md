# FORUM APP PART 3

IN TUFFIX

NEED TO INSTALL THESE THINGS
```
sudo apt-get install python-pip
pip3 install Flask
pip3 install Flask-BasicAuth
sudo apt install --yes python3-cassandra
pip3 install flask-cassandra
```

If you have already a container named "scylla" in Docker, run this command to delete it first.
```
docker rm -f scylla && docker rmi scylladb/scylla
```

To Start a single instance of scyllaDB
```
docker run --name scylla -d scylladb/scylla --smp 1 --memory 1G --overprovisioned 1 --developer-mode 1 --experimental 1
```

To initialize the database doing the following:
```
docker start scylla
export FLASK_APP=forum
flask init_db
```

GET FORUMS
```
curl localhost:5000/forums
```

GET threads
```
curl localhost:5000/forums/a8b18bea-02cd-40be-a97a-54926db8c75c
```

POST USER
```
curl -v -d '{"username": "dungho", "password": "whatisthat"}' -H "Content-Type: application/json" -X POST localhost:5000/users
```

POST FORUMS
```
curl -v -u holly:password -d '{"forum_name":"HTML"}' -H "Content-Type: application/json" -X POST localhost:5000/forums
```
