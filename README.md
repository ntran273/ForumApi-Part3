# FORUM APP PART 3

IN TUFFIX

NEED TO INSTALL THESE THINGS
```
cd ~
sudo apt-get install python-virtualenv
sudo apt-get install python-pip
virtualenv flask-env
source flask-env/bin/activate
pip install Flask
pip install Flask-BasicAuth
sudo apt install --yes python3-cassandra
pip3 install flask-cassandra
```

To Start a single instance of scyllaDB
```
docker run --name scylla -d scylladb/scylla --smp 1 --memory 1G --overprovisioned 1 --developer-mode 1 --experimental 1
```

To initialize the database doing the following:
```
docker start scylla
docker cp schema.cql scylla:/schema.cql
export FLASK_APP=forum
flask init_db
```
