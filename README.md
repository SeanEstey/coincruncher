![alt text](https://github.com/SeanEstey/coincruncher/raw/master/docs/chart.png)

### Installing bson-numpy

```
python3.6 -m pip install -U git+https://github.com/mongodb/mongo-python-driver.git
python3.6 -m pip install git+https://github.com/mongodb/bson-numpy.git
```

### Jupyterlab config

```
anaconda3/bin/pip install cufflinks, qgrid
jupyter labextension install @jupyter-widgets/jupyterlab-manager
jupyter labextension install qgrid
```

### Run MongoDB Daemon

```
mongod --auth --port 27017 --dbpath /data/db --bind_ip_all
```

### Run daemon

```
python3 daemon.py [--dbhost] [mongo_hostname]
```

### Run client

```
python3 client.py [--dbhost, --portfolio, --watchlist] [args]
```


