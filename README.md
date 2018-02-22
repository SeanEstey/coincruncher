### Install BitEx dev branch > 2.0.0 (Binance support)

```
git clone https://github.com/Crypto-toolbox/bitex.git
cd bitex
git checkout dev
python3 setup.py install
```

Dev 2.0 documentation: https://github.com/Crypto-toolbox/bitex/tree/dev

### Run daemon

```
python3 daemon.py [--dbhost] [mongo_hostname]
```

### Run client

```
python3 client.py [--dbhost, --portfolio, --watchlist] [args]
```


