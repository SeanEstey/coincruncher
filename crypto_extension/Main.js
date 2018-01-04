// Goes in ~/Library/Application Support/com.dmitrynikolaev.numi/extensions/

numi.addUnit({
    "id": "XRB",
    "phrases": "XRB, xrb, raiblock",
    "format" : "XRB",
    "baseUnitId": "CAD",
    "format": "XRB",
    "ratio":XRB_CAD
});

numi.addUnit({
    "id": "ETH",
    "phrases": "ETH, eth, ethereum",
    "format" : "ETH",
    "baseUnitId": "CAD",
    "format": "ETH",
    "ratio":ETH_CAD
});

numi.addUnit({
    "id": "BTC",
    "phrases": "BTC, btc, bitcoin",
    "format" : "BTC",
    "baseUnitId": "CAD",
    "format": "BTC",
    "ratio":BTC_CAD
});

log("reloaded prices...");
