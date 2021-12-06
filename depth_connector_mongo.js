"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const ws_1 = __importDefault(require("ws"));
const mongodb_1 = require("mongodb");
const https_1 = require("https");
class TelegramNotifyer {
    constructor(bot, key, chatid, url = "https://api.telegram.org") {
        this.bot = bot;
        this.key = key;
        this.chatid = chatid;
        if (url.charAt(url.length - 1) != "/") {
            url = url + "/";
        }
        this.url = url;
    }
    send_msg(message) {
        let botUrl = this.url + this.bot + ":" + this.key + "/sendMessage?chat_id=" + this.chatid + "&text=" + message;
        (0, https_1.get)(botUrl);
        console.log("Telegram Message dispatched!");
    }
}
class MongoDBconnector {
    constructor(mongo_url, dbname) {
        this.knownCollections = [];
        this.db_url = mongo_url;
        this.db_name = dbname;
        this.mclient = new mongodb_1.MongoClient(this.db_url);
    }
    connect() {
        return __awaiter(this, void 0, void 0, function* () {
            yield this.mclient.connect();
            console.log("Mongodb connected");
            this.mdb = this.mclient.db(this.db_name);
        });
    }
    disconnect() {
        return __awaiter(this, void 0, void 0, function* () {
            yield this.mclient.close();
            console.log("Mongodb disconnected");
        });
    }
    write_dataset(data) {
        let dbname = data.stream.replace("@", "_");
        if (!this.mdb) {
            return;
        }
        const collection = this.mdb.collection(dbname);
        if (!this.knownCollections.includes(dbname)) {
            this.knownCollections.push(dbname);
            collection.createIndex({ time: 1 });
        }
        var record = {
            time: new Date(),
            lastUpdateId: data.data.lastUpdateId,
            // bids: data.data.bids.map(x => [ Math.round(parseFloat(x[0]) * 10 ** 8), parseFloat(x[1]) ]),
            // asks: data.data.asks.map(x => [ Math.round(parseFloat(x[0]) * 10 ** 8), parseFloat(x[1]) ])
            asks: data.data.asks.map(x => x.map(y => parseFloat(y))),
            bids: data.data.bids.map(x => x.map(y => parseFloat(y))),
        };
        collection.insertOne(record).catch((e) => {
            console.error(e);
        });
        // console.log(dbname);
        // console.log(record);
    }
}
class WsConnector {
    // remove retry count?
    constructor(mdb, streamkey, telegramAlert, retry_count = 5, timeout = 10000 /*ms*/) {
        this.msgCounter = 0;
        this.mdb = mdb; // mongoDB connector
        this.streamkey = streamkey; // name of the stream
        this.telegramAlert = telegramAlert;
        this.retry_count = retry_count;
        this.timeout = timeout;
        this.pairs = [];
        this.depth = 20;
        this.url = "";
    }
    connect(url, pairs, depth = 20) {
        if (url.charAt(url.length - 1) != "/") {
            url = url + "/";
        }
        this.url = url + "stream?streams=" + this.streamkey;
        this.ws = new ws_1.default(this.url);
        this.pairs = pairs;
        this.depth = depth;
        this.ws.on("open", () => this.subscribe(pairs, depth));
        this.ws.on("message", this.onMessage.bind(this));
        this.ws.on("close", () => this.retryConnection(url));
        this.ws.on("error", this.onError.bind(this));
    }
    retryConnection(url) {
        return __awaiter(this, void 0, void 0, function* () {
            let reconnectionCount = 0;
            console.log("disconnected, retrying connection");
            this.telegramAlert.send_msg("disconnected, retrying connection");
            yield this.connect(this.url, this.pairs, this.depth);
            // TODO retry count?
            while (this.ws.readyState != ws_1.default.OPEN) {
                console.log("Connection attempt failed (" + reconnectionCount + "), retrying in " + this.timeout / 1000 + "s");
                this.telegramAlert.send_msg("Connection attempt failed (" + reconnectionCount + "), retrying in " + this.timeout / 1000 + "s");
                yield new Promise(f => setTimeout(f, this.timeout));
                yield this.connect(this.url, this.pairs, this.depth);
                reconnectionCount += 1;
            }
        });
    }
    onError(e) {
        this.telegramAlert.send_msg("ERROR with depth_connector_mongo.js websocket!");
    }
    ;
    subscribe(pairs, depth = 20) {
        this.pairs = pairs;
        this.depth = depth;
        const streamNames = [];
        // console.log(this.pairs, this.pairs.length);
        for (var i = 0; i < this.pairs.length; i++) {
            streamNames.push(pairs[i].toLowerCase() + "@depth" + depth);
        }
        var msg = JSON.stringify({
            "method": "SUBSCRIBE",
            "params": streamNames,
            "id": 1
        });
        if (this.ws.readyState == ws_1.default.OPEN) {
            console.log(msg);
            this.ws.send(msg);
        }
        else {
            console.log("Could not send Subscribe message, Websocket not ready");
        }
    }
    unsubscribe(pairs, depth = 20) {
        this.pairs = pairs;
        this.depth = depth;
        const streamNames = [];
        for (var i = 0; i < this.pairs.length; i++) {
            streamNames.push(pairs[i].toLowerCase() + "@depth" + depth);
        }
        var msg = JSON.stringify({
            "method": "UNSUBSCRIBE",
            "params": streamNames,
            "id": 1
        });
        if (this.ws.readyState == ws_1.default.OPEN) {
            console.log(msg);
            this.ws.send(msg);
        }
        else {
            console.log("Could not send Subscribe message, Websocket not ready");
        }
    }
    onMessage(rcvBytes) {
        this.msgCounter = this.msgCounter + 1;
        process.stdout.write("" + this.msgCounter);
        process.stdout.cursorTo(0);
        let binanceStream = JSON.parse(rcvBytes.toString("utf8"));
        if (binanceStream.data) {
            this.mdb.write_dataset(binanceStream);
        }
    }
    close() {
        // disenage callback
        this.ws.on("close");
        this.ws.close();
        console.log("ws closed");
    }
}
function main() {
    return __awaiter(this, void 0, void 0, function* () {
        // progst
        let notifier = new TelegramNotifyer("bot2140834908", "AAHMHizO44TOo8L5fh3TdW0LQJIY1rJ9ogs", "2137572068");
        const mongo_url = "mongodb://localhost:27017/";
        // const mongo_url: string = "mongodb://binance_depth:9iegZ3fDZTkPPQRMqJZ2@dev-sql.slice.local:27017?retryWrites=true&w=majority&authSource=binance_depth";
        // const mongo_url: string = "mongodb://admin:8uApV8s6dhgiV83KHUvX@dev-sql.slice.local:27017?retryWrites=true&w=majority";
        const dbname = "binance_depth";
        let depth = 20;
        const wss_retry_count = 5;
        const wss_retry_timeout = 1000; //delay in ms
        const wss_url = 'wss://stream.binance.com:9443/';
        const pairs = ['XRPBTC', 'XRPBNB', 'XRPETH', 'XRPUSDT', 'ADABTC', 'ADAETH', 'ADABNB', 'ADAUSDT', 'LINKBTC', 'LINKETH', 'LINKUSDT', 'LTCBTC', 'LTCETH', 'LTCBNB', 'LTCUSDT', 'XLMBTC', 'XLMETH', 'XLMBNB', 'XLMUSDT', 'XMRBTC', 'XMRETH', 'XMRBNB', 'XMRUSDT', 'TRXBTC', 'TRXETH', 'TRXBNB', 'TRXUSDT', 'VETBTC', 'VETETH', 'VETBNB', 'VETUSDT', 'NEOBTC', 'NEOETH', 'NEOBNB', 'NEOUSDT', 'ATOMBTC', 'ATOMBNB', 'ATOMUSDT', 'ETCBTC', 'ETCETH', 'ETCBNB', 'ETCUSDT', 'ZECBTC', 'ZECETH', 'ZECBNB', 'ZECUSDT', 'BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'ETHBTC', 'BNBBTC', 'BNBETH'];
        // const pairs = ['BTCUSDT'];
        let mdb = new MongoDBconnector(mongo_url, dbname);
        yield mdb.connect();
        let wssconnection = new WsConnector(mdb, "depth", notifier);
        yield wssconnection.connect(wss_url, pairs, depth);
        // check every 5 seconds if any messages were recieved
        let timeout = 10000;
        setInterval(function () {
            if (wssconnection.msgCounter == 0) {
                notifier.send_msg("binance_depth connection stuck with 0 recieved messages");
            }
            console.log(Date(), "recieved", wssconnection.msgCounter, "messages, thats", wssconnection.msgCounter / (timeout / 1000), "messages per second, subscribed pairs count:", wssconnection.pairs.length);
            wssconnection.msgCounter = 0;
        }, timeout);
        process.on("SIGINT", function () {
            return __awaiter(this, void 0, void 0, function* () {
                console.log("Caught SIGINT Signal");
                yield wssconnection.unsubscribe(pairs, depth);
                yield wssconnection.close();
                yield mdb.disconnect();
            });
        });
    });
}
main();
