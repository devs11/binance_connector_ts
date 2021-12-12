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
var nconf = require('nconf');
class TelegramNotifyer {
    constructor(bot_enable, bot, key, chatid, url = "https://api.telegram.org") {
        this.bot_enable = bot_enable;
        this.bot = bot;
        this.key = key;
        this.chatid = chatid;
        if (url.charAt(url.length - 1) != "/") {
            url = url + "/";
        }
        this.uri = url;
    }
    send_msg(message) {
        if (this.bot_enable) {
            let botUrl = this.uri + this.bot + ":" + this.key + "/sendMessage?chat_id=" + this.chatid + "&text=" + message;
            try {
                (0, https_1.get)(botUrl);
                Logger.log("Telegram Message dispatched!");
            }
            catch (e) {
                Logger.error("could not send telegram message!");
                Logger.error(e);
            }
        }
    }
}
class MongoDBconnector {
    constructor(configFile, telegramAlert) {
        this.knownCollections = [];
        if (configFile.mongodb.authentication) {
            this.db_url = "mongodb://" + configFile.mongodb.mongodb_username + ":" + configFile.mongodb.mongodb_password + "@" + configFile.mongodb.mongodb_host + ":" + configFile.mongodb.mongodb_port + "?retryWrites=true&w=majority&authSource=" + configFile.mongodb.mongodb_database;
        }
        else {
            this.db_url = "mongodb://" + configFile.mongodb.mongodb_host + ":" + configFile.mongodb.mongodb_port;
        }
        Logger.log("Database URI: " + this.db_url);
        this.db_name = configFile.mongodb.mongodb_database;
        this.telegramAlert = telegramAlert;
        this.mclient = new mongodb_1.MongoClient(this.db_url);
    }
    connect() {
        return __awaiter(this, void 0, void 0, function* () {
            yield this.mclient.connect();
            Logger.log("Mongodb connected");
            this.mdb = this.mclient.db(this.db_name);
            this.mclient.on('close', this.retry_connection);
            this.mclient.on('reconnect', this.reconnected);
        });
    }
    disconnect() {
        return __awaiter(this, void 0, void 0, function* () {
            yield this.mclient.close();
            Logger.log("Mongodb disconnected");
        });
    }
    retry_connection() {
        Logger.error("Lost Database connection, retrying...");
        this.telegramAlert.send_msg("ERROR with depth_connector_mongo.js websocket!");
    }
    reconnected() {
        Logger.log("MongoDB reconnected, go back to sleep");
        this.telegramAlert.send_msg("MongoDB reconnected");
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
            Logger.error(e);
        });
        // Logger.log(dbname);
        // Logger.log(record);
    }
}
class WsConnector {
    // remove retry count?
    // constructor(mdb: any, streamkey: string, retry_count: number = 5, timeout: number = 10000, telegramAlert: TelegramNotifyer) {
    constructor(mdb, configFile, telegramAlert) {
        this.msgCounter = 0;
        this.mdb = mdb; // mongoDB connector
        this.streamkey = configFile.binance.streamkey; // name of the stream
        this.retry_count = configFile.binance.retry_count;
        this.timeout = configFile.binance.timeout;
        this.pairs = [];
        this.depth = 20;
        this.uri = "";
        this.telegramAlert = telegramAlert;
    }
    connect(url, pairs, depth = 20) {
        if (url.charAt(url.length - 1) != "/") {
            url = url + "/";
        }
        this.uri = url + "stream?streams=" + this.streamkey;
        Logger.log("Binance URI: " + this.uri);
        this.ws = new ws_1.default(this.uri);
        this.pairs = pairs;
        this.depth = depth;
        this.ws.on("open", () => this.subscribe(pairs, depth));
        this.ws.on("message", this.onMessage.bind(this));
        this.ws.on("close", () => this.retryConnection(url));
        this.ws.on("error", this.onError.bind(this));
        Logger.log("Websocket connected.");
    }
    retryConnection(url) {
        return __awaiter(this, void 0, void 0, function* () {
            let reconnectionCount = 0;
            Logger.log("disconnected, retrying connection");
            this.telegramAlert.send_msg("disconnected, retrying connection");
            yield this.connect(this.uri, this.pairs, this.depth);
            // TODO retry count?
            while (this.ws.readyState != ws_1.default.OPEN) {
                Logger.log("Connection attempt failed (" + reconnectionCount + "), retrying in " + this.timeout / 1000 + "s");
                this.telegramAlert.send_msg("Connection attempt failed (" + reconnectionCount + "), retrying in " + this.timeout / 1000 + "s");
                yield new Promise(f => setTimeout(f, this.timeout));
                yield this.connect(this.uri, this.pairs, this.depth);
                reconnectionCount += 1;
            }
        });
    }
    onError(e) {
        this.telegramAlert.send_msg("ERROR with depth_connector_mongo.js websocket!");
        Logger.error("ERROR with depth_connector_mongo.js websocket!");
    }
    subscribe(pairs, depth = 20) {
        this.pairs = pairs;
        this.depth = depth;
        const streamNames = [];
        // Logger.log(this.pairs, this.pairs.length);
        for (var i = 0; i < this.pairs.length; i++) {
            streamNames.push(pairs[i].toLowerCase() + "@depth" + depth);
        }
        var msg = JSON.stringify({
            "method": "SUBSCRIBE",
            "params": streamNames,
            "id": 1
        });
        if (this.ws.readyState == ws_1.default.OPEN) {
            Logger.log(msg);
            this.ws.send(msg);
        }
        else {
            Logger.log("Could not send subscribe message");
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
            Logger.log(msg);
            this.ws.send(msg);
        }
        else {
            Logger.log("Could not send unsubscribe message!");
        }
    }
    onMessage(rcvBytes) {
        this.msgCounter = this.msgCounter + 1;
        if (process.stdout.isTTY) {
            process.stdout.write("" + this.msgCounter);
            process.stdout.cursorTo(0);
        }
        try {
            let binanceStream = JSON.parse(rcvBytes.toString("utf8"));
            if (binanceStream.data) {
                this.mdb.write_dataset(binanceStream);
            }
        }
        catch (e) {
            Logger.error("recieved unexpected data from Websocket");
            this.telegramAlert.send_msg("recieved unexpected data from Websocket");
            Logger.log(JSON.parse(rcvBytes.toString("utf8")));
            Logger.error(e);
        }
    }
    close() {
        // disenage callback
        this.ws.on("close");
        this.ws.close();
        Logger.log("ws closed");
    }
}
var Logger;
(function (Logger) {
    let configFile;
    function log(msg) {
        if (configFile.general.enable_log) {
            console.log(msg);
        }
    }
    Logger.log = log;
    function error(msg) {
        if (configFile.general.enable_err) {
            console.error(msg);
        }
    }
    Logger.error = error;
    function setConfig(config) {
        configFile = config;
    }
    Logger.setConfig = setConfig;
})(Logger || (Logger = {}));
function main() {
    return __awaiter(this, void 0, void 0, function* () {
        // read config file
        nconf.file({ file: 'depth_connector_mongo.json' });
        let configFile = nconf.get();
        Logger.setConfig(configFile);
        Logger.log("starting up...");
        var notifier = new TelegramNotifyer(configFile.general.enable_telegram_alert, configFile.telegram.bot_name, configFile.telegram.bot_key, configFile.telegram.chatid);
        let mdb = new MongoDBconnector(configFile, notifier);
        yield mdb.connect();
        let wssconnection = new WsConnector(mdb, configFile, notifier);
        yield wssconnection.connect(configFile.binance.wss_url, configFile.binance.pairs, configFile.binance.depth);
        // check every x seconds if any messages were recieved
        let spam_counter = 1;
        let spam_max = 1;
        setInterval(function () {
            if (wssconnection.msgCounter == 0) {
                if (spam_counter == spam_max) {
                    notifier.send_msg("binance_depth connection stuck with 0 recieved messages, resubscribing...");
                    Logger.error("binance_depth connection stuck with 0 recieved messages, resubscribing...");
                    spam_counter = 1;
                    spam_max = spam_max + 1;
                }
                else {
                    spam_counter = spam_counter + 1;
                }
                wssconnection.subscribe(configFile.binance.pairs, configFile.binance.depth);
            }
            else {
                if (configFile.general.enable_log) {
                    console.log(Date(), "recieved", wssconnection.msgCounter, "messages, thats", wssconnection.msgCounter / (configFile.general.check_interval / 1000), "messages per second,  subscribed pairs count:", wssconnection.pairs.length);
                }
            }
            wssconnection.msgCounter = 0;
            spam_max = 1;
            spam_counter = 1;
        }, configFile.general.check_interval);
        process.on("SIGINT", function () {
            return __awaiter(this, void 0, void 0, function* () {
                Logger.log("Caught SIGINT Signal");
                yield wssconnection.unsubscribe(configFile.binance.pairs, configFile.binance.depth);
                yield wssconnection.close();
                yield mdb.disconnect();
            });
        });
    });
}
main();
