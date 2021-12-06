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
Object.defineProperty(exports, "__esModule", { value: true });
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
    getCollectionStats() {
        var _a;
        return __awaiter(this, void 0, void 0, function* () {
            let status = yield ((_a = this.mdb) === null || _a === void 0 ? void 0 : _a.stats());
            return status;
        });
    }
}
function main() {
    return __awaiter(this, void 0, void 0, function* () {
        let notifier = new TelegramNotifyer("bot2140834908", "AAHMHizO44TOo8L5fh3TdW0LQJIY1rJ9ogs", "2137572068");
        const mongo_url = "mongodb://localhost:27017";
        // const mongo_url: string = "mongodb://binance_depth:9iegZ3fDZTkPPQRMqJZ2@dev-sql.slice.local:27017?retryWrites=true&w=majority&authSource=binance_depth";
        const dbname = "binance_depth";
        let mdb = new MongoDBconnector(mongo_url, dbname);
        yield mdb.connect();
        let old_stats;
        let default_timeout = 1000;
        let timeout_limit = 60 * 60 * 1000; // 1h
        let timeout = default_timeout;
        setInterval(function () {
            return __awaiter(this, void 0, void 0, function* () {
                let stats = yield mdb.getCollectionStats();
                console.log(stats);
                if (old_stats) {
                    if (stats.objects == old_stats.objects) {
                        notifier.send_msg("No Database Update for " + timeout / 1000 + " seconds!");
                        if ((timeout * 2) < timeout_limit) {
                            timeout = timeout * 2;
                        }
                        else {
                            timeout = timeout_limit;
                        }
                    }
                    else {
                        timeout = default_timeout;
                    }
                }
                old_stats = stats;
                yield setTimeout(() => { }, timeout);
            });
        }, 0);
        process.on("SIGINT", function () {
            return __awaiter(this, void 0, void 0, function* () {
                console.log("Caught SIGINT Signal");
                yield mdb.disconnect();
            });
        });
    });
}
main();
