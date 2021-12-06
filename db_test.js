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
    read_data() {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.mdb) {
                return;
            }
            const collection = this.mdb.collection("adabnb_depth20");
            let test = yield collection.find().forEach(doc => {
                // console.log(doc);
            });
        });
    }
}
function main() {
    return __awaiter(this, void 0, void 0, function* () {
        const mongo_url = "mongodb://localhost:27017";
        const dbname = "binance_depth";
        let mdb = new MongoDBconnector(mongo_url, dbname);
        yield mdb.connect();
        let stime = Date.now();
        yield mdb.read_data();
        console.log("done", Date.now() - stime);
        yield mdb.disconnect();
    });
}
main();
