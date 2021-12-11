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
var nconf = require('nconf');
function main() {
    return __awaiter(this, void 0, void 0, function* () {
        // 	const mongo_url: string = "mongodb://localhost:27017";
        // 	const dbname: string = "binance_depth";
        // 	let mdb = new MongoDBconnector(mongo_url, dbname);
        // 	await mdb.connect();
        // 	let stime = Date.now();
        // 	await mdb.read_data();
        // 	console.log("done", Date.now() - stime);
        // 	await mdb.disconnect();
        nconf.file({ file: 'depth_connector_mongo.json' });
        let config = nconf.get();
        console.log(config);
    });
}
main();
