import WebSocket from 'ws';
import {MongoClient, Db, Long, Timestamp} from 'mongodb';

import {get} from 'https';


class TelegramNotifyer {
	bot: string;
	key: string;
	chatid: string;
	url: string;

	constructor(bot: string, key: string, chatid: string, url: string = "https://api.telegram.org") {
		this.bot = bot;
		this.key = key;
		this.chatid = chatid;
		if (url.charAt(url.length -1) != "/") {
			url = url + "/";
		}
		this.url = url;
	}

	send_msg(message: string) {
		let botUrl: string = this.url + this.bot + ":" + this.key + "/sendMessage?chat_id=" + this.chatid + "&text=" + message;
		get(botUrl);
		console.log("Telegram Message dispatched!");
	}
}


// record to send to the mongodb
interface MongoRecord {
	time: Date;
	lastUpdateId: Number;
	bids: Number[][];
	asks: Number[][];
}

// records recieved from binance
interface BinanceStreamData {
	lastUpdateId: Number;
	bids: string[][];
	asks: string[][];
}
interface BinanceStream {
	stream: string;
	data: BinanceStreamData;
}


class MongoDBconnector {
	db_url: string;
	db_name: any;
	mdb?: Db;
	mclient: MongoClient;
	knownCollections: string[] = [];

	
	constructor(mongo_url: string, dbname: string) {
		this.db_url = mongo_url;
		this.db_name = dbname;
		this.mclient = new MongoClient(this.db_url);

	}

	async connect() {
		await this.mclient.connect();
		console.log("Mongodb connected");
		this.mdb = this.mclient.db(this.db_name);
	}

	async disconnect() {
		await this.mclient.close();
		console.log("Mongodb disconnected");
	}

	write_dataset(data: BinanceStream) {
		let dbname: string = data.stream.replace("@", "_");
		if (!this.mdb) {
			return;
		}
		const collection = this.mdb.collection(dbname);
		if (!this.knownCollections.includes(dbname)) {
			this.knownCollections.push(dbname);
			collection.createIndex({time: 1});
		}

		var record = <MongoRecord> {
			time: new Date(),
			lastUpdateId: data.data.lastUpdateId,
			// bids: data.data.bids.map(x => [ Math.round(parseFloat(x[0]) * 10 ** 8), parseFloat(x[1]) ]),
			// asks: data.data.asks.map(x => [ Math.round(parseFloat(x[0]) * 10 ** 8), parseFloat(x[1]) ])
			asks: data.data.asks.map(x => x.map(y => parseFloat(y))),
			bids: data.data.bids.map(x => x.map(y => parseFloat(y))),
		};

		collection.insertOne(record).catch( (e) => {
			console.error(e);
		});
		console.log(dbname);
		// console.log(record);
	}

}


class WsConnector {

	mdb: MongoDBconnector;
	telegramAlert: TelegramNotifyer;

	pairs: string[];
	depth: number; // 5, 10 or 20
	streamkey: string;
	retry_count: number;
	timeout: number;
	url: string;

	ws: any;

	// remove retry count?
	constructor(mdb: any, streamkey: string, telegramAlert: TelegramNotifyer, retry_count: number = 5, timeout: number = 10000 /*ms*/) {
		this.mdb = mdb; // mongoDB connector
		this.streamkey = streamkey; // name of the stream
		this.telegramAlert = telegramAlert;
		this.retry_count = retry_count;
		this.timeout = timeout;
		this.pairs = [];
		this.depth = 20;
		this.url = "";
	}

	connect(url: string, pairs: string[], depth: number = 20) {
		if (url.charAt(url.length -1) != "/") {
			url = url + "/";
		}
		this.url = url + "stream?streams=" + this.streamkey;
		this.ws = new WebSocket(this.url);
		this.pairs = pairs;
		this.depth = depth;

		this.ws.on("open", () => this.subscribe(pairs, depth));
		this.ws.on("message", this.onMessage.bind(this));
		this.ws.on("close", () => this.retryConnection(url));
		this.ws.on("error", this.onError.bind(this));
	}

	async retryConnection(url: string) {
		let reconnectionCount: number = 0;
		console.log("disconnected, retrying connection");
		this.telegramAlert.send_msg("disconnected, retrying connection");
		await this.connect(this.url, this.pairs, this.depth);
		// TODO retry count?
		while (this.ws.readyState != WebSocket.OPEN) {
			console.log("Connection attempt failed (" + reconnectionCount + "), retrying in " + this.timeout/1000 + "s");
			this.telegramAlert.send_msg("Connection attempt failed (" + reconnectionCount + "), retrying in " + this.timeout/1000 + "s");
			await new Promise(f => setTimeout(f, this.timeout));
			await this.connect(this.url, this.pairs, this.depth);
			reconnectionCount += 1; 
		}
	}

	onError(e: any) {
		this.telegramAlert.send_msg("ERROR with depth_connector_mongo.js websocket!")};

	subscribe(pairs: string[], depth: number = 20) {
		this.pairs = pairs;
		this.depth = depth;

		const streamNames = []
		// console.log(this.pairs, this.pairs.length);
		for (var i = 0; i < this.pairs.length; i++) {
			streamNames.push(pairs[i].toLowerCase() + "@depth" + depth)
		}

		var msg = JSON.stringify({
		"method": "SUBSCRIBE",
		"params": streamNames,
		"id": 1
		})

		if (this.ws.readyState == WebSocket.OPEN) {
			console.log(msg);
			this.ws.send(msg);
		} else {
			console.log("Could not send Subscribe message, Websocket not ready");
		}
	}

	unsubscribe(pairs: string[], depth: number = 20) {
		this.pairs = pairs;
		this.depth = depth;

		const streamNames = []
		for (var i = 0; i < this.pairs.length; i++) {
			streamNames.push(pairs[i].toLowerCase() + "@depth" + depth)
		}

		var msg = JSON.stringify({
		"method": "UNSUBSCRIBE",
		"params": streamNames,
		"id": 1
		})

		if (this.ws.readyState == WebSocket.OPEN) {
			console.log(msg);
			this.ws.send(msg);
		} else {
			console.log("Could not send Subscribe message, Websocket not ready");
		}

	}

	onMessage(rcvBytes: Buffer) {
		let binanceStream: BinanceStream = JSON.parse(rcvBytes.toString("utf8"));
		
		if (binanceStream.data) {
			this.mdb.write_dataset(binanceStream);
		}
	}

	close() {
		this.ws.close();
		console.log("ws closed");
	}
}





// progst

let notifier = new TelegramNotifyer("bot2140834908", "AAHMHizO44TOo8L5fh3TdW0LQJIY1rJ9ogs", "2137572068");

const mongo_url: string = "mongodb://localhost:27017/";
const dbname: string = "binance_depth";

let depth = 20; 
const wss_retry_count = 5;
const wss_retry_timeout = 1000; //delay in ms
const wss_url = 'wss://stream.binance.com:9443/';

const pairs = ['XRPBTC', 'XRPBNB', 'XRPETH', 'XRPUSDT', 'ADABTC', 'ADAETH', 'ADABNB', 'ADAUSDT', 'LINKBTC', 'LINKETH', 'LINKUSDT', 'LTCBTC', 'LTCETH', 'LTCBNB', 'LTCUSDT', 'XLMBTC', 'XLMETH', 'XLMBNB', 'XLMUSDT', 'XMRBTC', 'XMRETH', 'XMRBNB', 'XMRUSDT', 'TRXBTC', 'TRXETH', 'TRXBNB', 'TRXUSDT', 'VETBTC', 'VETETH', 'VETBNB', 'VETUSDT', 'NEOBTC', 'NEOETH', 'NEOBNB', 'NEOUSDT', 'ATOMBTC', 'ATOMBNB', 'ATOMUSDT', 'ETCBTC', 'ETCETH', 'ETCBNB', 'ETCUSDT', 'ZECBTC', 'ZECETH', 'ZECBNB', 'ZECUSDT', 'BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'ETHBTC', 'BNBBTC', 'BNBETH']
// const pairs = ['BTCUSDT'];


let mdb = new MongoDBconnector(mongo_url, dbname);
mdb.connect();
let wssconnection = new WsConnector(mdb, "depth", notifier);
wssconnection.connect(wss_url, pairs, depth);


process.on("SIGINT", function() {
	console.log("Caught SIGINT Signal");
	wssconnection.unsubscribe(pairs, depth);
	wssconnection.close();
	mdb.disconnect();
})