import WebSocket from 'ws';
import {MongoClient, Db, Long, Timestamp} from 'mongodb';

import {get} from 'https';
var nconf = require('nconf');


class TelegramNotifyer {
	bot_enable: Boolean;
	bot: string;
	key: string;
	chatid: string;
	url: string;

	constructor(bot_enable: Boolean, bot: string, key: string, chatid: string, url: string = "https://api.telegram.org") {
		this.bot_enable = bot_enable;
		this.bot = bot;
		this.key = key;
		this.chatid = chatid;
		if (url.charAt(url.length -1) != "/") {
			url = url + "/";
		}
		this.url = url;
	}

	send_msg(message: string) {
		if (this.bot_enable) {
			let botUrl: string = this.url + this.bot + ":" + this.key + "/sendMessage?chat_id=" + this.chatid + "&text=" + message;
			try {
				get(botUrl);
				Logger.log("Telegram Message dispatched!");
			} catch (e) {
				Logger.error("could not send telegram message!");
				Logger.error(e);
			}
		}
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

interface ConfigFile {
	"general": {
        "check_interval": number,
        "enable_log": Boolean,
		"enable_err": Boolean,
        "enable_telegram_alert": Boolean,
    },
    "telegram": {
        "bot_name": string, 
        "bot_key": string,
        "chatid": string,
        "telegram_url": string,
    },
    "binance": {
        "wss_url": string, 
        "streamkey": string,
        "retry_count": number, 
        "timeout": number,
        "depth": number,
        "pairs": string[],
    },
    "mongodb": {
        "mongodb_host": string,
        "mongodb_port": string,
        "mongodb_database": string,
        "authentication": Boolean,
        "mongodb_username": string,
        "mongodb_password": string,
    }
}

interface ListSubscriptions {
	"result": string[],
	"id": number,
}
class MongoDBconnector {
	db_url: string;
	db_name: any;
	mdb?: Db;
	mclient: MongoClient;
	knownCollections: string[] = [];
	telegramAlert: TelegramNotifyer;

	
	constructor(configFile: ConfigFile, telegramAlert: TelegramNotifyer) {
		if (configFile.mongodb.authentication) {
			this.db_url = "mongodb://" + configFile.mongodb.mongodb_username + ":" + configFile.mongodb.mongodb_password + "@" + configFile.mongodb.mongodb_host + ":" + configFile.mongodb.mongodb_port + "?retryWrites=true&w=majority&authSource=" + configFile.mongodb.mongodb_database;
		} else {
			this.db_url = "mongodb://" + configFile.mongodb.mongodb_host + ":" + configFile.mongodb.mongodb_port;
		}
		console.log(this.db_url);
		this.db_name = configFile.mongodb.mongodb_database;
		this.telegramAlert = telegramAlert;
		this.mclient = new MongoClient(this.db_url);
	}

	async connect() {
		await this.mclient.connect();
		Logger.log("Mongodb connected");
		this.mdb = this.mclient.db(this.db_name);

		this.mclient.on('close', this.retry_connection);
		this.mclient.on('reconnect', this.reconnected);
	}

	async disconnect() {
		await this.mclient.close();
		Logger.log("Mongodb disconnected");
	}

	retry_connection() {
		Logger.error("Lost Database connection, retrying...");
		this.telegramAlert.send_msg("ERROR with depth_connector_mongo.js websocket!");
	}

	reconnected() {
		Logger.log("MongoDB reconnected, go back to sleep");
		this.telegramAlert.send_msg("MongoDB reconnected");
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
			Logger.error(e);
		});
		// Logger.log(dbname);
		// Logger.log(record);
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
	msgCounter: number= 0;

	// remove retry count?
	// constructor(mdb: any, streamkey: string, retry_count: number = 5, timeout: number = 10000, telegramAlert: TelegramNotifyer) {
	constructor(mdb: MongoDBconnector, configFile: ConfigFile, telegramAlert: TelegramNotifyer) {
		this.mdb = mdb; // mongoDB connector
		this.streamkey = configFile.binance.streamkey; // name of the stream
		this.retry_count = configFile.binance.retry_count;
		this.timeout = configFile.binance.timeout;
		this.pairs = [];
		this.depth = 20;
		this.url = "";
		this.telegramAlert = telegramAlert;
		
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
		Logger.log("disconnected, retrying connection");
		this.telegramAlert.send_msg("disconnected, retrying connection");
		await this.connect(this.url, this.pairs, this.depth);
		// TODO retry count?
		while (this.ws.readyState != WebSocket.OPEN) {
			Logger.log("Connection attempt failed (" + reconnectionCount + "), retrying in " + this.timeout/1000 + "s");

			this.telegramAlert.send_msg("Connection attempt failed (" + reconnectionCount + "), retrying in " + this.timeout/1000 + "s");
			await new Promise(f => setTimeout(f, this.timeout));
			await this.connect(this.url, this.pairs, this.depth);
			reconnectionCount += 1; 
		}
	}

	onError(e: any) {
		this.telegramAlert.send_msg("ERROR with depth_connector_mongo.js websocket!");
		Logger.error("ERROR with depth_connector_mongo.js websocket!");
	}
		

	subscribe(pairs: string[], depth: number = 20) {
		this.pairs = pairs;
		this.depth = depth;

		const streamNames = []
		// Logger.log(this.pairs, this.pairs.length);
		for (var i = 0; i < this.pairs.length; i++) {
			streamNames.push(pairs[i].toLowerCase() + "@depth" + depth)
		}

		var msg = JSON.stringify({
			"method": "SUBSCRIBE",
			"params": streamNames,
			"id": 1
		})

		if (this.ws.readyState == WebSocket.OPEN) {
			Logger.log(msg);
			this.ws.send(msg);
		} else {
			Logger.log("Could not send Subscribe message, Websocket not ready");
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
			Logger.log(msg);
			this.ws.send(msg);
		} else {
			Logger.log("Could not send Subscribe message, Websocket not ready");
		}

	}

	onMessage(rcvBytes: Buffer) {
		this.msgCounter = this.msgCounter + 1;
		if (process.stdout.isTTY) {
			process.stdout.write(""+this.msgCounter);
			process.stdout.cursorTo(0);
		}

		let binanceStream: BinanceStream = JSON.parse(rcvBytes.toString("utf8"));
		if (binanceStream.data) {
			this.mdb.write_dataset(binanceStream);
		}
	}

	close() {
		// disenage callback
		this.ws.on("close");
		this.ws.close();
		Logger.log("ws closed");
	}
}

module Logger {
	let configFile: ConfigFile;
	export function log(msg: String) {
		console.log(configFile);
		if (configFile.general.enable_log) {
			console.log(msg);
		}
	}

	export function error(msg: String) {
		if (configFile.general.enable_err) {
			console.error(msg);
		}
	}

	export function setConfig(config: ConfigFile) {
		configFile = config;
	}
}


async function main() {
	
	// read config file
	nconf.file({ file: 'depth_connector_mongo.json' });
	
	let configFile: ConfigFile = nconf.get();
	Logger.setConfig(configFile);
	Logger.log("starting up...");

	var notifier: TelegramNotifyer = new TelegramNotifyer(configFile.general.enable_telegram_alert, configFile.telegram.bot_name, configFile.telegram.bot_key, configFile.telegram.chatid);
	let mdb = new MongoDBconnector(configFile, notifier);
	await mdb.connect();
	let wssconnection = new WsConnector(mdb, configFile, notifier);
	await wssconnection.connect(configFile.binance.wss_url, configFile.binance.pairs, configFile.binance.depth);

	// check every x seconds if any messages were recieved
	setInterval(function () {
		if (wssconnection.msgCounter == 0) {
			notifier.send_msg("binance_depth connection stuck with 0 recieved messages, resubscribing...");
			wssconnection.subscribe(configFile.binance.pairs, configFile.binance.depth);
		}
		if (configFile.general.enable_log) {
			console.log(Date(),  "recieved",  wssconnection.msgCounter,  "messages, thats",  wssconnection.msgCounter/(configFile.general.check_interval/1000),  "messages per second,  subscribed pairs count:",  wssconnection.pairs.length);
		}
		wssconnection.msgCounter = 0;
	}, configFile.general.check_interval); 

	process.on("SIGINT", async function() {
		Logger.log("Caught SIGINT Signal");
		await wssconnection.unsubscribe(configFile.binance.pairs, configFile.binance.depth);
		await wssconnection.close();
		await mdb.disconnect();
	})
}

main();

