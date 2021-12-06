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

interface MongoDBstats {
	db: String,
	collections: number,
	views: number,
	objects: number,
	avgObjSize: number,
	dataSize: number,
	storageSize: number,
	freeStorageSize: number,
	indexes: number,
	indexSize: number,
	indexFreeStorageSize: number,
	totalSize: number,
	totalFreeStorageSize: number,
	scaleFactor: number,
	fsUsedSize: number,
	fsTotalSize: number,
	ok: number
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

	async getCollectionStats() {
		let status: MongoDBstats = await this.mdb?.stats() as MongoDBstats;
		return status;
	}
}

async function main() {

	let notifier = new TelegramNotifyer("bot2140834908", "AAHMHizO44TOo8L5fh3TdW0LQJIY1rJ9ogs", "2137572068");
	
	const mongo_url: string = "mongodb://localhost:27017";
	// const mongo_url: string = "mongodb://binance_depth:9iegZ3fDZTkPPQRMqJZ2@dev-sql.slice.local:27017?retryWrites=true&w=majority&authSource=binance_depth";
	const dbname: string = "binance_depth";
	
	
	let mdb = new MongoDBconnector(mongo_url, dbname);
	await mdb.connect();

	let old_stats: MongoDBstats;


	let default_timeout: number = 1000;
	let timeout_limit: number = 60*60*1000; // 1h
	let timeout: number = default_timeout;
	setInterval(async function () {
		let stats = await mdb.getCollectionStats();
		console.log(stats);
		if (old_stats) {
			if(stats.objects == old_stats.objects) {
				notifier.send_msg("No Database Update for " + timeout/1000 + " seconds!");
				if ((timeout*2) < timeout_limit) {
					timeout = timeout*2;
				} else {
					timeout = timeout_limit;
				}
			} else {
				timeout = default_timeout;
			}
		}
		old_stats = stats;
		// await setTimeout(() => {}, timeout);

	}, timeout);

	process.on("SIGINT", async function() {
		console.log("Caught SIGINT Signal");
		await mdb.disconnect();
	})
}

main();