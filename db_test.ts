import {MongoClient, Db, Long, Timestamp, Admin} from 'mongodb';


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

	async read_data() {
		if (!this.mdb) {
			return;
		}
		const collection = this.mdb.collection("adabnb_depth20");
		let test = await collection.find().forEach(doc => {
			// console.log(doc);
		});
	}
}


async function main() {
	
	const mongo_url: string = "mongodb://localhost:27017";
	const dbname: string = "binance_depth";
	
	
	let mdb = new MongoDBconnector(mongo_url, dbname);
	await mdb.connect();
	let stime = Date.now();
	await mdb.read_data();
	console.log("done", Date.now() - stime);
	await mdb.disconnect();
}

main();