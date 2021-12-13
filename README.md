# Binance connector TS

Typescript tool for getting Binance Order book Data / Market depth data in real time using the Binance Websocket API ( https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md ) and writing it to a mongoDB Database. 

The Collections are automatically created and an Index is set up on "time". 

I'd **highly** recommend to enable database compression on mongoDB (otherwise you will require about 3-4GB / day since mongoDB uses 8-Byte Floats): https://docs.mongodb.com/manual/core/wiredtiger/

## Setup

Copy the `depth_connector_mongo.json_sample` to `depth_connector_mongo.json` and edit it accordingly. 

Run `npm install` to install the dependencies. 

Run `tsc; node depth_connector_mongo.js` to start the Connector. 

I'd recommend running it with the **PM2 Process Manager** (https://pm2.keymetrics.io/). 

