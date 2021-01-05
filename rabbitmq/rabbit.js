const amqp = require("amqplib");

const AMQP_CONN_URL = process.env.AMQP_CONN_URL;

// This worker consume jobs from the request queue
const CONSUME_QUEUE_NAME = "request";
// This worker publish processed jobs to the processed queue
const PUBLISH_QUEUE_NAME = "processed";

// To publish
var publishConnection = null;
var publishChannel = null;
// To consume
var consumeConnection = null;
var consumeChannel = null;

// Open connection for publishing
const openPublishConnection = async () => {
	try {
        publishConnection = await amqp.connect(AMQP_CONN_URL)
		publishConnection.on('close', (err) => {
			if (err) throw err;
			console.log('[AMQP] Publish connection closed!'); 
		})
		console.log('[AMQP] Publish connection established!');
	}	
    catch (err){
        throw err;
    }
}

// Open channel for publishing
const openPublishChannel = async () => {
	try {
        publishChannel = await publishConnection.createChannel();
        publishChannel.on('close', (err) => {
			if (err) throw err;
			console.log('[AMQP] Publish channel closed!');
		})
		console.log('[AMQP] Publish channel created!');
	}	
    catch (err){
        throw err;
    }
}

// Open connection for consuming
const openConsumeConnection = async () => {
	try {
        consumeConnection = await amqp.connect(AMQP_CONN_URL)
		consumeConnection.on('close', (err) => {
			if (err) throw err;
			console.log('[AMQP] Consume connection closed!'); 
		})
		console.log('[AMQP] Consume connection established!');
	}	
    catch (err){
        throw err;
    }
}

// Open channel for consuming
const openConsumeChannel = async () => {
	try {
        consumeChannel = await consumeConnection.createChannel();
        consumeChannel.on('close', (err) => {
			if (err) throw err;
			console.log('[AMQP] Consume channel closed!');
		})
		console.log('[AMQP] Consume channel created!');
	}	
    catch (err){
        throw err;
    }
}

const openAll = async () => {
	try {
		// Open connections and channels
        await openPublishConnection();
		await openPublishChannel();
		
		await openConsumeConnection();
		await openConsumeChannel();
	}	
    catch (err){
        throw err;
    }
}

const closeAll = async () => {
	try {
		// Close all channels and connections
        await publishChannel.close();
        await publishConnection.close();
        
        await consumeChannel.close();
        await consumeConnection.close();
	}	
    catch (err){
        throw err;
    }
}

const publish = async (job) => {
    try {
        await publishChannel.assertQueue(PUBLISH_QUEUE_NAME);
        await publishChannel.sendToQueue(PUBLISH_QUEUE_NAME, Buffer.from(JSON.stringify(job)), {persistent: true});
        console.log(`[AMQP] Job for user ${job.userId} published`);
    }
    catch (err){
        throw err;
    }
}

const consume = async (processFun) => {
    try {
        await consumeChannel.assertQueue(CONSUME_QUEUE_NAME);
		await consumeChannel.prefetch(1);
        consumeChannel.consume(CONSUME_QUEUE_NAME, async (job) => {
            const data = JSON.parse(job.content.toString());
			console.log(`[AMQP] Received job for user ${data.userId}`);

			// Process the job
			const answer = await processFun(data.link);
			console.log('[AMQP] Job processed!');
			// Publish answer to processed queue
			await publish({answer, userId: data.userId});
			consumeChannel.ack(job);
        })
		console.log("[AMQP] Waiting for jobs...");
	}
    catch (err){
        throw err;
    }
}

module.exports = {
	openAll, closeAll, consume
};