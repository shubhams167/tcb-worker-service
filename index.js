const express = require('express');
const app = express();
const http = require('http').createServer(app);
const io = require('socket.io')(http, {
	cors: {
		origin: '*',
	}
});
const rabbit = require('./rabbitmq/rabbit');

io.on('connection', async (socket) => {
	console.log('[socket.io] Client connected');
	
	try{
		// Open all connections and channels
		await rabbit.openAll();
		// Start consuming jobs
		rabbit.consume(getAnswer);
	} catch(err) {
		console.error(`[AMQP] ${err}`);
	}

	socket.on('disconnect', async () => {
		console.log('[socket.io] Client disconnected');
		// Close all channels and connections
		await rabbit.closeAll();
	});
});

app.use(express.json());

app.get('/', (req, res, next) => {
	res.status(200).send(`I am 👷🏼‍♂️ ${process.env.WORKER_ID}`);
});

app.get('/qna', async (req, res, next) => {
	const link = req.body.link;
	try{
		const answer = await getAnswer(link);
		res.attachment('answer.html');
		res.status(200).send(answer);
	} catch(e){
		console.log(e);
		res.status(500);
		next(e);
	}
});

const getAnswer = (link) => {
	return new Promise((resolve, reject) => {
		// Get a connected client
		const sockets = io.of('/').sockets;
		const socketID = sockets.keys().next().value;
		const socket = sockets.get(socketID);

		socket.emit("get-question", link, (data) => {
			if (!data) reject('Error');
			resolve(data);
		});
	});
}

http.listen(3000, async () => {
	console.log(`Worker ${process.env.WORKER_ID} listening on port 3000`);
});
