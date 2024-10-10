const cluster = require('cluster');
const express = require('express');
const Redis = require('ioredis');
const { RateLimiterRedis } = require('rate-limiter-flexible');
const fs = require('fs');
const path = require('path');

const numCPUs = 2; 
const app = express();
const port = 3000;


const redisClient = new Redis({
  host: 'localhost',
  port: 6379,
});


const rateLimiter = new RateLimiterRedis({
  storeClient: redisClient,
  keyPrefix: 'ratelimit',
  points: 20, 
  duration: 60, 
});

const secondLimiter = new RateLimiterRedis({
  storeClient: redisClient,
  keyPrefix: 'secondlimit',
  points: 1,
  duration: 1, 
});


const taskQueue = {};


async function processTask(userId) {
  const timestamp = new Date().toISOString();
  const logMessage = `Task completed for user ${userId} at ${timestamp}\n`;
  
  
  fs.appendFile(path.join(__dirname, 'task_log.txt'), logMessage, (err) => {
    if (err) console.error('Error writing to log file:', err);
  });
  
  console.log(logMessage);
}


async function processQueue(userId) {
  if (taskQueue[userId] && taskQueue[userId].length > 0) {
    const task = taskQueue[userId].shift();
    await processTask(userId);
    setTimeout(() => processQueue(task), 1000); 
  }
}

app.use(express.json());

app.post('/task', async (req, res) => {
  const { userId } = req.body;

  if (!userId) {
    return res.status(400).json({ error: 'User ID is required' });
  }

  try {
    await rateLimiter.consume(userId);
    await secondLimiter.consume(userId);

   
    await processTask(userId);
    res.json({ message: 'Task processed successfully' });
  } catch (rateLimitError) {
   
    if (!taskQueue[userId]) {
      taskQueue[userId] = [];
    }
    taskQueue[userId].push(userId);

    if (taskQueue[userId].length === 1){
      setTimeout(() => processQueue(userId), 1000);
    }
    res.status(200).json({ message: 'Task queued for processing' });
  }
});

if (cluster.isMaster) {
  console.log(`Master ${process.pid} is running`);
  for (let i = 0; i < numCPUs; i++){
    cluster.fork();
  }

  cluster.on('exit', (worker, code, signal) => {
    console.log(`Worker ${worker.process.pid} died`);
    cluster.fork();
  });
} else{
  app.listen(port, () => {
    console.log(`Worker ${process.pid} started and listening on port ${port}`);
  });
}