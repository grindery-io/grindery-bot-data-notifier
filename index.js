import express from 'express';
import './src/subscriber.js';

const app = express();

app.get('/', (req, res) => {
  res.send("Health Check");
});

const port = process.env.PORT || 3000;

const server = app.listen(port, function () {
  console.log(`Bot Data Notifier auth API listening on port ${port}`);
});

export default app;
