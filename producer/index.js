import { Producer, KafkaClient } from 'kafka-node'

const client = new KafkaClient()

const producer = new Producer(client)

const payloads = [
  { topic: 'new_users', messages: JSON.stringify({ name: 'Eduardo' }) },
];

producer.on('error', function (err) { console.log(err) })

await new Promise((resolve, reject) => {
  producer.on('ready', function () {
    producer.send(payloads, function (err, data) {
      if (err) reject(err)
      console.log(data);
      resolve()
    });
  })
})

process.exit(1)