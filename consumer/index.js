import { Consumer, KafkaClient } from 'kafka-node'
import crypto from 'crypto'

const client = new KafkaClient()

const topicsToCreate = [
  {
    topic: 'new_users',
    partitions: 1,
    replicationFactor: 1
  }
]

const users = new Map() // users 'database'

await new Promise((resolve, reject) => {
  client.createTopics(topicsToCreate, (error, result) => {
    if (error) reject(error)

    resolve()
  })
})

const consumer = new Consumer(client, topicsToCreate.map(data => ({ topic: data.topic })))

const topicHandlers = {
  new_users: (value) => {
    const user = JSON.parse(value)

    user.uuid = crypto.randomUUID()

    users.set(user.uuid, user)

    console.log('user created')

    console.table([...users.values()])
  }
}

consumer.on('message', (message) => {
  const { value, topic } = message

  topicHandlers[topic](value)
})

consumer.on('error', (error) => console.log('CONSUMER ERROR: ', error.message))

console.log('consumer started...')