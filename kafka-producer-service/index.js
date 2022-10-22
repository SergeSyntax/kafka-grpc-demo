const { Kafka, CompressionTypes } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'kafka-producer-service',
  brokers: ['kafka:9092'],
});

const producer = kafka.producer({
  allowAutoTopicCreation: false,
});

const produceMessage = async () => {
  await producer.connect();
  const [recordMetadata] = await producer.send({
    topic: 'task_created',
    acks: -1,
    messages: [{ key: 'id_2', value: '2Hello KafkaJS user!' }],
    compression: CompressionTypes.GZIP,
  });

  console.log(recordMetadata);

  await producer.disconnect();
};

setInterval(produceMessage, 20000);
