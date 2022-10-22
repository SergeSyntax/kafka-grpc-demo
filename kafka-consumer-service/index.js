const { Kafka, PartitionAssigners } = require('kafkajs');

/** #region 
 test
 partitionAssigners: [PartitionAssigners.CooperativeStickyAssignor]
 rebalanced strategy is identical to StickAssignor (balanced like Round Robin, and then minimizes partition movements
  when consumer join/leave the group in order to minimize movements) but supports cooperative rebalance and therefore consumers can keep on consuming from the topic
#endregion */

const kafka = new Kafka({
  clientId: 'kafka-producer-service',
  brokers: ['kafka:9092'],
});

const consumer = kafka.consumer({
  groupId: 'my-second-consumer-group',
  allowAutoTopicCreation: false,
});

const consume = async () => {
  await consumer.connect();
  await consumer.subscribe({
    topic: 'task_created',
    fromBeginning: true,
    partitionAssigners: [PartitionAssigners.roundRobin],
  });

  await consumer.run({
    // eachMessage: async ({ topic, partition, message }) => {
    //   console.log({
    //     value: message.value.toString(),
    //     topic: topic.toString(),
    //     partition: partition,
    //     offset: message.offset,
    //   });
    //   // await consumer.commitOffsets([{ offset: message.offset, topic, partition }]);
    // },

    eachBatch: async ({
      batch,
      resolveOffset,
      heartbeat,
      commitOffsetsIfNecessary,
      uncommittedOffsets,
      isRunning,
      isStale,
      pause,
    }) => {
      for (let message of batch.messages) {
        console.log({
          topic: batch.topic,
          partition: batch.partition,
          highWatermark: batch.highWatermark,
          message: {
            offset: message.offset,
            key: message.key.toString(),
            value: message.value.toString(),
            headers: message.headers,
          },
        });

        resolveOffset(message.offset);
        await commitOffsetsIfNecessary(message.offset);
        await heartbeat();
      }
    },
    autoCommit: false,
    partitionsConsumedConcurrently: 1,
    eachBatchAutoResolve: false,
  });
};

consume();

consumer.on('consumer.connect', console.log);

async function shutDown() {
  await consumer.disconnect();
}

process.on('SIGTERM', shutDown);
process.on('SIGINT', shutDown);
