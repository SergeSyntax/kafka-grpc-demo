# Kafka CLI

## **kafka-topics**
- list kafka topics
  ```sh
  docker exec -it kafka kafka-topics.sh --bootstrap-server localhost:9092 --list
  ```
- create kafka topic
  ```sh
  docker exec -it kafka kafka-topics.sh --bootstrap-server localhost:9092 --create --topic some_topic_name --partitions 3 --replication-factor 2
  ```
- describe kafka topic
  ```sh
  docker exec -it kafka kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic some_topic_name
  ```
- delete kafka topic
  ```sh
  docker exec -it kafka kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic some_topic_name
  ```

## **kafka-console-producer**
- produce to a specific topic with the acknowledge property set to all:
  ```sh
  docker exec -it kafka kafka-console-producer.sh --bootstrap-server localhost:9092 --topic some_topic_name --property parse.key=true --property key.separator=: --producer-property acks=all
  ```


## **kafka-console-consumer**
- consume a specific topic with from-beginning and format attributes:
  ```sh
  docker exec -it kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic some_topic_name --group some_group --from-beginning --formatter kafka.tools.DefaultMessageFormatter --property print.timestamp=true --property print.key=true --property print.value=true
  ```

## **kafka-consumer-groups**
- note: if you don't specify a consumer group when initializing a consumer it will generate a temporary consumer group that will not commit offset and be removed when it unsubscribe.  

- list consumer groups:
  ```sh
  docker exec -it kafka kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
  ```
- describe one consumer group:
  ```sh
  docker exec -it kafka kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group some_group --describe 
  ```
- delete one consumer group:
  ```sh
  docker exec -it kafka kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group some_group --delete 
  ```
- reset offset for all topics on one consumer group (only if inactive):
  ```sh
  docker exec -it kafka kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group some_group --reset-offsets --to-earliest --execute --all-topics
  ```
- move the offset forward by two for a specific consumer group and topic, can use negative values to reduce offset (only if inactive):
  ```sh
  docker exec -it kafka kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group some_group --reset-offsets --shift-by 2 --execute --topic some_topic
  ```
