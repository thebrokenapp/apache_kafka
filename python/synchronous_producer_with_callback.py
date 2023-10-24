from confluent_kafka import Producer

conf = {
          'bootstrap.servers': 'localhost:9092'
       }

def delivery_callback(err, msg):
  if err:
    print("Error occurred: {}".format(err))
  else:
    print("Msg sent to topic {topic} with key as {key} & value as {value} and sent to partition {partition} & written to offset {offset}".format(
      topic = msg.topic(), key = msg.key().decode('utf-8'), value = msg.value().decode('utf-8'), partition = msg.partition(), offset = msg.offset()))

producer = Producer(conf)
topic = 'sales-topic'
# note how messages are sent in order
for i in range(10):
  producer.produce(topic, key="key" + str(i), value="value" + str(i), callback = delivery_callback)
  producer.flush()
                  

#bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic sales-topic --formatter kafka.tools.DefaultMessageFormatter --property print.timestamp=true --property print.key=true --property print.value=true  --property print.partition=true
