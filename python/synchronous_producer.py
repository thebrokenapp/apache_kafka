from confluent_kafka import Producer

conf = {'bootstrap.servers': 'localhost:9092}

producer = Producer(conf)

# note how messages are sent in order
for i in range(10):
  producer.produce(topic, key="key" + sr(i), value="value" + str(i)
  producer.flush()
                  
