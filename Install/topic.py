#before runing check if  the kafka server is  runing by using  this ::: telnet localhost 9092
#run the server using this :  bin/kafka-server-start.sh config/kraft/server.properties

from kafka.admin import KafkaAdminClient, NewTopic

admin_client = KafkaAdminClient(bootstrap_servers="localhost:9092")
topic_list = [NewTopic(name="normalized_records", num_partitions=1, replication_factor=1)]
admin_client.create_topics(new_topics=topic_list, validate_only=False)
print("Topic created" )
