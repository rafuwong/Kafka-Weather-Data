from kafka import KafkaAdminClient, KafkaProducer, KafkaConsumer
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
from kafka.errors import UnknownTopicOrPartitionError
import time
import report_pb2
broker = 'localhost:9092'
admin_client = KafkaAdminClient(bootstrap_servers=[broker])

try:
    admin_client.delete_topics(["temperatures"])
    print("Deleted topics successfully")
except UnknownTopicOrPartitionError:
    print("Cannot delete topic/s (may not exist yet)")

time.sleep(3) # Deletion sometimes takes a while to reflect

# TODO: Create topic 'temperatures' with 4 partitions and replication factor = 1
admin_client.create_topics([NewTopic(name="temperatures", num_partitions=4, replication_factor=1)])

#Create Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=[broker],
    retries = 10, # Setting: Retry up to 10 times
    acks = 'all' # Setting: Wait for ALL in-sync replicas to recieve data
    )

#Create a Function to publish the temperature temperature data

def stream_temp(date_entry, degree):
    report_msg = report_pb2.Report()
    report_msg.date = date_entry
    report_msg.degrees = degree
    #Use a .SerializeToString() to convert protobuf obj to bytes
    serialized_output = report_msg.SerializeToString()

    #Date is in YYYY-MM-DD format so use str.split('-')[1] to grab month
    month = report_msg.date.split('-')[1]
    #Create a dictionary to convert to month number (MM) to full string of the month
    month_dict = {'01':'January', '02':'February', '03':'March',
            '04':'April', '05':'May', '06':'June', '07':'July',
            '08':'August', '09':'September', '10':'October',
            '11':'November', '12':'December' }

    month_str = month_dict[month].encode() #Encode to bytes
    #Stream the temp with month name as message key 
    producer.send(topic ='temperatures', key = month_str, value = serialized_output)

import weather

while True:
    for date, degrees in weather.get_next_weather(delay_sec=.1):
        stream_temp(date, degrees)



print("Topics:", admin_client.list_topics())