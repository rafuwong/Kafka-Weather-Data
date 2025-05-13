
from kafka import KafkaConsumer
import report_pb2

def consumer_gen():
    consumer = KafkaConsumer(
        group_id = 'debug',
        bootstrap_servers =['localhost:9092']
    )
    
    consumer.subscribe(["temperatures"])
    
    while True:
        batch = consumer.poll(1000) #1sec get another batch

        for topic_partition, messages in batch.items():
            for msg in messages:
                report_output = report_pb2.Report.FromString(msg.value) #unserialize messge
                key_output = msg.key.decode('utf-8')
                output_dict = {
                    'partition': topic_partition[1],
                    'key': key_output,
                    'date': report_output.date,
                    'degrees': report_output.degrees
                }
                print(output_dict)
    

if __name__ == "__main__":
    consumer_gen()