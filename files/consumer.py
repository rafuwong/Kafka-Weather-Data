from kafka import KafkaConsumer, TopicPartition
import report_pb2
import sys #Import for argv
import json #For creating and reading json files
import os #Check for file paths
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt

broker = "localhost:9092"

## JSON Partition File Section ##

def create_json_file(part_num):
    #Json file will correspond to the named partition-N.json
    json_file_name = f"/files/partition-{part_num}.json"
    if not os.path.exists(json_file_name):
        with open(json_file_name, "w") as f:
            #Initialize the contents to {"partition": N, "offset": 0}
            json.dump({"partition": part_num, "offset": 0}, f)

def load_json_file(part_num):
    json_file_name = f"/files/partition-{part_num}.json"
    if os.path.exists(json_file_name):
        with open(json_file_name, "r") as f:
            return json.load(f)
    else:
        print("No partition create_json_file first")

def save_json_file(output_data, num): #output_data is a dict
    json_file_name = f"/files/partition-{output_data[num]['partition']}.json"
    path2 = json_file_name + ".tmp"
    
    with open(path2, "w") as f:
        json.dump(output_data[num], f)
        os.rename(path2, json_file_name)
    
## MAIN CODE SECTION ##

def main(num_partitions):
    consumer_list = [] #Create a list of consumer for each partition
    global broker #For local host 9092
    
    #Loop to create a consumer for each partition
    #and create a list of tuples for each partition
    consumer = KafkaConsumer(
            bootstrap_servers = [broker],
            group_id = 'temperature_group',
        )
    consumer.assign([TopicPartition("temperatures", partition) for partition in num_partitions]) #Manual Partition assignment
    for partition in num_partitions:
        create_json_file(partition)
        consumer_list.append( partition) #Append tuple to consumer_list
    
    data_dict = {}
    topic_part = None
    #Loop through the list of tuples consumer_list
    for partition in consumer_list:
        data_dict[partition] = load_json_file(partition) 
        topic_part = TopicPartition('temperatures', partition)
        consumer.seek(topic_part, data_dict[partition]["offset"])
        
    month_dict = {'01':'January', '02':'February', '03':'March',
            '04':'April', '05':'May', '06':'June', '07':'July',
            '08':'August', '09':'September', '10':'October',
            '11':'November', '12':'December' }
        
    while True:
        batch = consumer.poll(1000) 
        for topic_partition, messages in batch.items():
            for msg in messages:
                report_output = report_pb2.Report.FromString(msg.value) #unserialize messge
                month_year = report_output.date.split('-')[:2] # Extract month and year from date
                month = month_dict[month_year[1]]
                year = month_year[0]
                num = topic_partition.partition
                if month not in data_dict[num]:
                    data_dict[num][month] = {}
                
                if year not in data_dict[num][month]:
                    data_dict[num][month][year] = {
                        "count": 1,
                        "sum": report_output.degrees,
                        "avg": report_output.degrees,
                        "start": report_output.date,
                        "end": report_output.date
                    }
                
                if str(report_output.date) <= data_dict[num][month][year]["end"]:
                    continue
                
                # Update stats for the month/year
                data_dict[num][month][year]["count"] += 1
                data_dict[num][month][year]["sum"] += report_output.degrees
                data_dict[num][month][year]["avg"] = data_dict[num][month][year]["sum"] / data_dict[num][month][year]["count"]
                data_dict[num][month][year]["end"] = report_output.date
                data_dict[num]['offset'] = consumer.position(topic_part)
            save_json_file(data_dict, num) # Save updated stats to JSON file

if __name__ == "__main__":
    # Check to make sure 3 arguments are use the file + 2 numbers
    if len(sys.argv) < 2:
        print("Input more partitions following consumer.py <num1, num2, ...>")
        sys.exit(1)

    #List Comphrehension to take in any number of partitions inputted
    partition_number = [int(parti) for parti in sys.argv[1:]]
    main(partition_number)
