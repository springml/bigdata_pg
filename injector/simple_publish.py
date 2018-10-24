
#python program to publish messages into google pubsub

'''Running the program:
export GOOGLE_APPLCATION_CREDENTIALS=<service-account.json>
python simple_publish.py --topic_path=projects/myspringml/topics/sjsml --input_file=input.json --num_messages=1
python simple_publish.py --topic_path=projects/dataflowtesting-218212/topics/streamdemo --input_file=input.json --num_messages=1'''

from google.cloud import pubsub_v1
import json
import argparse
import time
import random

def callback(message_future):
    if message_future.exception():
        print('Publishing message on {} threw an Exception {}.'.format(
            topic_name, message_future.exception()))
    else:
        print(message_future.result())

names = ["Ini", "Sreekar", "Arya", "Dhruv", "Shruthi", "Girish"]
numbers = [1,2,3,4,5,6,7,8,9]

#function to publish messages. 
def publish_function(topic_path,input_file,num_messages):
    publisher = pubsub_v1.PublisherClient()
    print('Published message IDs:')
    #for loop to publish number of messagers passed as argument
    for n in range(1, int(num_messages)+1):
     #open the input file to read the sample message
    #  with open(input_file) as f:
    #     data = str(json.load(f))
     names_index = random.randint(0, len(names)-1)
     numbers_index = random.randint(0, len(numbers)-1)

    #  data = names[names_index] + ", " + str(numbers[numbers_index])
     data = names[names_index] + ", " + str(n)
     
     print(data)
     #data being published into pubsub should be a byte stream
     data = data.encode('utf-8')
     #publish the data
     message_future = publisher.publish(topic_path, data=data)
     #every published message has a call back function
     message_future.add_done_callback(callback)
     
    #  time.sleep(30)
     


if __name__ == '__main__':
  #declaring command line arguments for the program 
  parser = argparse.ArgumentParser(description='Publish messages to pubsub')

  
  parser.add_argument('--topic_path', required=True,
                        help='Name of the GCP topic to publish to')

  parser.add_argument('--input_file', required=False,
                        help='Local path of the file that has the sample message')

  parser.add_argument('--num_messages', required=True,
                        help='Number of messages to publish')


  args = parser.parse_args()
  topic_path=args.topic_path
  input_file=args.input_file
  num_messages=args.num_messages
  publish_function(topic_path,input_file,num_messages)