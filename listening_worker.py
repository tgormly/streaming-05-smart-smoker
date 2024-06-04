"""
Tim Gormly
6/2/2024

    This program actively and continuously listens for messages on three queues, simulating sensor readings from a smart Smoker.
    A message will be received as a struct.  When unpacked, the struct will let us know the queue and the temperature reading.

    Timestamps will be transmitted in the struct, but these will be ignored.  Instead, it will be assumed that the smoker is transmitting data at 30 second intervals.

    Python deques will be used to keep a window of streamed data.  Each time new data is received, it will be added to the relevant deque and then the deque will be reviewed for various issues.  Has the smoker temperature fallen?  Has the temperature stalled on one of the meat thermometers?  These questions will be answered everytime data is received.  If there is an issue, an alert will be generated.

    One consumer will receive messages from multiple queues and handle them with different callback functions.

"""

import pika
import sys
import time
import struct
from datetime import datetime
from collections import deque
from util_logger import setup_logger

logger, logname = setup_logger(__file__)

# create deques to hold windows of data for each sensor
# each reading is 30 seconds apart, so each deque has a maxlen equal to
# 2 * window in minutes.  A 5 length deque results in a 2.5 minute window
smoker_deque = deque(maxlen = 5) # 5/2 = 2.5 minute window
foodA_deque = deque(maxlen = 20) # 20/2 = 10 minute window
foodB_deque = deque(maxlen = 20) # 20/2 = 10 minute window

# define a function used to check the values at the beginning and end of an
def calculate_window_delta(collection):
    '''
    receives a collection and returns the difference between the last item and the first item
    '''
    return collection[-1] - collection[0]

# define a callback function for each sensor that will be called when a message is received
def smoker_callback(ch, method, properties, body):
    """ 
    callback function used when a message is received on the smoker queue
    """
    # unpack struct sent by emmiter_of_tasks.py
    timestamp, temperature = struct.unpack('!df', body) # timestamp will only be used for logging

    # convert timestamp back to a string for logging
    timestamp_str = datetime.fromtimestamp(timestamp).strftime("%m/%d/%y %H:%M:%S")
    logger.info(f' [Smoker Reading]: {timestamp_str}: {temperature}°F')

    # Add new temperature to deque
    smoker_deque.append(temperature)

    # Once the deque is full, check for a temperature drop
    if len(smoker_deque) == smoker_deque.maxlen:
        if calculate_window_delta(smoker_deque) <= -15:
            logger.info(f'''
******* [ALERT!] ********
*SMOKER TEMPERATURE ALERT!     
*TEMPERATURE HAS FALLEN BY 15°F
*{timestamp_str}
*Current Temp: {temperature}
*************************\n''')

    # when done with task, tell the user
    logger.info(" [Smoker Reading] Done.")
    # acknowledge the message was received and processed 
    # (now it can be deleted from the queue)
    ch.basic_ack(delivery_tag=method.delivery_tag)

def foodA_callback(ch, method, properties, body):
    """ 
    callback function used when a message is received on the foodA queue
    """
    # unpack struct sent by emmiter_of_tasks.py
    timestamp, temperature = struct.unpack('!df', body) # timestamp will only be used for logging

    # convert timestamp back to a string for logging
    timestamp_str = datetime.fromtimestamp(timestamp).strftime("%m/%d/%y %H:%M:%S")
    logger.info(f' [Food A Reading]: {timestamp_str}: {temperature}°F')

    # Add new temperature to deque
    foodA_deque.append(temperature)

    # Once the deque is full, check for a temperature drop
    if len(foodA_deque) == foodA_deque.maxlen:
        if calculate_window_delta(foodA_deque) <= 1:
            logger.info(f'''
******* [ALERT!] ********
*FOOD A ALERT!     
*TEMPERATURE HAS STALLED
*{timestamp_str}
*Current Temp: {temperature}
*************************\n''')

    # when done with task, tell the user
    logger.info(" [Food A Reading] Done.")
    # acknowledge the message was received and processed 
    # (now it can be deleted from the queue)
    ch.basic_ack(delivery_tag=method.delivery_tag)

def foodB_callback(ch, method, properties, body):
    """ 
    callback function used when a message is received on the foodB queue
    """
    # unpack struct sent by emmiter_of_tasks.py
    timestamp, temperature = struct.unpack('!df', body) # timestamp will only be used for logging

    # convert timestamp back to a string for logging
    timestamp_str = datetime.fromtimestamp(timestamp).strftime("%m/%d/%y %H:%M:%S")
    logger.info(f' [Food B Reading]: {timestamp_str}: {temperature}°F')

    # Add new temperature to deque
    foodB_deque.append(temperature)

    # Once the deque is full, check for a temperature drop
    if len(foodB_deque) == foodB_deque.maxlen:
        if calculate_window_delta(foodB_deque) <= 1:
            logger.info(f'''
******* [ALERT!] ********
*FOOD B ALERT!     
*TEMPERATURE HAS STALLED
*{timestamp_str}
*Current Temp: {temperature}
*************************\n''')

    # when done with task, tell the user
    logger.info(" [Food B Reading] Done.")
    # acknowledge the message was received and processed 
    # (now it can be deleted from the queue)
    ch.basic_ack(delivery_tag=method.delivery_tag)

# define a main function to run the program
def main(hn: str = "localhost"):
    """ Continuously listen for messages on several named queues."""

    # when a statement can go wrong, use a try-except block
    try:
        # try this code, if it works, keep going
        # create a blocking connection to the RabbitMQ server
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=hn))

    # except, if there's an error, do this
    except Exception as e:
        print()
        print("ERROR: connection to RabbitMQ server failed.")
        print(f"Verify the server is running on host={hn}.")
        print(f"The error says: {e}")
        print()
        sys.exit(1)

    try:
        # use the connection to create a communication channel
        channel = connection.channel()

        # Declare queues
        queues = ('01-smoker', '02-food-A', '03-food-B')

        # create durable queue for each queue
        for queue in queues:
            # delete queue if it exists
            channel.queue_delete(queue=queue)
            # create new durable queue
            channel.queue_declare(queue=queue, durable=True)

        # restrict worker to one unread message at a time
        channel.basic_qos(prefetch_count=1) 

        # listen to each queue and execute corresponding callback function
        channel.basic_consume( queue='01-smoker', on_message_callback=smoker_callback)
        channel.basic_consume( queue='02-food-A', on_message_callback=foodA_callback)
        channel.basic_consume( queue='03-food-B', on_message_callback=foodB_callback)

        # print a message to the console for the user
        print(" [*] Ready for work. To exit press CTRL+C")

        # start consuming messages via the communication channel
        channel.start_consuming()

    # except, in the event of an error OR user stops the process, do this
    except Exception as e:
        print()
        print("ERROR: something went wrong.")
        print(f"The error says: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print()
        print(" User interrupted continuous listening process.")
        sys.exit(0)
    finally:
        print("\nClosing connection. Goodbye.\n")
        connection.close()


# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":
    # call the main function with the information needed
    main("localhost")
