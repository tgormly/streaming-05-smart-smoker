# Steaming Data
# streaming-05-smart-smoker

## Tim Gormly
### 05/27/2024

In this module, we will use multiple producers, consumers, and RabbitMQ queues to appropriately handle live data being streamed from a smart smoker.

<strong>Emitter of Tasks</strong> will simulate receiving readings from the sensors on a smart smoker and producing messages from the readings that are sent to RabbitMQ.

<strong>Listening Worker</strong> A single listening worker Will be used to receive messages from three separate RabbitMQ queues and handle them accordingly.

<hr>

### Requirements
A valid Python environment is required, as well as the pika library.  This repo was written in Python 3.11.9.

RabbitMQ services will need to be active.  Additional information on RabbitMQ can be found here: https://www.rabbitmq.com/tutorials

<hr>

### Executing Code
First initialize RabbitMQ services

In one console, run <code>emitter_of_tasks.py</code>

In another console, run <code>listening_worker.py</code>

<hr>

## Emitter of Tasks

This script will read one line from a csv file every 30 seconds.  

Depending on the contents of the different columns in the file, a message will me routed to each appropriate queue, per row.

Run the script in a console to begin sending messages.

![Image of console output while worker is sending messages](/Images/Producer_Console.png)

<em>Console output while worker is sending messages</em>

## Listening Worker

The script creates one worker that will listen to three separate RabbitMQ queues on one connection.

The data from each sensor will be added to a Python deque that keeps a window of the most recent X values.  Each time a new value is added, that sensor's deque is reviewed for any issues.

Run the script in a console to begin receiving messages.

![Image of console output while worker is receiving messages](/Images/Consumer_Console.png)

<em>Console output while worker is receiving messages.  Alerts for food stalling on both sensors can be seen.</em>

![Image of console output while worker is receiving messages](/Images/Smoker_Alert.png)

<em>Log file from worker session. Alert for drop in smoker temperature can be seen.</em>

![Side-by-Side of Producer and Consumer](/Images/Two_Consoles.png)

<em>Producer and Consumer working simultaneously.</em>

Tim Gormly