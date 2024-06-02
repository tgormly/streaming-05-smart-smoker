# Steaming Data
# streaming-05-smart-smoker

## Tim Gormly
### 05/27/2024

In this module, we will use multiple producers, consumers, and RabbitMQ queues to appropriately handle live data being streamed from a smart smoker.

<strong>Emitter of Tasks</strong> will simulate receiving readings from the sensors on a smart smoker and producing messages from the readings that are sent to RabbitMQ.

### Requirements
A valid Python environment is required, as well as the pika library.  This repo was written in Python 3.11.9.

RabbitMQ services will need to be active.  Additional information on RabbitMQ can be found here: https://www.rabbitmq.com/tutorials

### Executing Code
First initialize RabbitMQ services

In one console, run <code>emitter_of_tasks.py</code>

<hr>

## Emitter of Tasks

This script will read one line from a csv file every 30 seconds.  

Depending on the contents of the different columns in the file, a message will me routed to each appropriate queue, per row.