# Steaming Data
# streaming-05-smart-smoker

## Tim Gormly
### 05/27/2024

In this module, we will use multiple producers, consumers, and RabbitMQ queues to appropriately handle live data being streamed from a smart smoker.

<hr>

## Emitter of Tasks

This script will read one line from a csv file every 30 seconds.  

Depending on the contents of the different columns in the file, a message will me routed to each appropriate queue, per row.