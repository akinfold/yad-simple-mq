# Installation

Just...

1. clone this repo,
2. install and run locally MongoDB,
3. install pymongo
4. run yadsimplemq/example/workgenerator.py it will create a lot of work for worker.
5. and finally run yadsimplemq/example/workconsumer.py
6. play with Worker arguments in yadsimplemq/example/workconsumer.py

Also you may try to use pip, theoretically it might work:

    pip install git+https://github.com/akinfold/yad-simple-mq.git#egg=yad-simple-mq

# ToDo

* Make worker acknowledge finished tasks.
* Finish graceful worker termination mechanism.
* Add ability to get tasks results.

# Notes

## Pymongo

As it is just test project I'm add pymongo as requirement to make installation as simple as I can.

## AMQP

I know about AMQP, I even read some docs about it and use pika to interact with RabbitMQ in some past projects,
but implement AMQP for test project it's madness.
So I make BaseMessageQueue as simple as I can, but compatible with AMQP backend in some way.