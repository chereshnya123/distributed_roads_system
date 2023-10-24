import pika, json, sys
from requests_protocol import *
from logger import Logger
import os

log = Logger("system.log")

def SendInitMessage():
    global channel
    channel.basic_publish(exchange="init_etl_exchange",
                          routing_key="worker_initialized",
                          body="")

def HandleGetRoadsRequest(body): # "worker_id" : [1, 2, 3, ...]
    global roads_database
    log.Log(f"Worker{worker_id}: Handling 'get_roads'-request...")
    
    roads = json.loads(body) # {"1" : [...], "2": [...], ...}
    response = []
    for road_num in roads:
        response.append(dict({"road_num":road_num,
                              "coordinates": roads_database[str(road_num)]}))
    
    channel.basic_publish(exchange=manager_exchange,
                          routing_key=worker_reply,
                          body = json.dumps(response))
    
def HandleDataReceive(body): # {"number" : [coords]}
    global roads_database, worker_id
    body = json.loads(body)
    roads_database=body

def HandleRequest(ch, method, properties, body):

    log.Log("Worker: Handling some request")
    if method.routing_key == get_roads_by_number_for + worker_id:
        HandleGetRoadsRequest(body)
    elif method.routing_key == data_for + worker_id:
        HandleDataReceive(body)
    else:
        raise NotImplementedError

worker_id = sys.argv[1]
roads_database = dict()
connection = pika.BlockingConnection(
    pika.ConnectionParameters('localhost'))
channel = connection.channel()

queue_result = channel.queue_declare(queue='', exclusive=True)
queue_name = queue_result.method.queue

channel.queue_bind(queue=queue_name,
                   exchange=workers_exchange,
                   routing_key=data_for + worker_id)
channel.queue_bind(queue=queue_name,
                   exchange=workers_exchange,
                   routing_key=get_roads_by_number_for + worker_id)

channel.basic_consume(queue=queue_name,
                      on_message_callback=HandleRequest)


log.Log("Worker: start consuming...")
SendInitMessage()
try:
    channel.start_consuming()
except KeyboardInterrupt:
    log.Log("Worker: Was terminated")
    channel.close()
    connection.close()
    os._exit(0)