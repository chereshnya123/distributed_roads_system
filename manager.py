import json, pika
from requests_protocol import *
from logger import Logger
import os

log = Logger("system.log")
connection = pika.BlockingConnection(
    pika.ConnectionParameters('localhost'))
channel = connection.channel()

roads_number_to_name = dict() # number -> name
manager_info = dict() # worker -> какие дороги хранит
#   {"request" : "list/get_roads" (, "roads": [...nums...])}

def SendInitMessage(channel):
    channel.basic_publish(exchange="init_etl_exchange",
                          routing_key="manager_initialized",
                          body="")

def GetIdByRoadNum(road_num):
    for worker_id in manager_info:
        if int(road_num) in manager_info[worker_id]:
            return int(worker_id)
    raise WrongRoadException(road_num)

def HandleRequest(request): 
    log.Log("Manager: Handling message...")

    request = json.loads(request.decode())
    if request["request"] == "list":
        log.Log("Manager: Got 'list'-request")
        response = json.dumps(roads_number_to_name)
        channel.basic_publish(exchange="client_exchange", body=response, routing_key="list")
    elif request["request"] == "get_roads":
        log.Log("Manager: Got get_roads\n")
        roads_nums = sorted(list(request["roads"].split()))
        workers_requests = dict() #     {"1": [roads_for_1], ...}
        worker_id = 0
        for num in roads_nums:
            # Get number of worker keeping this road (by num)
            worker_id = GetIdByRoadNum(num)

            if str(worker_id) in workers_requests.keys():
                workers_requests[str(worker_id)].append(num)
            else:
                workers_requests[str(worker_id)] = [num]

        for worker_id in workers_requests:
            log.Log("Manager: Sent req to workers\n")
            channel.basic_publish(exchange=workers_exchange,
                                  routing_key=get_roads_by_number_for + str(worker_id),
                                  body=json.dumps(workers_requests[worker_id]))
    else:
        print(f"Manager: Unrecognized request: {request['request']}, try again")

        

def AppendWorkerResponse(response, body):
    for road in body:
        road_num = road["road_num"]
        coordinates = road["coordinates"]
        response[road_num] = coordinates
    return response
    

def HandleMessage(ch, method, properties, body):
    global manager_info, roads_number_to_name, channel
    response = dict()

    if method.routing_key == "manager_request":
        try:
            HandleRequest(body)
        except WrongRoadException as wrong_road:
            channel.basic_publish(exchange=client_exchange,
                                  routing_key="err",
                                  body=wrong_road.road)
    elif method.routing_key == worker_reply:
        response = AppendWorkerResponse(response, json.loads(body.decode()))
        channel.basic_publish(exchange=client_exchange,
                                routing_key=manager_get_roads_reply,
                                body=json.dumps(response))
    elif method.routing_key == "initial_message_info":
        manager_info = json.loads(body.decode())
    elif method.routing_key == "initial_message_roads_names":
        roads_number_to_name = json.loads(body.decode())
    else:
        response = "{'response': 'Unrecognized request'}"
        channel.basic_publish(exchange=client_exchange,
                              routing_key="err",
                              body=response)
        
queue = channel.queue_declare(queue='', exclusive=True)
queue_name = queue.method.queue

channel.queue_bind(queue=queue_name,
                   exchange=manager_exchange,
                   routing_key="manager_request")
channel.queue_bind(queue=queue_name,
                   exchange=manager_exchange,
                   routing_key="initial_message_roads_names")
channel.queue_bind(queue=queue_name,
                   exchange=manager_exchange,
                   routing_key="initial_message_info")
channel.queue_bind(queue=queue_name,
                   exchange=manager_exchange,
                   routing_key=worker_reply)
channel.basic_consume(queue=queue_name, on_message_callback=HandleMessage, auto_ack=False)
log.Log("Start consuming...\n")
# print("Start cons")
# SendInitMessage(channel)
try:
    channel.start_consuming()
except KeyboardInterrupt:
    print("Manager was terminated")
    log.Log("Manager: Was terminated")
    channel.close()
    connection.close()
    os._exit(0)
    
