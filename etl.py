import pika, json, sys, subprocess
from requests_protocol import *
from multiprocessing import Process
import json
import time

processes = []
kRoadPerWorker = 10
def StartWorker(worker_number):
    print("Executing worker")
    exec(open('worker.py ' + worker_number).read())

def StartManager():
    print("Executing manager")
    exec(open('manager.py').read())
    
def DeclareExchanges(channel):
    channel.exchange_declare(exchange=client_exchange,
                         exchange_type='direct')
    channel.exchange_declare(exchange=manager_exchange,
                            exchange_type='direct')
    channel.exchange_declare(exchange=workers_exchange,
                            exchange_type='direct')
    channel.exchange_declare(exchange="init_etl_exchange",
                             exchange_type="direct")

def StartWorkersManager(number_of_workers):
    global processes
    for i in range(number_of_workers):
        processes.append(subprocess.Popen(
        f"python3 worker.py {i}".split(), stdin=None, stdout=None,
        stderr=None, close_fds=True
        ))
    processes.append(subprocess.Popen(
        "python3 manager.py".split(), stdin=None, stdout=None,
        stderr=None, close_fds=True
    ))

def StopProcesses():
    print("Wrapping up processes...")
    for p in processes:
        p.kill()        

def GetGeojsonFile():
    geojson_file = open('../roads/seoul.json')
    roads = json.load(geojson_file)["features"]
    return roads

def ManagerInitCallback(ch, method, properties, body):
    channel.stop_consuming()

def WorkerInitCallback(ch, method, properties, body):
    channel.stop_consuming()
    
def WaitForSubprocs():
    time.sleep(1)
    

def GetRoadsNumbersInfo(roads):
    result = dict()
    for i, road in enumerate(roads):
        try:
            result[i] = road["properties"]["name"]
        except KeyError:
            result[i] = road["properties"]["@id"]
    return result

conn = pika.BlockingConnection(
    pika.ConnectionParameters('localhost'))
channel = conn.channel()
roads = GetGeojsonFile()
roads_amount = len(roads)
number_of_workers = (roads_amount // kRoadPerWorker) + 1 if roads_amount % kRoadPerWorker else roads_amount // kRoadPerWorker 
DeclareExchanges(channel)
StartWorkersManager(number_of_workers)
WaitForSubprocs() # w8 till all the managers started
print("ETL: Initialized")
channel.basic_publish(exchange=manager_exchange,
                      routing_key="initial_message_roads_names",
                      body=json.dumps(roads))


manager_info = dict()

def GetEnumerateRoads(roads, left, right):
    result = dict()
    road_num = left
    for road in roads:
        result[road_num] = road["geometry"]["coordinates"]
        road_num += 1
    return result

#Distributing roads to workers
segment = roads_amount // number_of_workers
for i in range(number_of_workers):
    left = i*segment
    if i == number_of_workers - 1:
        right = len(roads)
    else:
        right = (i+1)*segment
        if right >= len(roads):
            right = len(roads)
            manager_info[i] = list(range(left, right))
            enumerate_roads_for_worker = GetEnumerateRoads(roads[left:right], left, right)
            channel.basic_publish(exchange=workers_exchange,
                          routing_key=data_for + str(i),
                          body=json.dumps(enumerate_roads_for_worker))
            break
        
    manager_info[i] = list(range(left, right))
    enumerate_roads_for_worker = GetEnumerateRoads(roads[left:right], left, right)
    # Sending roads to worker
    channel.basic_publish(exchange=workers_exchange,
                          routing_key=data_for + str(i),
                          body=json.dumps(enumerate_roads_for_worker))

roads_numbers_info = GetRoadsNumbersInfo(roads)

channel.basic_publish(exchange=manager_exchange,
                      routing_key="initial_message_info",
                      body=json.dumps(manager_info))
channel.basic_publish(exchange=manager_exchange,
                      routing_key="initial_message_roads_names",
                      body=json.dumps(roads_numbers_info))
try:
    print("Client started")
    import client
except:
    pass
    
StopProcesses()