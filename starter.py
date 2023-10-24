import pika, subprocess, time
from requests_protocol import *

def DeclareExchanges():
    global channel
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
def WaitForSubprocs():
    time.sleep(1)
    
    
def EstimateWorkersAmount():
    roads = GetGeojsonFile()
    roads_amount = len(roads)
    number_of_workers = (roads_amount // kRoadPerWorker) + 1 if roads_amount % kRoadPerWorker else roads_amount // kRoadPerWorker 


if __name__ == "main":
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    DeclareExchanges()
    number_of_workers = 
    StartWorkersManager()
    WaitForSubprocs()
    
    
        