import pika, json
from requests_protocol import *
import os
conn = pika.BlockingConnection(
    pika.ConnectionParameters('localhost'))
channel = conn.channel()

def Terminate():
    global conn, channel
    channel.close()
    conn.close()
    print("Client was terminated")

def GetListCallback(ch, method, properties, body):
    response_dict = json.loads(body.decode())
    for num in response_dict:
        print(f"{num} : {response_dict[num]}")
    channel.stop_consuming()
    
def ValidateGetRoadsInput(params):
    if len(params) < 1:
        return False
    for num in params:
        try:
            tmp = int(num)
        except:
            return False
    return True

def HandleGetRoadsReply(ch, method, properties, body):
    global channel
    if method.routing_key == "err":
        print(f"No such road: {body.decode()}")
        channel.stop_consuming()
        return
    response = json.loads(body)
    for road_num in response:
        print("Number of road: ", road_num)
        for coordinate in response[road_num]:
            print(*coordinate)
        print("_"*25)
    channel.stop_consuming()

queue = channel.queue_declare(queue='', exclusive=True)
queue_name = queue.method.queue

channel.queue_bind(queue=queue_name, exchange=client_exchange, routing_key= manager_get_roads_reply)
channel.queue_bind(queue=queue_name, exchange=client_exchange, routing_key= "list")
channel.queue_bind(queue=queue_name, exchange=client_exchange, routing_key= "err")
try:
    while (True):
        print("Enter command:\n")
        user_input = input()
        parsed_input = user_input.split()
        if len(parsed_input) == 0:
            continue
        command = parsed_input[0]
        if (command == "list"):
            channel.basic_publish(exchange=manager_exchange,
                                  body="{\"request\": \"list\"}",
                                  routing_key="manager_request")
            channel.basic_consume(queue=queue_name,
                                on_message_callback=GetListCallback,
                                auto_ack=False)
            print("Waiting for roads list...")
            channel.start_consuming()
        elif (command == "get"):
            if (ValidateGetRoadsInput(parsed_input[1:])):
                road_numbers = parsed_input[1:]
                for road_num in road_numbers:
                    # road_nums = " ".join(parsed_input[1:])
                    request = dict({"request": "get_roads", "roads": road_num})
                    request = json.dumps(request)
                    channel.basic_publish(exchange=manager_exchange,
                                            routing_key="manager_request",
                                            body=request)
                    channel.basic_consume(queue=queue_name,
                                        on_message_callback=HandleGetRoadsReply)
                    channel.start_consuming()
            else:
                print("Not enought params!")
        elif command == "exit":
            Terminate()
            break
        else:
            print("Not available method. Usage:\n\tget {road number}\n\tlist")
except KeyboardInterrupt:
    Terminate()
                