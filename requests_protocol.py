class WrongRoadException(Exception):
    def __init__(self, road):
        self.road = road

workers_exchange = "workers_exchange"
get_roads_by_number_for = "GET /workers/roads/"
worker_reply = "worker_roads_reply"
data_for = "data_for_"

manager_exchange = "manager_exchange"
manager_get_roads_reply = "manager_get_roads_reply"

client_exchange = "client_exchange"

