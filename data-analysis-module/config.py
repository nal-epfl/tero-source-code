from datetime import datetime
min_cluster_coverage = 10

redis_host = "127.0.0.1"
redis_password = ""
redis_port = 0

mongo_host = "127.0.0.1"
mongo_port = 0
mongo_user = ""
mongo_password = ""

base_path = ""

stable_period_min_length = {
    "295590": 30, 
    "135305": 30, 
    "118849": 30, 
    "116088": 30, 
    "319965": 30, 

    "273486": 30, 
    "273195": 30, 
    "747108": 30, 
    "267128": 30, 
    "464426": 30, 
}

qoe_bands = {
    "295590": 15, 
    "135305": 15, 
    "118849": 15, 
    "116088": 15, 
    "319965": 15, 

    "273486": 15, 
    "273195": 15, 
    "747108": 15, 
    "267128": 15, 
    "464426": 15, 
}

stable_share = .15
grouping_window_size = 12

reference_date = datetime(year=2021, month=5, day=1)

min_cluster_coverage = 10
max_spike_proportion = .5