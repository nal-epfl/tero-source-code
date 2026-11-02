import os
from dotenv import load_dotenv

load_dotenv()

redis_host = os.getenv('REDIS_HOST')
redis_password = os.getenv('REDIS_PASSWORD')
redis_port = os.getenv('REDIS_PORT')

mongo_host = os.getenv('MONGO_HOST')
mongo_user = os.getenv('MONGO_USER')
mongo_password = os.getenv('MONGO_PASSWORD')

base_path = "thumbnail-postprocessing"

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

qoe_band = 20

stable_share = .15

games = ["295590","464426", "116088", "118849", "273195", "319965",  "135305", "273486", "747108", "267128", "314852", "461764", "742409", "762836", "128974", "970338", "614266", "452439","819896", "902129", "853669"]