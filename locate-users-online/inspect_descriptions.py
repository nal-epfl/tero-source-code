import json
import os

path = "/media/data/storage/twitch"

with open("with_youtube.json", "w+") as output:
    for file_name in os.listdir(path):    
        with open("{}/{}".format(path, file_name), "r") as f:
            for l in f:
                data = json.loads(l.strip())

                for user in data["data"]:
                    if user["description"] and "@" in user["description"].lower() and "youtube" in user["description"].lower():
                        output.write("{}\n".format(json.dumps({"twitch_id": user["id"], "description": user["description"]})))