import os
import json

from collections import Counter

path = "/home/calvarez/Projects/Tero-Core/locate-users-online/stream-tags"



if __name__ =="__main__":
    compiled_results = {}
    
    for file_name in os.listdir(path):        
        with open(os.path.join(path, file_name)) as f:
            for l in f:
                data = json.loads(l.strip())
                
                if data["stream"]["user_id"] not in compiled_results:
                    compiled_results[data["stream"]["user_id"]] = {"locations": {}, "tags": Counter()}

                compiled_results[data["stream"]["user_id"]]["tags"].update(data["stream"]["tags"])

                for country in data["countries"]:
                    if country["country_code"] not in compiled_results[data["stream"]["user_id"]]["locations"]:
                        compiled_results[data["stream"]["user_id"]]["locations"][country["country_code"]] = Counter()

                    compiled_results[data["stream"]["user_id"]]["locations"][country["country_code"]].update([country["tag"]])
               

    with open("compiled-all.json", "w+") as f:
        for user, user_data in compiled_results.items():
            formatted_data = {"tags": user_data["tags"].most_common(), "locations": {}}
            
            for country, country_tags in user_data["locations"].items():
                formatted_data["locations"][country] = country_tags.most_common()

            f.write(json.dumps({"user_id": user, **formatted_data}) + "\n")
