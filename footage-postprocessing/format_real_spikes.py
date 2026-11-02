from pymongo import MongoClient
from datetime import timedelta, datetime
from config import mongo_host, mongo_user, mongo_password, stable_period_min_length, stable_share


class SpikesFormatting:
    def __init__(self):
        self.mongo_client = MongoClient("mongodb://{}:27017/".format(mongo_host),
                                        username=mongo_user,
                                        password=mongo_password)
    

    def group_spikes(self, spikes):
        # if two spikes are separated by less than 10 seconds then they are considered part of the same incident -> it is normal that some points are separated by "normal" latency points because of the drift between footage and thumbnails. We need to live with that.
        grouped_spikes = [[spikes[0]]]

        for idx in range(1, len(spikes)):
            if spikes[idx]["date"] - grouped_spikes[-1][-1]["date"] > timedelta(seconds=10):
                grouped_spikes.append([spikes[idx]])
            else:
                grouped_spikes[-1].append(spikes[idx])
        
        return grouped_spikes


    def summarize_groups(self, group):
        summaries = []
        
        for g in group:
            summaries.append({"game_id": g[0]["game_id"], "user_id": g[0]["user_id"], "stream_id": g[0]["stream_id"], "start": g[0]["date"].timestamp(), "end": g[-1]["date"].timestamp()})
        
        return summaries


    def run(self):
        for spikes_data in self.mongo_client.explore_users.spikes_analysis.find(projection={"_id": False}):
            if not spikes_data["real"]:
                continue

            real_spikes_sorted = sorted([{**x, "date": datetime.fromtimestamp(x["date"])} for x in spikes_data["real"]], key=lambda x: x["date"])

            grouped_spikes = self.group_spikes(real_spikes_sorted)

            to_insert = self.summarize_groups(grouped_spikes)
            if to_insert:
                self.mongo_client.explore_users.real_spikes.insert_many(to_insert)


    def clean_old(self):
        pass


if __name__ == "__main__":
    postprocess = SpikesFormatting()
    postprocess.clean_old()
    postprocess.run()