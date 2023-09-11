import multiprocessing

from functools import partial
from datetime import datetime
from utils.logger import get_logger
from utils.utils import get_users_by_region, get_biggest_division, get_stored_locations
from db.mongo_controller import MongoController

number_cores = 15


class GameParameterEstimator:
    def __init__(self, game, users_by_id, users_by_location):
        self.db_controller = MongoController()
        self.game_id = game
        self.users_location_grouped = users_by_location
        self.users_location_by_id = users_by_id


    def get_users_in_area(self, country, state):
        return self.users_location_grouped.get(country, {}).get(state)


    def run(self, game_spikes):
        spikes_by_region = {}

        for user_id, anomaly in game_spikes.items():
            location = self.users_location_by_id.get(user_id, {})
            
            if location:
                division, _ = get_biggest_division(location)

                if not division:
                    continue

                if location["country_code"] not in spikes_by_region:
                    spikes_by_region[location["country_code"]] = {}
                
                if division not in spikes_by_region[location["country_code"]]:
                    spikes_by_region[location["country_code"]][division] = []
                
                spikes_by_region[location["country_code"]][division].extend(anomaly)

        for country, spikes_per_region in spikes_by_region.items():
            to_store = []

            for region, region_spikes in spikes_per_region.items():
                users_in_state = self.get_users_in_area(country, region)
                
                if not users_in_state:
                    continue

                t = 0

                for u in users_in_state:
                    user_latency = self.db_controller.count_latency_points(u, self.game_id)
                    
                    t += user_latency

                if t == 0:
                    continue

                p_e = 1.0 * len(region_spikes) / t
                significance_value = t * p_e * (1 - p_e)

                to_store.append({"game_id": self.game_id, "country": country, "region": region, "p_e": p_e, "significance": significance_value, "n_anomalies": len(region_spikes), "n_points": t})
                
            self.db_controller.store_parameters(to_store)


def process_game(users_by_id, users_by_location, game_data):
    estimator = GameParameterEstimator(game_data["game_id"], users_by_id, users_by_location)
    estimator.run(game_data["game_spikes"])
    
    return game_data["game_id"]



class ParametersEstimator:
    def __init__(self):
        self.db_controller = MongoController()
        self.logger = get_logger("parameter_estimation")
        self.users_location_by_id = get_stored_locations(self.db_controller.mongo_client)
                

    def get_user_spikes(self, user_id):
        spikes = {}
        for x in self.db_controller.get_spikes(user_id):
            spike = {**x, "date": datetime.fromtimestamp(x["date"])}
            if spike["game_id"] not in spikes:
                spikes[spike["game_id"]] = []
                        
            spikes[spike["game_id"]].append(spike)

        return spikes

        
    def run(self):
        self.logger.info('Starting process...')          
        users_with_spikes = self.db_controller.get_users_with_spikes()
        self.logger.info("Found {} users with spikes".format(len(users_with_spikes)))

        games = self.db_controller.get_all_games()

        spikes = {game: {} for game in games}
        total_spikes = {game: 0 for game in games}

        for user_id in users_with_spikes:
            user_spikes = self.get_user_spikes(user_id)

            for game, game_spikes in user_spikes.items():
                spikes[game][user_id] = game_spikes
                total_spikes[game] += len(game_spikes)

        self.logger.info('Grouped spikes per game. Total found: {}'.format(total_spikes))   
        
        users_location_grouped = get_users_by_region(self.users_location_by_id)

        pool = multiprocessing.Pool(number_cores)
        tmp = partial(process_game, self.users_location_by_id, users_location_grouped)
        game_data = [{"game_id": game_id, "game_spikes": game_spikes} for game_id, game_spikes in spikes.items()]

        results = pool.imap(func=tmp, iterable=game_data)

        for r in results:
            self.logger.info("Finished processing {}.".format(r))

        
            


if __name__ == "__main__":
    processor = ParametersEstimator()
    processor.run()