class DBController:
    def __init__(self, since_date):
        self.since_date = since_date

    def get_all_users(self):
        pass
    
    def get_user_location(self, user_id):
        return self.mongo_client.location.users.find({"user_id": str(user_id)}, projection={"_id": False})

    def get_all_latency(self, user_id):
        pass
    
    def get_latency_in_range(self, user_id, range_start, range_end):
        pass

    def store_latency(self, to_store):
        pass

    def store_glitch(self, to_store):
        pass

    def store_spikes(self, to_store):
        pass

    def store_qoe_sequences(self, to_store):
        pass

    def store_locations(self, to_store):
        pass

    def close(self):
        pass

    def get_pending_glitches(self):
        pass

    def replace_glitched_value(self, logger, glitch, alternatives, new_latency_value):
        pass

    def reverse_failed_replacements(self, logger):
        pass

    def get_affected_sequences(self, game_id, user_id, since_date):
        pass