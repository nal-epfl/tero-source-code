from utils.utils import get_users_locations
from db.mongo_controller import MongoController

from utils.logger import get_logger


class GroupLocations:
    def __init__(self) -> None:
        self.db_controller = MongoController()
        self.logger = get_logger("group_locations")


    def store_locations(self):
        self.logger.info("Getting all user locations...")
        users = self.db_controller.get_all_users()
        user_locations = get_users_locations(self.db_controller,  users)

        self.logger.info("Finished fetching user locations")

        self.db_controller.store_locations(user_locations)
        self.logger.info("Stored summarized user locations...")


if __name__ == "__main__":
    group_locations = GroupLocations()
    group_locations.store_locations()