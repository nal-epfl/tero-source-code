from storage.storage_controller import StorageController
from config import path_to_storage


class LocalStorageController(StorageController):
    def __init__(self):
        super().__init__()     
        self.path_to_storage = path_to_storage
        
    def save_image(self, stream, game_id, thumbnail_date, image_data):        
        file_path = "{}/{}_{}_{}_{}.png".format(self.path_to_storage, game_id, thumbnail_date.strftime('%Y-%m-%d-%H-%M-%S'), stream.stream_id, stream.user_id)       
                    
        file = open(file_path, "wb")
        file.write(image_data)
        file.close()
        
        return file_path