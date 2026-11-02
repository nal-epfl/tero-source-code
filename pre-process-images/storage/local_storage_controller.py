import cv2
import os

from storage.storage_controller import StorageController
from config import batch_size


class LocalStorageController(StorageController):
    def __init__(self, idx, storage, game_id=None):
        super().__init__(idx)
        self.game_id = game_id
        if game_id:
            self.storage = "{}/{}".format(storage, game_id)
        else:
            self.storage = storage
           

    def get_images(self):
        if not self.game_id:
            already_processed = set()
            # with open("already_processed.txt", "r") as f:
            #     for l in f:
            #         already_processed.add(l.strip())

            thumbnails = []

            with open("already_processed.txt", "a+") as f:
                for x in os.listdir("{}".format(self.storage)):
                    abs_path = os.path.join(self.storage, x.strip())
                    
                    if abs_path not in already_processed:
                        thumbnails.append(abs_path)
                        f.write("{}\n".format(abs_path))
                    
                    if len(thumbnails) >= batch_size:
                        break
            
            return thumbnails


    def read_image(self, image, metadata):
        if not metadata:
            return None

        real_name = image
        if self.game_id:
            real_name = "{}/{}_{}_{}.png".format(self.storage, metadata["date"], metadata["stream_id"], metadata["user_id"])       

        try:            
            img = cv2.imread(real_name)
            return img
        except Exception as e:
            print(e)
            return None       


    def store_image(self, image, image_name, tag):
        # path_to_store = self.local_buffer.get(tag, None)

        # if path_to_store:
        path = "{}/{}".format("/media/ripe3/tiny", image_name)
        cv2.imwrite(path, image)


    def clean_up_images(self, images, logger):
        logger.info("Deleting images from local.")

        deleted_images = []
        
        # for image in images:
        #     try:
        #         if os.path.isfile(image):
        #             os.remove(image)

        #         self.already_processed_file.write(image + "\n")

        #         deleted_images.append(image)
        #     except Exception:
        #         pass

        # logger.info("Deleted {}/{} local images.".format(len(deleted_images), len(images)))

        return deleted_images
    
    
    def on_failure_zip(self, images, logger):    
        pass