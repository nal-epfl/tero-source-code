import os

storage_path = "/raid1/image-processing/zip-storage"

with open("check_if_zips-to_delete.json", "r") as f:
    for l in f:
        file = l.strip()
        file = file.split("/")[-1]

        if os.path.isfile(os.path.join(storage_path, file)):
            os.remove(os.path.join(storage_path, file))

