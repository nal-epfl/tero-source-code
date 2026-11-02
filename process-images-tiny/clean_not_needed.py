import os
import sys

remote_path = "/mnt/nal/scratch/tiny-image-processing"

if __name__ == "__main__":
    if len(sys.argv) == 0:
        sys.exit(0)

    file_name = sys.argv[1]

    files_to_keep = set()
    with open("{}/to-process/{}.txt".format(remote_path, file_name), "r") as f:
        for l in f:
            line = l.strip()
            files_to_keep.add("{}/images-in-process/{}/{}".format(remote_path, file_name, line))


    for x in os.listdir("{}/images-in-process/{}".format(remote_path, file_name)):
        file = "{}/images-in-process/{}/{}".format(remote_path, file_name, x)

        if "extra" in x:
            os.remove(file)
            continue

        if file not in files_to_keep:
            os.remove(file)