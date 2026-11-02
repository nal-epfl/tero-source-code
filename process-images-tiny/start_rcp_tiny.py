import re
import subprocess
import redis
import paramiko

from scp import SCPClient

from subprocess import PIPE
from config import redis_host, redis_password
from config import jumphost, jumphost_user, jumphost_password, remote_path

from logger import get_logger



class RCPContainersController:
    def __init__(self):
        self.logger = get_logger("rcp_containers-tiny")
        self.cache = redis.Redis(host=redis_host, port=6379, password=redis_password)

    def run(self):
        result = subprocess.run(["runai", "list", "jobs", "--suppress-deprecation-message"], capture_output=True)
            
        lines = result.stdout.decode("utf8").split("\n")

        ongoing = set()
        for line in lines:
            if "Train" in line:
                ongoing.add(line.split()[0])

        for technique in ["easyocr-tiny", "paddleocr-tiny"]:
            # Check if one process is currently ongoing (keep cost under control)
            if technique in ongoing:
                self.logger.info("Process still ongoing: {}".format(technique))
                continue

            # Check if there is something to process at all       
            if self.cache.scard(technique.replace("-", "_")) > 0:
                # pop one value and "reserve" the processor -> how to release?
                subprocess.run(["runai", "submit", "--name", technique, "--image", "registry.rcp.epfl.ch/nal-tero-project/{}:v0.1".format(technique), "--gpu", "1", "--existing-pvc", "claimname=nal-scratch,path=/scratch",
                                "--auto-deletion-time-after-completion", "0"])



if __name__ == "__main__":
    controller = RCPContainersController()
    controller.run()
        