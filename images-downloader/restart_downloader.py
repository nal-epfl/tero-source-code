import psutil
import subprocess

from logger import get_logger

logger = get_logger("restart_downloaders")
PROCNAME = "/downloader.py"

def kill_downloaders():
    for proc in psutil.process_iter():
        # check whether the process name matches
        try:
            complete_proc_name = " ".join(proc.cmdline())
            if not complete_proc_name:
                continue

            if PROCNAME in complete_proc_name:
                logger.info("Killing process: {}".format(complete_proc_name))
                proc.kill()
        
        except Exception:
            continue



# def start_downloaders():
#     result = subprocess.run(['/home/calvarez/images-downloader/scripts/create-workers.sh', '19'], stdout=subprocess.DEVNULL)
#     return result.stdout


if __name__ == "__main__":
    logger.info("Starting process...")
    kill_downloaders()
    logger.info("Finishing process...")    