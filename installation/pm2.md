## PM2 configuration

Most of the system consists of small functions scheduled using PM2 (https://pm2.keymetrics.io/):
```
sudo apt install npm
sudo npm install pm2@latest -g
```

### Commands to deploy modules

* For each module, replace $HOME with the path to the module's location.

#### Streams tracker module
- ````pm2 start $HOME/streams-tracker/scripts/stream-gatherer.sh --name streams-tracker/gatherer --cron="*/30 * * * *" --no-autorestart````
- ````pm2 start $HOME/streams-tracker/track_current.py --interpreter $HOME/streams-tracker/venv/bin/python3 --name streams-tracker/track_current --restart-delay=60000````
- ````pm2 start $HOME/streams-tracker/compress_stream_data.py --interpreter $HOME/streams-tracker/venv/bin/python3 --name streams-tracker/compress-streams --restart-delay=600000````


#### Download module
- ````pm2 start $HOME/images-downloader/twitch_api_process.py --interpreter $HOME/images-downloader/venv/bin/python3 --name images-download/twitch_api  --cron-restart="0 */2 * * *"````
- ````pm2 start $HOME/images-downloader/coordinator.py --interpreter $HOME/images-downloader/venv/bin/python3 --name images-download/coordinator````
- ````pm2 start $HOME/images-downloader/scripts/reset-queried.sh --name images-download/reset-queried --restart-delay=7200000````


#### Location module
- ````pm2 start $HOME/locate-users-online/filter_users.py --interpreter $HOME/locate-users-online/venv/bin/python3 --name locate-users/filter_users  --restart-delay=1800000````
- ````pm2 start $HOME/locate-users-online/search_steam.py --interpreter $HOME/locate-users-online/venv/bin/python3 --name locate-users/search_steam  --restart-delay=300000````
- ````pm2 start $HOME/locate-users-online/search_by_twitch_description.py --interpreter $HOME/locate-users-online/venv/bin/python3 --name locate-users/search_by_twitch_description --restart-delay=300000````
- ````pm2 start $HOME/locate-users-online/search_by_name_youtube.py --interpreter $HOME/locate-users-online/venv/bin/python3 --name locate-users/search_youtube --restart-delay=900000````
- ````pm2 start $HOME/locate-users-online/search_location.py --interpreter $HOME/locate-users-online/venv/bin/python3 --name locate-users/search_location  --restart-delay=300000````
- ````pm2 start $HOME/locate-users-online/post_process_users.py --interpreter $HOME/locate-users-online/venv/bin/python3 --name locate-users/post_process_users --restart-delay=300000````
- ````pm2 start $HOME/locate-users-online/process_stream_tags.py --interpreter $HOME/locate-users-online/venv/bin/python3 --name locate-users/process_stream_tags --restart-delay=900000````
- ````pm2 start $HOME/locate-users-online/complement_with_twitch_tags.py --interpreter $HOME/locate-users-online/venv/bin/python3 --name locate-users/complement_with_twitch_tags --restart-delay=7200000````
- ````pm2 start $HOME/locate-users-online/location_from_youtube.py --interpreter $HOME/locate-users-online/venv/bin/python3 --name locate-users/location_from_youtube --restart-delay=21600000````
- ````pm2 start $HOME/locate-users-online/youtube_twitch_connection.py --interpreter $HOME/locate-users-online/venv/bin/python3 --name locate-users/youtube_twitch_connection --restart-delay=7200000````
- ````0 9 * * * $HOME/locate-users-online/venv/bin/python3 $HOME/locate-users-online/reset_youtube_quota.py````


#### Image processing module

- ````pm2 start $HOME/image-processing-module/pre-process-images/pre_process_images.py --interpreter $HOME/image-processing-module/pre-process-images/venv/bin/python3 --name image-process/pre_process_images --restart-delay=500````
    * It is recommended to create several parallel instances of this process. To avoid having data mix, the process can optionally receive an index:
    ````pm2 start $HOME/image-processing-module/pre-process-images/pre_process_images.py --interpreter $HOME/image-processing-module/pre-process-images/venv/bin/python3 --name image-process/pre_process_images-idx --restart-delay=500 -- idx````

- ````pm2 start $HOME/image-processing-module/process-images/run_processing.py --interpreter $HOME/image-processing-module/process-images/venv/bin/python3 --name image-process/run_processing --restart-delay=500````
- ````pm2 start $HOME/image-processing-module/process-images/match_pytesseract.py --interpreter $HOME/image-processing-module/process-images/venv/bin/python3 --name image-process/pytesseract --restart-delay=500````

- __Docker images__:

    1. ````pm2 start $HOME/image-processing-module/process-images/run-paddleocr/run_paddleocr.sh --name image-process/paddleocr-gpu0 --restart-delay=500 -- 0 $PATH````
    2. ````pm2 start $HOME/image-processing-module/process-images/run-easyocr/run_easyocr.sh --name image-process/easyocr-gpu1 --restart-delay=500 -- 1 $PATH````

    * It is recommened to create several parallel instances of both previous processes, at least one per available GPU. The processes receive two parameters: index of the GPU where the process will run and the path to the local storage where the images to process are located.

- ````pm2 start $HOME/image-processing-module/process-images/add_missing.py --interpreter $HOME/image-processing-module/process-images/venv/bin/python3 --name image-process/clean-missing --restart-delay=21600000````

- ````pm2 start $HOME/image-processing-module/process-raw-latency/process_raw_latency.py --interpreter $HOME/image-processing-module/process-raw-latency/venv/bin/python3 --name process-raw/process_raw_latency --restart-delay=1000````

- __Tiny image processing__:

    - ````pm2 start $HOME/image-processing-module/process-images-tiny/run_tiny_image_process.py --interpreter $HOME/image-processing-module/process-images-tiny/venv/bin/python3 --name tiny-images/run_process --restart-delay=1000````
    - ````pm2 start $HOME/image-processing-module/process-images-tiny/match_pytesseract.py --interpreter $HOME/image-processing-module/process-images/venv/bin/python3 --name tiny-images/tiny_tesseract-0  --restart-delay=1000````
    - ````pm2 start $HOME/image-processing-module/process-images-tiny/run-paddleocr/run_paddleocr.sh --name tiny-images/tiny_paddleocr-gpu0-0 --restart-delay=1000 -- 0````
    - ````pm2 start $HOME/image-processing-module/process-images-tiny/run-easyocr/run_easyocr.sh --name tiny-images/tiny_easyocr-gpu1-0  --restart-delay=1000 -- 1````
    - ````pm2 start $HOME/image-processing-module/process-raw-latency/compile_tiny_results.py --interpreter $HOME/image-processing-module/process-raw-latency/venv/bin/python3 --name tiny-images/compile_results --restart-delay=2000````


#### Data processing module

- ````pm2 start $HOME/data-analysis-module/find_spikes_glitches.py --interpreter $HOME/data-analysis-module/venv/bin/python3 --name data-process/spikes_glitches --restart-delay=60000````
- ````pm2 start $HOME/data-analysis-module/group_locations.py --interpreter $HOME/data-analysis-module/venv/bin/python3 --name data-process/group_locations  --restart-delay=1800000````
- ````pm2 start $HOME/data-analysis-module/location_changes_analysis_online.py --interpreter $HOME/data-analysis-module/venv/bin/python3 --name data-process/location-changes --restart-delay=30000````
- ````pm2 start $HOME/data-analysis-module/get_latency_for_distributions_online.py --interpreter $HOME/data-analysis-module/venv/bin/python3 --name data-process/distribution --restart-delay=60000````
- ````pm2 start $HOME/data-analysis-module/parameter_estimator.py --interpreter $HOME/data-analysis-module/venv/bin/python3 --name data-process/parameter_estimator --restart-delay=1800000````
- ````pm2 start $HOME/data-analysis-module/online_spike_detection.py --interpreter $HOME/data-analysis-module/venv/bin/python3 --name data-process/online --restart-delay=1000````


#### Data backup
- ````pm2 start $HOME/data-backup/backup_mongo.py --interpreter $HOME/data-backup/venv/bin/python3 --name backup/backup_mongo  --restart-delay=86400000````
- ````pm2 start $HOME/data-backup/backup_redis.py --interpreter $HOME/data-backup/venv/bin/python3 --name backup/backup_redis --restart-delay=86400000````
                