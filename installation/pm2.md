$HOME=""

## Stream tracker module
pm2 start $HOME/streams-tracker/scripts/stream-gatherer.sh --name streams-tracker/gatherer --cron="*/30 * * * *" --no-autorestart
pm2 start $HOME/streams-tracker/track_current.py --interpreter $HOME/streams-tracker/venv/bin/python3 --name streams-tracker/track_current --restart-delay=60000
pm2 start $HOME/streams-tracker/compress_stream_data.py --interpreter $HOME/streams-tracker/venv/bin/python3 --name streams-tracker/compress-streams --restart-delay=600000

## Download module
pm2 start $HOME/images-downloader/twitch_api_process.py --interpreter $HOME/images-downloader/venv/bin/python3 --name images-download/twitch_api  --cron-restart="0 */2 * * *"
pm2 start $HOME/images-downloader/coordinator.py --interpreter $HOME/images-downloader/venv/bin/python3 --name images-download/coordinator
pm2 start $HOME/images-downloader/scripts/reset-queried.sh --name images-download/reset-queried --restart-delay=7200000

## Location module
pm2 start $HOME/locate-users-online/filter_users.py --interpreter $HOME/locate-users-online/venv/bin/python3 --name locate-users/filter_users  --restart-delay=1800000

pm2 start $HOME/locate-users-online/search_steam.py --interpreter $HOME/locate-users-online/venv/bin/python3 --name locate-users/search_steam  --restart-delay=300000
pm2 start $HOME/locate-users-online/search_by_twitch_description.py --interpreter $HOME/locate-users-online/venv/bin/python3 --name locate-users/search_by_twitch_description --restart-delay=300000
pm2 start $HOME/locate-users-online/search_by_name_youtube.py --interpreter $HOME/locate-users-online/venv/bin/python3 --name locate-users/search_youtube --restart-delay=900000

pm2 start $HOME/locate-users-online/search_location.py --interpreter $HOME/locate-users-online/venv/bin/python3 --name locate-users/search_location  --restart-delay=300000

pm2 start $HOME/locate-users-online/post_process_users.py --interpreter $HOME/locate-users-online/venv/bin/python3 --name locate-users/post_process_users --restart-delay=300000

pm2 start $HOME/locate-users-online/process_stream_tags.py --interpreter $HOME/locate-users-online/venv/bin/python3 --name locate-users/process_stream_tags --restart-delay=900000
pm2 start $HOME/locate-users-online/complement_with_twitch_tags.py --interpreter $HOME/locate-users-online/venv/bin/python3 --name locate-users/complement_with_twitch_tags --restart-delay=7200000

pm2 start $HOME/locate-users-online/location_from_youtube.py --interpreter $HOME/locate-users-online/venv/bin/python3 --name locate-users/location_from_youtube --restart-delay=21600000
pm2 start $HOME/locate-users-online/youtube_twitch_connection.py --interpreter $HOME/locate-users-online/venv/bin/python3 --name locate-users/youtube_twitch_connection --restart-delay=7200000
0 9 * * * $HOME/locate-users-online/venv/bin/python3 $HOME/locate-users-online/reset_youtube_quota.py

## Image processing module
pm2 start /home/calvarez/pre-process-images/pre_process_images.py --interpreter /home/calvarez/pre-process-images/venv/bin/python3 --name image-process/pre_process_images-0 --restart-delay=500
pm2 start /home/calvarez/pre-process-images/pre_process_images.py --interpreter /home/calvarez/pre-process-images/venv/bin/python3 --name image-process/pre_process_images-1 --restart-delay=500 -- 1
pm2 start /home/calvarez/pre-process-images/pre_process_images.py --interpreter /home/calvarez/pre-process-images/venv/bin/python3 --name image-process/pre_process_images-2 --restart-delay=500 -- 2

pm2 start /home/calvarez/process-images/run_processing.py --interpreter /home/calvarez/process-images/venv/bin/python3 --name image-process/run_processing --restart-delay=500

pm2 start /home/calvarez/process-images/match_pytesseract.py --interpreter /home/calvarez/process-images/venv/bin/python3 --name image-process/pytesseract-0 --restart-delay=500
pm2 start /home/calvarez/process-images/match_pytesseract.py --interpreter /home/calvarez/process-images/venv/bin/python3 --name image-process/pytesseract-1 --restart-delay=500

pm2 start /home/calvarez/process-images/run-easyocr/run_easyocr.sh --name image-process/easyocr-gpu1-0 --restart-delay=500 -- 1
pm2 start /home/calvarez/process-images/run-easyocr/run_easyocr.sh --name image-process/easyocr-gpu0-0 --restart-delay=500 -- 0 
pm2 start /home/calvarez/process-images/run-easyocr/run_easyocr.sh --name image-process/easyocr-gpu1-1 --restart-delay=500 -- 1
pm2 start /home/calvarez/process-images/run-easyocr/run_easyocr.sh --name image-process/easyocr-gpu0-1 --restart-delay=500 -- 0 

pm2 start /home/calvarez/process-images/run-paddleocr/run_paddleocr.sh --name image-process/paddleocr-gpu0-0 --restart-delay=500 -- 0
pm2 start /home/calvarez/process-images/run-paddleocr/run_paddleocr.sh --name image-process/paddleocr-gpu0-1 --restart-delay=500 -- 0
pm2 start /home/calvarez/process-images/run-paddleocr/run_paddleocr.sh --name image-process/paddleocr-gpu1-0 --restart-delay=500 -- 1

pm2 start /home/calvarez/process-images/add_missing.py --interpreter /home/calvarez/process-images/venv/bin/python3 --name image-process/clean-missing --restart-delay=21600000

pm2 start /home/calvarez/process-raw-latency/process_raw_latency.py --interpreter /home/calvarez/process-raw-latency/venv/bin/python3 --name process-raw/process_raw_latency --restart-delay=1000


## Data processing module