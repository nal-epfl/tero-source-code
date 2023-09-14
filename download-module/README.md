## Download module

### Scripts:
1. __coordinator__: State keeper with several responsibilities: (1) Send ids to run Twitch API queries; (2) Send streams to track to downloaders; (3) Keep user tracking historical stats.
2. __downloader__: Periodically downloads thumbnails from a set of streams.
3. __twitch_api_process__: Runs queries to the Twitch API.


### Configuration parameters and secrets:
1. __games_to_probe__: List of Twitch-generated game IDs to download (i.e. with network data onscreen).
2. __width__ / __height__: Twitch thumbnails default size.
3. __offline_expire__ / __finished_expire__: Time to wait before trying to query for a user that was found to be offline/after a user's stream finishes.
4. __users_batch_size__: Number of users to query at once before reporting back to the coordinator.
5. __default_to_sleep__: Time to wait between batches of Twitch API calls.
6. __max_queue_size__ / __min_queue_size__: Min/Max number of streams that a single downloader can be assigned.
7. __downloader_fetch_count__: Number of users a downloader can fetch from the queue at a time.


### Data files:
1. __data/games.json__: List of games to track. Format:
```
    "Twitch-generated game ID": {
        "name": "Game name / long identifier",
        "id": "Tero-generated game ID"
    },
```


### Redis configuration:
1. __Twitch API__:
    * __twitch_api_to_query set__: List of Twitch IDs to query.
    * __twitch_api_online__: Answer from __twitch_api_process__, list of Twitch streams currently online.

2. __Coordinator state keeping__:
    * __current_probes__: Stream data currently being processed by downloaders.  
    * __offline__ / __finished__: Users that have recently been found to be offline / recently gone offline.
    * __users_tracked_log__: Users tracking recent history.

3. __Communication with downloaders__:
    * __to_download__: List of streams from which to download thumbnails.
    * __from_workers__: Updates from downloaders: a streamer has gone offline.
    * __streams_idx__: Streams being processed by downloader __idx__.