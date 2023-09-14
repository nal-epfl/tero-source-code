## Stream tracker module

### Scripts:
1. __stream_gatherer__: Periodically fetch all streams from Twitch, store information and separate users to be used by the location module. Default: once every 30 minutes. 
2. __track_current__: Higher frequency fetching exclusive for streams currently tracked by the download module.
3. __compress_stream_data__: Once a stream finishes, compress the high frequency data in one line, summarizing game changes.
4. __process_all_streams__: Compress all stream data downloaded by __stream_gatherer__ in the same one-line format.


### Configuration parameters and secrets:
1. __path_to_storage__: Path to stream data storage.


### Redis configuration:
1. __active_streams set__: All streams currently online.
2. __current_probes hashmap__: All streams currently tracked (i.e images being downloaded) by the download module.
3. __finished_streams set__: Streams recently offline.
4. __stream_files set__: New stream files from __stream_gatherer__ to be processed by __process_stream_tags__ (location module). 
5. __new_users set__: New users to locate.
6. __new_stream_files set__: New stream files from __stream_gatherer__ to be processed by __process_all_streams__.


### MongoDB configuration:
1. __streams database__:
    * __metadata__: Stores basic stream data (stream, user, stream start) for all streams.
    * __game_names__: Maps ids to game name to allow for a more compact metadata/summaries representation.
    * __data__: Data obtained by __track_current__, deleted once the stream is finished and summarized.
    * __summaries__: One-line summary of high frequency stream data.
