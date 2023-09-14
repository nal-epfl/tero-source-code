## Location module

### Requirements:
1. __CLIFF__:
    * Docker image: https://hub.docker.com/r/rahulbot/cliff-clavin
    * Tested version: rahulbot/cliff-clavin:2.6.1

2. __Xponents__:
    * Docker image: https://hub.docker.com/r/mubaldino/opensextant
    * Tested version: xponents:v3

3. __Mordecai__:
    * Python requirements: `python -m spacy download en_core_web_lg`
    * Installation instructions: https://github.com/openeventdata/mordecai
        ``` 
        docker pull elasticsearch:5.5.2
        wget https://andrewhalterman.com/files/geonames_index.tar.gz --output-file=wget_log.txt
        tar -xzf geonames_index.tar.gz
        docker run -d -p 127.0.0.1:9200:9200 -v $(pwd)/geonames_index/:/usr/share/elasticsearch/data elasticsearch:5.5.2
        ```

4. __Steam location parsing__:
    * Install Ruby.
    * Install Steam Friends utility: https://github.com/Holek/steam-friends-countries

5. __NLTK__:
    * Before running any script, run `python base_scripts/prepare_nltk.py`


### Scripts
1. __filter_users__: Queue users to process by the different searching scripts. Store/update users for churn out stats.
2. **Searching scripts:**:
    * __search_by_twitch_description__: Find locations in Twitch descriptions: use the 3 services, combine results and store accepted/rejected locations. Find potential youtube links in descriptions.
    * __search_by_name_youtube__: Find locations by finding a Youtube account with the same name as a Twitch account.
    * __search_steam__: Find locations by finding a Steam account with the same name as a Twitch account.
3. **Twitch descriptions processing**:
    * __cliff_locator__: Find locations in descriptions using CLIFF: geoparse, use heuristic, and return locations that fulfill / do not fulfill the heuristic.
    * __xponents_locator__: Find locations in descriptions using Xponents: geoparse, use heuristic, and return locations that fulfill / do not fulfill the heuristic.
    * __mordecai_locator__: Find locations in descriptions using Mordecai: geoparse, use heuristic, and return locations that fulfill / do not fulfill the heuristic.
4. **Twitch tags processing**:
    * __process_stream_tags__: Compiles tags from stored streams files.
    * __complement_with_twitch_tags__: Compares locations rejected by __search_by_twitch_description__ with stored tags.
5. **Youtube connections**:
    * __youtube_twitch_connection__: Takes potential youtube links detected by __search_by_twitch_description__ and searches account information if available.
    * __location_from_youtube__: Takes connections found by __youtube_twitch_connection__ and extracts location information if available.
    * __reset_youtube_quota__: Resets youtube quota counter.
6. __search_location__: Converts locations to a standarized format using Nominatim+Geonames.
7. __post_process_users__: Inserts newly found locations in the database while: (1) updating user locations if relocation is detected, (2) preventing location conflicts (same user has two different locations that can not be sorted over time).


### Configuration parameters and secrets:
1. __sleep_on_failure__ / __sleep_on_success__: Pacing parameters for Nominatim and Geonames.
2. __steam_api_key__: Secret key for the Steam API (Tutorial: https://cran.r-project.org/web/packages/CSGo/vignettes/auth.html).
3. __cliff_host__ / __cliff_port__: IP/Port of the CLIFF service.
4. __xponents_host__: IP of the Xponents service.
5. __youtube_api_key__ / __daily_quota__ / __youtube_safety_factor__: Youtube API key / daily quota / fraction of the rate limit to use before backing off.
6. __nominatim_user_agent__: Required user agent identifying the project (see: https://operations.osmfoundation.org/policies/nominatim/)
7. __geonames_username__: User name for Geonames (registration: https://www.geonames.org/login, docs: https://www.geonames.org/export/web-services.html)


### Redis configuration:
1. __Youtube__:
    * __youtube set__: Users to process.
    * __old_youtube set__: Stores users already processed.
    * __youtube_quota set__: Currently used quota (resets every 24 hours).
    * __twitch_youtube_suspects set__: Users with potential Twitch-Youtube connections.
    * __twitch_youtube_checked set__: Stores all potential connections already processed, regardless of the results.
    * __found_youtube set__: Stores all successful connections.
    * __youtube_locations hashmap__: Stores the ids of all users with a Youtube location.
2. __Steam__:
    * __steam set__: Users to process.
    * __old_steam set__: Stores users already processed.
    * __processed_steam set__: Counts the number of users processed for logging.
3. __Twitch__:
    * __twitch set__: Users to process.
    * __old_twitch set__: Stores users already processed.
    * __found_twitch set__: Stores the ids of all users with a Youtube location.
    * __locations_lost hashmap__: Stores all NLP-based locations rejected by the procedure.
4. __Twitch tags__:
    * __stream_files set__: List of stream files to extract tags from.
    * __tags_recovered__: Locations recovered after using the tags.
    * __lost_post_tags__: Locations rejected after using the tags.
5. __to_locate set__: List of potential locations found by the previous processes.
6. __parsed_users set__: Locations parsed and ready to insert in the database.


### MongoDB configuration:
1. __user_stats database__:
    * __steam__: Steam location extraction statistics.
    * __descriptions__: Twitch descriptions extraction statistics.
    * __youtube__: Youtube location extraction statistics.
    * __located_users__: Located user churn out statistics.
    * __all_users__: User churn out statistics.
    
2. __location database__:
    * __cache__: Nominatim+Geonames cache to decrease redundant API calls.
    * __not_parsed__: User locations that could not be parsed by Nominatim+Geonames.
    * __conflicts__: Contains users with conflicting locations.
    * __users__: User locations.
    * __tags__: Twitch tag information per user.