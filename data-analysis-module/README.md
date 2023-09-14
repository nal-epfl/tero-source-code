## Data analysis module

### Scripts
1. __find_spikes_glitches__: Takes all recently inserted latency values, separates the series into subsequences,  removes glitches, and marks spikes.
2. __group_locations__: Consolidates all user locations, removing redundant entries ("Paris, France" and "France" becomes "Paris, France") and leaving one entry per user.
3. __location_changes_analysis__ / __location_changes_analysis_online__: Runs the location change pipeline: (1) group sequences inside a QoE band, (2) finds region-level clusters, (3) Finds potential location/server changes.
4. __get_latency_for_distributions__ / __get_latency_for_distributions_online__: Extracts all latency data to be used for distribution computation.
5. __shared_anomalies/__: Finds shared anomalies -- simultaneous spikes in the same geographical region that are unlikely to happen at the same time by chance.


### Redis configuration
1. __logs_latency__: Data recently outputted by the __process raw results__ submodule to process by this module (starting from __find_spikes_glitches__).
2. __to_publish__: New data to publish.
3. __new_to_group__: Recently inserted data that needs to be processed by __location_changes/online/group_results_online__.
4. __new_location_changes__: Recently inserted data that needs to be processed by __location_changes/online/location_changes_detection_online__.
5. __new_to_cluster__: Recently inserted data that needs to be processed by __location_changes/online/cluster_detection_online__.


### MongoDB configuration
1. __data database__:
    * __latency__: Latency information compiled after comparing the output of the OCR engines.
    * __alternative_values__: Output of the third OCR engine if not all 3 agree.
2. __processed database__:
    * __find_spikes_glitches__:
        * __latency__: Latency with glitches removed.
        * __glitches__: Glitches removed from the __latency__ collection
        * __spikes__: Points marked as spikes (they are still part of the __latency__ collection).
        * __qoe__: Per user and game, subsequences obtained after grouping consecutive points inside one QoE band distance.
        * __discarded_latency__: Stores all latency discarded due to several reasons: user does not have enough data, is too unstable, etc.
        * __discarded_spikes__: Stores all spikes discarded due to several reasons: user does not have enough data, is too unstable, the spike is >=1000ms etc.
        * __user_game_stats__: Summarizes user data per game for online anomaly detection.
        * __active_days__: Stores the list of days the user has streamed a certain game. Days are represented as indexes starting from 2021-05-01.
    * __group_locations__:
        * __locations__: Locations grouped.
    * __shared_anomalies/__:
        * __parameters__: Parameters per (region, game) for shared anomaly detection.
        * __shared_anomaly__: Basic information about shared anomalies detected.
        * __shared_anomaly_details__: Detailed information about shared anomalies detected, including all overlaps and anomalies found in the same time window.
    * __location_changes_analysis__:
        * __grouped_spikes__: Groups of consecutive spikes that are likely related to the same network problem.
        * __grouped_sequences__: Groups of sequences contained inside the same QoE band.
        * __clusters__: Regional clusters per game, including all users inside the cluster, the coverage (percentage of users inside out of all users in the region).
        * __changes_summary__: Location changes per user-game pair.
        * __users_without_changes__: List of all user-game pairs without a location change.
3. __distribution database__:
    * __latency__: Latency information used to compute distributions. Data from a user is only included if: (1) the user has no location changes, (2) the user has location changes, but a subsequence is contained inside a regional cluster that contains at least 10% of the users in their region.
4. __shared_anomaly database__:
    * __region_map__: For each game and country, it associates an index to each region with data.
5. __partitioned database__:
    * __latency-"game_id"-"country code"-"region index"__: Latency information from __game id__ belonging to users located in region __region index__. Conversion from __region index__ to region name can be found in the __region_map__ collection.
    * __spikes-"game_id"-"country code"-"region index"__: Spikes obtained from __game id__ belonging to users located in region __region index__. Conversion from __region index__ to region name can be found in the __region_map__ collection.