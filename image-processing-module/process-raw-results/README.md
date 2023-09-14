## Image processing module: process raw results

### Scripts:
1. __process_raw_latency__: Takes the results from each OCR engine, extracts numeric information using game-specific heuristics, and, for each result, and compares the output of the engines to decide the final result.
2. __compile_tiny_results__: Schedule tiny image batches to be processed. Once all engines are done, compile and upload results and clean processed images.
3. __parsers/__: OCR engine specific processing.
4. __game_processors/__: Game-specific processing.


### Configuration parameters and secrets:
1. __emergency_backup_dir__: Path to storage in case S3 is not available.
2. __offline_storage_path__: Path to store the compiled results.
3. __tiny_results_path__ / __tiny_to_process_path__ / __tiny_img_storage__: Path to all tiny (i.e without pre-processing) images to process / store results from tiny images / processing metadata. 
4. __long_term_storage__: Path to long term storage for tiny results.
5. __stream_ends_storage__: Path to store files with stream ends: last time we were able to download a thumbnail from a given stream.


### Redis configuration:
1. __to_postprocess__: Input to the module: list of batches to process.
2. __to_confirm__: List of images that require to be processed by the __process-images-tiny__ submodule.
3. __new_latency__ / __logs_latency__: List of latency information inserted in the database after this module finishes processing. Used by: __data-analysis-module/shared_anomalies/online_spike_detection__ and __data-analysis-module/find_spikes_glitches__.
4. __zips_to_delete__: Zip files already processed but still stored.


### MongoDB configuration:
1. __data database__:
    * __latency__: Latency information compiled after comparing the output of the OCR engines.
    * __alternative_values__: Output of the third OCR engine if not all 3 agree.
2. __results database__:
    * __metadata__: Per-batch statistics.