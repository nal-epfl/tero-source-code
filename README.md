# Tero source code

### Configuration parameters common to all modules
1. __twitch_api_id__/__twitch_client_secret__: Twitch API secrets.
2. __mongo_host__ / __mongo_port__ / __mongo_user__ / __mongo_password__: MongoDB credentials.
3. __redis_host__ / __redis_port__ / __redis_password__: Redis cache credentials.
4. __secret_key__: Key used to convert Twitch-based IDs into random non-reversible IDs through consistent hashing.
5. __client_id__: Key for footage access.
6. __rw_access_key__ / __rw_access_key__ / __bucket_name__: S3 credentials and bucket information.
7. __jumphost__ / __jumphost_user__ / __jumphost_password__: credentials used to move the data for GPU processing.
8. __remote_path__: path to store the data for GPU processing.


### Citation
Alvarez, Catalina, and Katerina Argyraki. "Using Gaming Footage as a source of Internet latency information." Proceedings of the 2023 ACM on Internet Measurement Conference. 2023.