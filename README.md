# Tero source code

### Configuration parameters common to all modules
1. __safety_factor__: Percentage of the rate limit to use before waiting for next renewal.
2. __twitch_api_id__/__twitch_client_secret__: Twitch API secrets.
3. __base_path__: Path where the logs should be stored.
4. __mongo_host__ / __mongo_port__ / __mongo_user__ / __mongo_password__: MongoDB credentials.
5. __redis_host__ / __redis_port__ / __redis_password__: Redis cache credentials.
6. __secret_key__: Key used to convert Twitch-based IDs into random non-reversible IDs through consistent hashing.


### Citation
Alvarez, Catalina, and Katerina Argyraki. "Using Gaming Footage as a source of Internet latency information." Proceedings of the 2023 ACM on Internet Measurement Conference. 2023.
