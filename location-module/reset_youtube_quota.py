import redis
from config import redis_host, redis_port, redis_password


if __name__ == "__main__":
    cache = redis.Redis(host=redis_host, port=redis_port, password=redis_password)
    cache.spop("youtube_quota", 1)