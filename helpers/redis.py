import redis
from config import *
# Connect to Redis
def getRedisClient():
    redisClient = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
    return redisClient
