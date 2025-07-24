const redis = require('redis');
const bluebird = require('bluebird');
const { config } = require('../config.js');

// Promisify BEFORE creating client
bluebird.promisifyAll(redis.RedisClient.prototype);
bluebird.promisifyAll(redis.Multi.prototype);

class Redis {
  constructor(redisUrl = null) {
    this.redisUrl = redisUrl || config.url;
    this.client = null;
  }

  async connect() {
    if (this.client) return;

    try {
      const { hostname, port } = new URL(
        this.redisUrl.startsWith('redis://') ? this.redisUrl : `redis://${this.redisUrl}`
      );

      this.client = redis.createClient({ host: hostname, port });

      this.client.on("connect", () => {
        console.log(`${this.redisUrl} Connected to Redis!`);
      });
      this.client.on("error", (err) => {
        console.error("Redis connection error: ", err);
      });
      this.client.on("ready", () => {
        console.log("Redis client is ready!");
      });

    } catch (error) {
      console.error('Failed to create Redis client:', error);
      throw error;
    }
  }

  async get(key) {
    await this.connect();
    return await this.client.getAsync(key);
  }

  async hGet(key, fields = null) {
    await this.connect();

    if (!key) return null;

    console.log({key, fields} ," Redis Get keys")
    if (fields && Array.isArray(fields)) {
      const data = await this.client.hmgetAsync(key, fields);
      return data.reduce((acc, value, index) => {
        acc[fields[index]] = value;
        return acc;
      }, {});
    } else if (fields && typeof fields === 'string') {
      const value = await this.client.hgetAsync(key, fields);
      return { [fields]: value };
    } else {
      console.log(" Redis Get Before Call")

      const data = await this.client.hgetallAsync(key);
      console.log(" Redis Get After Call")

      if (!data || Object.keys(data).length === 0) return null;
      return data;
    }
  }

  async set(key, value, expirationInSec = null) {
    await this.connect();
    if (expirationInSec) {
      await this.client.setAsync(key, value, 'EX', expirationInSec);
    } else {
      await this.client.setAsync(key, value);
    }
  }

  async hSet(key, data, expirationInSec = null) {
    await this.connect();
    await this.client.hmsetAsync(key, data);
    if (expirationInSec) {
      await this.client.expireAsync(key, expirationInSec);
    }
  }

  async hSetS(key, keyName, data, expirationInSec = null) {
    await this.connect();
    await this.client.hsetAsync(key, keyName, data);
    if (expirationInSec) {
      await this.client.expireAsync(key, expirationInSec);
    }
  }

  async del(key) {
    await this.connect();
    return await this.client.delAsync(key);
  }

  async scan(pattern, batchSize = 100) {
    await this.connect();

    let cursor = '0';
    const keys = [];

    do {
      const [nextCursor, resultKeys] = await this.client.scanAsync(cursor, 'MATCH', pattern, 'COUNT', batchSize);
      cursor = nextCursor;
      keys.push(...resultKeys);
    } while (cursor !== '0');

    return keys;
  }

  async unlinkByPattern(pattern, batchSize = 100) {
    await this.connect();
    const keys = await this.scan(pattern, batchSize);
    if (keys.length === 0) return 0;
    return await this.client.unlinkAsync(...keys);
  }

  async quit() {
    if (this.client) {
      await this.client.quitAsync();
      this.client = null;
    }
  }
}

module.exports = Redis;
