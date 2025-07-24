import { createClient } from 'redis';
import { config } from '../config.js'; 

export default class Redis {
  constructor(redisUrl = null) {
    this.redisUrl = redisUrl || config.url;
    this.client = null;
  }

  async connect() {
    if (this.client) return;

    this.client = createClient({ url: this.redisUrl });

    this.client.on('error', (err) => {
      console.error('[Redis Error]:', err);
    });

    await this.client.connect();
  }

  async get(key) {
    await this.connect();
    return await this.client.get(key);
  }

async hGet(key, fields = null) {
    await this.connect();

    if (!key) {
        return null;
    }

    if (fields && Array.isArray(fields)) {
        const data = await this.client.hmGet(key, fields);
        return data.reduce((acc, value, index) => {
            acc[fields[index]] = value;
            return acc;
        }, {});
    } else if (fields && typeof fields === 'string') {
        const value = await this.client.hGet(key, fields);
        return { [fields]: value };
    } else {
        const data = await this.client.hGetAll(key);
        if (!data || Object.keys(data).length === 0) {
            return null;
        }
        return data;
    }
}

  async set(key, value, expirationInSec = null) {
    await this.connect();
    if (expirationInSec) {
       await this.client.set(key, value, { EX: expirationInSec });
    } else {
       await this.client.set(key, value);
    }
  }

  async hSet(key, data, expirationInSec = null) {
    await this.connect();
  
    await this.client.hSet(key, data);
  
    // Optionally set expiry on the key
    if (expirationInSec) {
      await this.client.expire(key, expirationInSec);
    }
  }

  async hSetS(key, keyName, data, expirationInSec = null) {
    await this.connect();
  
    await this.client.hSet(key, keyName, data);
  
    // Optionally set expiry on the key
    if (expirationInSec) {
      await this.client.expire(key, expirationInSec);
    }
  }


  async del(key) {
    await this.connect();
    return await this.client.del(key);
  }

  async scan(pattern, batchSize = 100) {
    await this.connect();

    let cursor = 0;
    const keys = [];

    do {
      const result = await this.client.scan(cursor, {
        MATCH: pattern,
        COUNT: batchSize,
      });
      cursor = result.cursor;
      keys.push(...result.keys);
    } while (cursor !== '0');

    return keys;
  }

  async unlinkByPattern(pattern, batchSize = 100) {
    await this.connect();
    const keys = await this.scan(pattern, batchSize);
    if (keys.length === 0) return 0;
    return await this.client.unlink(...keys);
  }

  async quit() {
    if (this.client) {
      await this.client.quit();
      this.client = null;
    }
  }
}
