package cs;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisOperation {
    private static JedisPool pool;

    private boolean DEBUG;

    public RedisOperation() {

    }
    

    public void debug(String msg) {
        System.out.println("[DEBUG]" + msg);
    }

    public RedisOperation(boolean blockWhenExhaused,int maxIdle,int maxTotal,int minIdle, String host, boolean debug) {
    	JedisPoolConfig config = new JedisPoolConfig();
    	config.setBlockWhenExhausted(blockWhenExhaused);
    	config.setMaxIdle(maxIdle);
    	config.setMaxTotal(maxTotal);
    	config.setMinIdle(minIdle);
    	pool = new JedisPool(host);
        DEBUG = debug;
    }

    public RedisOperation(JedisPoolConfig config, String host, int port, boolean debug) {
        DEBUG = debug;
    }
    
    
    
    
    
    // 获得redis连接
    public Jedis redisGetResource(){
    	return pool.getResource();
    }
    
    // 归还redis连接
    public void redisReturnResource(Jedis jedis){
    	pool.returnResource(jedis);
    }
    
    // JedisPool设置
    public JedisPoolConfig redisJedisPoolConfig(boolean blockWhenExhaused,int maxIdle,int maxTotal,int minIdle) {
    	JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
    	jedisPoolConfig.setBlockWhenExhausted(blockWhenExhaused);
    	jedisPoolConfig.setMaxIdle(maxIdle);
    	jedisPoolConfig.setMaxTotal(maxTotal);
    	jedisPoolConfig.setMinIdle(minIdle);
    	
    	return jedisPoolConfig;
    }
    
    
    
 
    /*
     * *****************************************String相关操作************************************************************	
     */ 	
	
    // 按key，value插入一个String类型的键值对数据
	public void redisSetLine(String key , String value) {
		Jedis jedis = pool.getResource();
		try {
			jedis.set(key,value);
		} catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
                jedis = null;
            }
        } finally {
            if (jedis != null) {
                pool.returnResource(jedis);
                jedis = null;
            }
        }	
	}
	
	// 获取一个String类型的key-value
	   public String redisGetLine(String key) {
			Jedis jedis = pool.getResource();
			try {
				return jedis.get(key);
			} catch (Exception e) {
	            if (jedis != null) {
	                pool.returnBrokenResource(jedis);
	                jedis = null;
	            }
	        } finally {
	            if (jedis != null) {
	                pool.returnResource(jedis);
	                jedis = null;
	            }
	        }	
			return null;
		}

/*
 * *****************************************List相关操作************************************************************	
 */ 
	
    // 从队列（尾）右端插入一个list成员数据
    public void redisRpushLine(String key, String member) {
        Jedis jedis = pool.getResource();
        try {
            jedis.rpush(key, member);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
                jedis = null;
            }
        } finally {
            if (jedis != null) {
                pool.returnResource(jedis);
                jedis = null;
            }
        }
    }

    // 从队列左端插入一个list成员数据
    public void redisLpushLine(String key, String member) {
        Jedis jedis = pool.getResource();
        try {
            jedis.lpush(key, member);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
                jedis = null;
            }
        } finally {
            if (jedis != null) {
                pool.returnResource(jedis);
                jedis = null;
            }
        }
    }

	// 从队列（尾）右端删除一个list类型键值对数据
    public void redisRpopLine(String key) {
        Jedis jedis = pool.getResource();
        try {
            String result = jedis.rpop(key);
            if (DEBUG) {
                debug("The value is " + result);
            }

        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
                jedis = null;
            }
        } finally {
            if (jedis != null) {
                pool.returnResource(jedis);
                jedis = null;
            }
        }
    }

	// 从队列左端删除一个list类型键值对数据
    public String redisLpopLine(String key) {
        Jedis jedis = pool.getResource();
        try {
            String result = jedis.lpop(key);
            if (DEBUG) {
                debug("The value is " + result);
            }
            return result;
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
                jedis = null;
            }
        } finally {
            if (jedis != null) {
                pool.returnResource(jedis);
                jedis = null;
            }
        }
        return null;
    }
    
	// 	从队列中删除指定的一个成员数据
	public void redisLrem(String key,int count,String value){
		Jedis jedis = pool.getResource();
		try {		
			jedis.lrem(key, count, value);
		} catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
                jedis = null;
            }
        } finally {
            if (jedis != null) {
                pool.returnResource(jedis);
                jedis = null;
            }
        }
	}
    
	// 将数据从一个队列中右弹出，再左压入一个队列
	public String redisRpopLpush(String skey,String dkey){
		Jedis jedis = pool.getResource();
		try {		
		     String getResult = jedis.rpoplpush(skey, dkey);
		     return getResult;
		} catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
                jedis = null;
            }
        } finally {
            if (jedis != null) {
                pool.returnResource(jedis);
                jedis = null;
            }
        }
        return null;
	}
	
	// 返回 key 对应 list 的长度,key 不存在返回 0,如果 key 对应类型不是 list 返回错误
	public Long redisListLen(String key) {
        Jedis jedis = pool.getResource();
        try {
			return jedis.llen(key);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
                jedis = null;
            }
        } finally {
            if (jedis != null) {
                pool.returnResource(jedis);
                jedis = null;
            }
        }
        return null;
		}

	// 获取一个List中的所有元素或某一范围内的元素
    public List<String> redisLrange(String key, long start, long end) {
        Jedis jedis = pool.getResource();
        try {
            List<String> list = jedis.lrange(key, start, end);
            return list;
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
                jedis = null;
            }
        } finally {
            if (jedis != null) {
                pool.returnResource(jedis);
                jedis = null;
            }
        }
        return null;
    }

    /*
     * *****************************************Set相关操作************************************************************	
     */ 
	// 插入一个set类型键值对数据
    public void redisSaddLine(String key, String member) {
        Jedis jedis = pool.getResource();
        try {
            jedis.sadd(key, member);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
                jedis = null;
            }
        } finally {
            if (jedis != null) {
                pool.returnResource(jedis);
                jedis = null;
            }
        }
    }

	// 向set中插入一组数据
    public void redisSaddGroup(String key, String[] members) {
        Jedis jedis = pool.getResource();
        try {
            jedis.sadd(key, members);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
                jedis = null;
            }
        } finally {
            if (jedis != null) {
                pool.returnResource(jedis);
                jedis = null;
            }
        }
    }

	// 随机从set中选择一个元素，但不删除
    public String redisSrandMember(String key) {
        Jedis jedis = pool.getResource();
        try {
            String result = jedis.srandmember(key);
            if (DEBUG) {
                debug("The value is " + result);
            }
            return result;
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
                jedis = null;
            }
        } finally {
            if (jedis != null) {
                pool.returnResource(jedis);
                jedis = null;
            }
        }
        return null;
    }
    
   // 删除并返回 key 对应 set 中随机的一个元素,如果 set 是空或者 key 不存在返回 nil
    public String redisSpop(String key) {
        Jedis jedis = pool.getResource();
        try {
            String result = jedis.spop(key);
            if (DEBUG) {
                debug("The value is " + result);
            }
            return result;
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
                jedis = null;
            }
        } finally {
            if (jedis != null) {
                pool.returnResource(jedis);
                jedis = null;
            }
        }
        return null;
    }

	// 返回 key 对应 set 的所有元素,结果是无序的
    public Set<String> redisSmembers(String key) {
        Jedis jedis = pool.getResource();
        try {
            Set<String> result = jedis.smembers(key);
            if (DEBUG) {
                debug("The value is " + result);
            }
            return result;
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
                jedis = null;
            }
        } finally {
            if (jedis != null) {
                pool.returnResource(jedis);
                jedis = null;
            }
        }
        return null;
    }

    /*
     * *****************************************Sorted Set相关操作************************************************************	
     */ 
	// 插入一个sorted set类型键值对数据
    public void redisZaddLine(String key, String score, String member) {
        Jedis jedis = pool.getResource();
        try {
            if (!Double.valueOf(score).isNaN()) {
                jedis.zadd(key, Double.parseDouble(score), member);
            }
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
                jedis = null;
            }
        } finally {
            if (jedis != null) {
                pool.returnResource(jedis);
                jedis = null;
            }
        }
    }
    
	// 删除一个sorted set类型键值对数据中的指定元素
    public void redisZremLine(String key, String member) {
        Jedis jedis = pool.getResource();
        try {
            jedis.zrem(key, member);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
                jedis = null;
            }
        } finally {
            if (jedis != null) {
                pool.returnResource(jedis);
                jedis = null;
            }
        }
    }

	// 返回sorted set集合中 score 在给定区间的元素
    public Set<String> redisZrangeByScore(String key, String min, String max) {
        Jedis jedis = pool.getResource();
        try {
            Set<String> result = jedis.zrangeByScore(key, min, max);
            if (DEBUG) {
                StringBuffer buffer = new StringBuffer();
                for (Iterator iter = result.iterator(); iter.hasNext(); ) {
                    buffer.append(iter.next().toString() + " ");
                }
                debug("The value is " + buffer.toString());
            }
            return result;
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
                jedis = null;
            }
        } finally {
            if (jedis != null) {
                pool.returnResource(jedis);
                jedis = null;
            }
        }
        return null;
    }

	// 返回sorted set集合中 排名 在给定区间的元素（按score从小到大排序）
    public Set<String> redisZrange(String key, long start, long end) {
        Jedis jedis = pool.getResource();
        try {
            Set<String> result = jedis.zrange(key, start, end);
            if (DEBUG) {
                StringBuilder builder = new StringBuilder();
                for (String s : result) {
                    builder.append(s + " ");
                }
                debug("The value is " + builder.toString());
            }
            return result;
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
                jedis = null;
            }
        } finally {
            if (jedis != null) {
                pool.returnResource(jedis);
                jedis = null;
            }
        }
        return null;
    }

	// 返回sorted set中给定元素对应的 score
    public String redisZscore(String key, String element) {
        Jedis jedis = pool.getResource();
        try {
            String result = jedis.zscore(key, element).toString();
            if (DEBUG) {
                debug("The score is " + result);
            }
            return result;
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
                jedis = null;
            }
        } finally {
            if (jedis != null) {
                pool.returnResource(jedis);
                jedis = null;
            }
        }
        return null;
    }

//  删除sorted set集合中 score 在给定区间的元素
    public void redisZremRangeByScore(String key, String min, String max) {
        Jedis jedis = pool.getResource();
        try {
            jedis.zremrangeByScore(key, min, max);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
                jedis = null;
            }
        } finally {
            if (jedis != null) {
                pool.returnResource(jedis);
                jedis = null;
            }
        }
    }

	// 删除sorted set集合中在给定 排名 区间内的元素
    public void redisZremRangeByRank(String key, long start, long end) {
        Jedis jedis = pool.getResource();
        try {
            jedis.zremrangeByRank(key, start, end);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
                jedis = null;
            }
        } finally {
            if (jedis != null) {
                pool.returnResource(jedis);
                jedis = null;
            }
        }
    }

 // 返回sorted set集合中元素的个数
    public Long redisZcard(String key) {
        Jedis jedis = pool.getResource();
        try {
            Long result = jedis.zcard(key);
            if (DEBUG) {
                debug("The result is " + result);
            }
            return result;
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
                jedis = null;
            }
        } finally {
            if (jedis != null) {
                pool.returnResource(jedis);
                jedis = null;
            }
        }
        return null;
    }
    
    
    /*
     * *****************************************Hash相关操作************************************************************	
     */ 
    
	// 插入一个hash类型键值对数据，设置 hash field 为指定值,如果 key 不存在,则先创建
    public void redisHsetLine(String key, String field, String value) {
        Jedis jedis = pool.getResource();
        try {
            jedis.hset(key, field, value);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
                jedis = null;
            }
        } finally {
            if (jedis != null) {
                pool.returnResource(jedis);
                jedis = null;
            }
        }
    }
    
	// 插入一个hash类型键值对数据，设置多个 hash field 为指定值,如果 key 不存在,则先创建
    public void redisHmsetLine(String key, Map<String,String> map) {
        Jedis jedis = pool.getResource();
        try {
            jedis.hmset(key, map);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
                jedis = null;
            }
        } finally {
            if (jedis != null) {
                pool.returnResource(jedis);
                jedis = null;
            }
        }   	
    }
    
	// 获取一个指定的 hash field
	   public String redisHgetLine(String key, String field) {
		   Jedis jedis = pool.getResource();
		   try {
			   return jedis.hget(key, field);
			} catch (Exception e) {
	            if (jedis != null) {
	                pool.returnBrokenResource(jedis);
	                jedis = null;
	            }
	        } finally {
	            if (jedis != null) {
	                pool.returnResource(jedis);
	                jedis = null;
	            }
	        }
	        return null;
	   }

	// 获取多个指定的 hash field
    public List<String> redisHmget(String key, String fields) {
        Jedis jedis = pool.getResource();
        try {
		     List<String> getResult = jedis.hmget(key, fields);
		     return getResult;
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
                jedis = null;
            }
        } finally {
            if (jedis != null) {
                pool.returnResource(jedis);
                jedis = null;
            }
        }
        return null;
    }
    
    // 删除一个指定的hash field数据
    public void redisHdel(String key, String field) {
        Jedis jedis = pool.getResource();
        try {
            jedis.hdel(key, field);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
                jedis = null;
            }
        } finally {
            if (jedis != null) {
                pool.returnResource(jedis);
                jedis = null;
            }
        }
    }
    


 // 返回指定 hash 的 field 数量
    public Long redisHlen(String key) {
        Jedis jedis = pool.getResource();
        try {
            Long num = jedis.hlen(key);
            if (DEBUG) {
                debug("The length is " + num);
            }
            return num;
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
                jedis = null;
            }
        } finally {
            if (jedis != null) {
                pool.returnResource(jedis);
                jedis = null;
            }
        }
        return null;
    }

 // 返回一个 hash 的所有 field
    public Set<String> redisHkeys(String key) {
        Jedis jedis = pool.getResource();
        try {
            Set<String> result = jedis.hkeys(key);
            if (DEBUG) {
                debug("The result is " + key);
            }
            return result;
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
                jedis = null;
            }
        } finally {
            if (jedis != null) {
                pool.returnResource(jedis);
                jedis = null;
            }
        }
        return null;
    }

    // 返回一个 hash 的所有 value
    public List<String> redisHvals(String key) {
        Jedis jedis = pool.getResource();
        try {
            List<String> result = jedis.hvals(key);
            if (DEBUG) {
                debug("The result is " + key);
            }
            return result;
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
                jedis = null;
            }
        } finally {
            if (jedis != null) {
                pool.returnResource(jedis);
                jedis = null;
            }
        }
        return null;
    }
    
    // 返回一个 hash 的所有 filed 和 value   
    public Map<String, String> redisHgetAll(String key) {
        Jedis jedis = pool.getResource();
        try {
            Map<String, String> result = jedis.hgetAll(key);
            return result;
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
                jedis = null;
            }
        } finally {
            if (jedis != null) {
                pool.returnResource(jedis);
                jedis = null;
            }
        }
        return null;
    }
    
  /*
   * *****************************************key的相关操作************************************************************	
   */ 
    
    // 查看一个数据库中是否存在某个key
 	public boolean redisExists(String key) {
 		Jedis jedis = pool.getResource();
 		try {
 			return jedis.exists(key);
 		} catch (Exception e) {
             if (jedis != null) {
                 pool.returnBrokenResource(jedis);
                 jedis = null;
             }
         } finally {
             if (jedis != null) {
                 pool.returnResource(jedis);
                 jedis = null;
             }
         }	
 		return false;
 	}

 	
	// 删除某一个key
    public void redisDeleOne(String key) {
        Jedis jedis = pool.getResource();
        try {
            jedis.del(key);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
                jedis = null;
            }
        } finally {
            if (jedis != null) {
                pool.returnResource(jedis);
                jedis = null;
            }
        }
    }

    // 返回给定 key 的 value 类型
    public String redisType(String key) {
        Jedis jedis = pool.getResource();
        try {
            return jedis.type(key);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
                jedis = null;
            }
        } finally {
            if (jedis != null) {
                pool.returnResource(jedis);
                jedis = null;
            }
        }
        return null;
    }

    
    // 返回当前数据库的 key 数量
 	public Long redisSizeDB() {
 		Jedis jedis = pool.getResource();
 		try {
 			return jedis.dbSize();
 		} catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
                jedis = null;
            }
        } finally {
            if (jedis != null) {
                pool.returnResource(jedis);
                jedis = null;
            }
        }
 		return null;
 	}
    
 	
    // 为 key 指定过期时间,单位是秒。返回 1 成功,返回 0 表示 key 已经设置过过期时间或者不存在
    public void redisExpireKey(String key, int seconds) {
        Jedis jedis = pool.getResource();
        try {
            jedis.expire(key, seconds);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
                jedis = null;
            }
        } finally {
            if (jedis != null) {
                pool.returnResource(jedis);
                jedis = null;
            }
        }
    }
    
    
    // ttl key 返回设置过过期时间的 key 的剩余过期秒数， -1 表示 key 不存在或者没有设置过过期时间
    public Long redisTTL(String key) {
        Jedis jedis = pool.getResource();
        try {
            return jedis.ttl(key);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
                jedis = null;
            }
        } finally {
            if (jedis != null) {
                pool.returnResource(jedis);
                jedis = null;
            }
        }
        return null;
    }

    // select db-index 通过索引选择数据库,默认连接的数据库所有是 0,默认数据库数是 16 个。返回 1 表示成功,0 失败
    public boolean redisSelectDB(int index) {
        Jedis jedis = pool.getResource();
        try {
            if (index >= 0 && index < 16) {
                jedis.select(index);
                return true;
            }
            return false;
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
                jedis = null;
            }
        } finally {
            if (jedis != null) {
                pool.returnResource(jedis);
                jedis = null;
            }
        }
        return false;
    }

    // move key db-index 将 key 从当前数据库移动到指定数据库。返回 1 成功，返回 0 则 key 不存在,或者已经在指定数据库中
    public boolean redisMoveKey(String key, int index) {
        Jedis jedis = pool.getResource();
        try {
            return (jedis.move(key, index) == 1);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
                jedis = null;
            }
        } finally {
            if (jedis != null) {
                pool.returnResource(jedis);
                jedis = null;
            }
        }
        return false;
    }
    
    // 删除当前数据库的所有key，此方法不会失败。慎用
    public void redisDeleteDB() {
        Jedis jedis = pool.getResource();
        try {
            jedis.flushDB();
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
                jedis = null;
            }
        } finally {
            if (jedis != null) {
                pool.returnResource(jedis);
                jedis = null;
            }
        }
    }

    // 删除所有数据库中的所有 key,此方法不会失败。更加慎用
    public void redisFlushALL() {
        Jedis jedis = pool.getResource();
        try {
            jedis.flushAll();
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
                jedis = null;
            }
        } finally {
            if (jedis != null) {
                pool.returnResource(jedis);
                jedis = null;
            }
        }
    }

}