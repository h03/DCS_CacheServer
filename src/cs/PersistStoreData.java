package cs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.client.Put;

import redis.clients.jedis.Jedis;


/* 
 * 持久化存储类
 * 将Redis中的数据批量存入HBase中
 */

public class PersistStoreData{
	
	
//	RedisOperation redisOp = new RedisOperation(new JedisPoolConfig(), "localhost", true);
	
	private Jedis jedis;  // redis连接
	private RedisOperation redisOp;
	private int dbIndex; // 需要持久化的数据在redis中所属的数据库序号
	private String key;   // 需要持久化数据的所属key组
	private long amount;   // 每次批量持久化的数据量
	private String tableName; // 需要持久化到HBase中的某个表名
	private String colFamily; 
	private String column; 
	private HbaseOperation hbaseOpt;
	
	
	// 构造函数，设定需要批量持久化的数据key以及数据量。
	public PersistStoreData(HbaseOperation hbaseOpt,RedisOperation redisOp,int dbIndex,String key,long amount,String tableName,String colFamily,String column){
		this.redisOp = redisOp;
		this.jedis = this.redisOp.redisGetResource();
		this.dbIndex = dbIndex;
		this.key = key;
		this.amount = amount;
		this.tableName = tableName;
		this.colFamily = colFamily;
		this.column = column;
		this.hbaseOpt = hbaseOpt;

	}
	
	public boolean runPersist(long last){
		long current = System.currentTimeMillis();
		try {
			if(jedis.zcard(key)>2*amount){
				csPersistDataInOrder(hbaseOpt,amount);			
				long end = System.currentTimeMillis();
				redisOp.redisReturnResource(jedis);
				System.out.println("Hbase持久化" + amount + "条数据，共执行了：" + (end-current) + "毫秒");
				return true;
			}	
			else if((current-last)>1000*6){
				long num = jedis.zcard(key);
				if (num>0){
				csPersistDataInOrder(hbaseOpt,num);
				long end = System.currentTimeMillis();
				redisOp.redisReturnResource(jedis);
				System.out.println("Hbase持久化" + num + "条数据，共执行了：" + (end-current) + "毫秒");
				}
				return true;
			} else {
				redisOp.redisReturnResource(jedis);
				System.out.println("No data to persist !");
				
				return false;
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}
	
	// 将指定的一个redis数据库中的数据批量存入hbase中,这个在cache 节点请求下线时调用，目前未使用到。
	public void csPersistStoreData() throws IOException {		
        jedis.select(dbIndex);
        Set<String> keys = jedis.keys("*");   //列出所有的key，查找特定的key如：redis.keys("foo")   
        Iterator<String> t1=keys.iterator();
        HbaseOperation hbaseOpt = new HbaseOperation(tableName);
        List<Put> batchPut = new ArrayList<Put>();
        String userRowKey = null;
        String value = null;
        int i = 0;
        while(t1.hasNext()){   
            userRowKey = t1.next();
            value = jedis.get(userRowKey);
    		System.out.println(value);
    			 Put put = new Put(userRowKey.getBytes());
    			 put.add(colFamily.getBytes(), column.getBytes(), value.getBytes());
    			 batchPut.add(put);                     
            i++;
            
            if(i%amount == 0){
            	hbaseOpt.hbaseBatchPut(tableName, batchPut);
            	System.out.println("Insert " + i + " data !");
            	i = 0;
            }
        }   
        if(i!=0){
        hbaseOpt.hbaseBatchPut(tableName, batchPut);
        System.out.println("OK! Insert " + i + " data !");
        }       
	}

	
	// 按keySet(sorted set)中的时序顺序将redis数据库中的一组指定key-value数据批量存入hbase中
	public void csPersistDataInOrder(HbaseOperation hbaseOpt,long storeNum) throws IOException {
		jedis.select(dbIndex);
		Set<String> set = jedis.zrange(key, 0, storeNum-1);		
        List<Put> batchPut = new ArrayList<Put>();
        String value = null;
        int i = 0;
        String[] userRowKeys = new String[set.size()];
		for(Iterator<String> it = set.iterator(); it.hasNext(); ) {
			 userRowKeys[i] = it.next();
			 value = jedis.get(userRowKeys[i]);
			 Put put = new Put(userRowKeys[i].getBytes());
			 put.add(colFamily.getBytes(), column.getBytes(), value.getBytes());
			 batchPut.add(put);
			 i++;
		}	
        hbaseOpt.hbaseBatchPut(tableName, batchPut);    // 这之后为刚被删除的redis数据设置TTL，或将数据成组从redis中删除以留出内存空间
        System.out.println("OK! Batch put " + i + " data into HBase !");    
        jedis.zrem(key, userRowKeys);
        jedis.del(userRowKeys);
        System.out.println("Delete " + i + " key-values from redis !");    
		
	}
	
	

}
