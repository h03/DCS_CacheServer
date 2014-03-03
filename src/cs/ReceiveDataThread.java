package cs;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

import redis.clients.jedis.Jedis;

 /*
  *  CacheServer接收源客户端数据
  *  由ReceiveStreamData类创建和启动线程
  */

public class ReceiveDataThread extends Thread {
	private Socket connectToClient;
	private DataInputStream fromUClient;
	private long interval; // 当前系统时间，以计算间隔时间
	private RedisOperation redisOp;
	private Jedis jedis;  // redis连接
	private String keySet; // 按时序保存多个String key的sorted set


	// 接收带时序特征的流式数据
	public ReceiveDataThread(Socket socket,RedisOperation redisOp,int dbIndex,String keySet) throws IOException {
		super();
		connectToClient = socket;
		this.redisOp = redisOp;
		this.jedis = this.redisOp.redisGetResource();
		this.keySet = keySet;
		fromUClient = new DataInputStream(connectToClient.getInputStream());
		jedis.select(dbIndex);
		start();
		
	}
	
	public void run() {
		try {
			String userID = null;
			String userKey;
			Long timeStamp;
			String strData = null;
			boolean flag = true;
			long i = 0;
			
			if(jedis.zcard(keySet)>600){
			//	Thread.yield();
			}

     		while(flag) {
				userID= fromUClient.readUTF();
				if(!userID.equals("EndInput") && userID!=null){
					timeStamp = fromUClient.readLong();
					userKey = userID + i +"-" + timeStamp;
					strData = fromUClient.readUTF();
					jedis.set(userKey,strData); // 将userKey和原始数据按String类型key-value存入redis
					jedis.zadd(keySet, timeStamp, userKey); // 将userKey按timeStamp排序存入到keySet中					
					i++;
					flag = true;
				} 
				else if (userID.equals("EndInput")){
					System.out.println("EndInput from a user client !");
				    flag = false;
				}
				else {
					System.out.println("No data from a user client !");
					flag = false;
				}
			}
			fromUClient.close();
			connectToClient.close();
			redisOp.redisReturnResource(jedis);
		} catch(IOException e) {
			e.printStackTrace();
		}
	}

}
