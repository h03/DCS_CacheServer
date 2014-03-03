package cs;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import redis.clients.jedis.Jedis;

/* 
 * CacheServer等待源客户端的连接请求，并创建接收数据的线程,
 * 端口号为:5900
 * 与UserClient中的SendStreamData类对应
 */

public class ReceiveStreamData extends Thread {
	
	private RedisOperation redisOp;
	private int dbIndex;
	private String keySet;
	
	
	public static void main(String[] args) {

	}
	
	
	public ReceiveStreamData(RedisOperation redisOp,int dbIndex,String keySet){
		this.redisOp = redisOp;
		this.dbIndex = dbIndex;
		this.keySet = keySet;
	}
	
	public void run() {
		csReceiveStreamData();
	}

	private void csReceiveStreamData() {
		try {
			System.out.println("Waiting for user client sending data...");
			ServerSocket serverSocket = new ServerSocket(5900);
			Socket connectToClient = null;
			while(true) {
				connectToClient = serverSocket.accept();
				new ReceiveDataThread(connectToClient,redisOp,dbIndex,keySet);
			}
				
			} catch(IOException e) {
				e.printStackTrace();
			}
	
		
	}

}
