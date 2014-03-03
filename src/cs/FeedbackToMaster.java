package cs;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;



/* CacheServer向Master发送本地服务器的运行状态
 * 端口号：5700
 * 对应Master程序中的ReceiveFromCache
 */

public class FeedbackToMaster {
	
	private String masterIP;
	private String cacheIP;
	
public static void main(String[] srgs) {

	}



public FeedbackToMaster(String masterIP,String cacheIP){
	this.masterIP = masterIP;
	this.cacheIP = cacheIP;
}

    // 获取本机IP地址
	public static String csGetLocalIP() {
		try {
		InetAddress ind = InetAddress.getLocalHost();
		return ind.getHostAddress().toString();
		} catch(IOException e) {
			System.out.println("获取本地IP地址发生异常！");
			return null;
		}	
	}
	
	
	// 缓存节点将自己的状态发送给Master
	public void csFeedbackToMaster() {
		try {
			String state = null;
			String ack = null;
			Socket connectToMaster = new Socket(masterIP,5700);
			DataOutputStream toMaster = new DataOutputStream(connectToMaster.getOutputStream());
			DataInputStream fromMaster = new DataInputStream(connectToMaster.getInputStream());
			state = CacheServerState.csCheckState();
			toMaster.writeUTF(state);  // 发送cache server的状态信息
			toMaster.writeUTF(cacheIP);  // 发送cache server的本地IP地址
			toMaster.flush();
			
				ack = fromMaster.readUTF();
		
			if(ack == null) {
				System.out.println("master has no feedback！");
			}
			else if(ack.equals("OK")){
				System.out.println("Cache node " + cacheIP + " 已将状态发送给Master服务器！");

			} else if(ack.equals("yes")){
				System.out.println("已将高负载信息更新至cache集群表中！");
				
			} else if(ack.equals("wait")) {
				System.out.println("cache服务器将不再接收新的请求，直到负载降级！");
				
			} else if(ack.equals("save")){
				System.out.println("需要完成善后工作！将内存数据全部持久化！");
			} 
							
			fromMaster.close();
			toMaster.close();
			connectToMaster.close();
			
		} catch(IOException e) {
			e.printStackTrace();
		} finally {
			
		}
	}
	

}
