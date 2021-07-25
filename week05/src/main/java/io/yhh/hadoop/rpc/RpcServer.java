package io.yhh.hadoop.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

public class RpcServer {
	
	public final static int PORT = 8899;
	
	
	public static void main(String[] args) {
		RPC.Builder builder = new RPC.Builder(new Configuration());
		
		//bind ip
		builder.setBindAddress("127.0.0.1");
		//set port
		builder.setPort(RpcServer.PORT);
		
		builder.setProtocol(ServiceProtocol.class);
		builder.setInstance(new ServiceProtocolImpl());
		
		try {
			
			RPC.Server server = builder.build();
			server.start();
			System.out.println("Hadoop Server started at:" + "127.0.0.1" +":" + RpcServer.PORT);
		}catch(Exception e) {
			e.printStackTrace();
		}
	}

}
