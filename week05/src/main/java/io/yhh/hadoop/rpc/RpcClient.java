package io.yhh.hadoop.rpc;

import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

public class RpcClient {

	static InetSocketAddress addr = new InetSocketAddress("127.0.0.1", RpcServer.PORT);
	
	static void printName(String studentId, String studentName) {
		if (null== studentName) {
			System.out.println("no such student, studentId=" + studentId);
		}else {
			System.out.println("Yes, studentId=" + studentId +" name=" + studentName);
		}
		
	}
	public static void main(String[] args) throws Exception {
		ServiceProtocol proxy = RPC.getProxy(ServiceProtocol.class, ServiceProtocol.versionID, addr, new Configuration());
		
		String studentId ="1111122";
		System.out.println("call ServiceProtocol: findName,studentId= " + studentId);
		
		String studentName = proxy.findName(studentId);
		printName(studentId, studentName);
		
		studentId ="20210579020278";
		System.out.println("call ServiceProtocol: findName,studentId= " + studentId);		
		studentName = proxy.findName(studentId);
		printName(studentId, studentName);
	}

}
