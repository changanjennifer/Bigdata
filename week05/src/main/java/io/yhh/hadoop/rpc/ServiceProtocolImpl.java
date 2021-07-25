package io.yhh.hadoop.rpc;

import java.io.IOException;

import org.apache.hadoop.ipc.ProtocolSignature;

public class ServiceProtocolImpl implements ServiceProtocol{

	public long getProtocolVersion(String protocol, long clientVersion) throws IOException {

		return ServiceProtocol.versionID;
	}

	public ProtocolSignature getProtocolSignature(String protocol, long clientVersion, int clientMethodsHash)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	public String findName(String studentId) {
		System.out.println("ServiceProtocolImpl findName studentId="+studentId);

		if ("20210579020278".equals(studentId)) {
			return "yinhuihui";
		}else {
			return null;
		}

	}

	public int add(int v1, int v2) {
		System.out.println("add v1="+ v1 +" v2="+ v2);
		return v1+v2;
	}

}
