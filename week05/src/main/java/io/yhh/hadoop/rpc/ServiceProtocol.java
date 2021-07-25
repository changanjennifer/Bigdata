package io.yhh.hadoop.rpc;

import org.apache.hadoop.ipc.VersionedProtocol;

/**
 * @author JenniferYin
 *
 */
public interface ServiceProtocol extends VersionedProtocol{

	public static final long versionID = 1L;
	
	String findName(String studentId);
	
	int add (int v1, int v2);
}
