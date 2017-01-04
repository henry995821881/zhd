package org.hadoop.rpc;

import org.apache.hadoop.ipc.VersionedProtocol;

public interface BizProtocol  {
	
	public static final long versionID = 10010L;
	
	public String sayHi(String name);
	
}
