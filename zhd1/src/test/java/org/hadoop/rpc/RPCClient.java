package org.hadoop.rpc;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.VersionedProtocol;

public class RPCClient {

	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		//BizProtocol proxy = (BizProtocol) RPC.getProxy( BizProtocol.class, 10010, new InetSocketAddress("192.168.1.44", 9527), conf);
		BizProtocol proxy = (BizProtocol) RPC.getProxy( BizProtocol.class, 10010, new InetSocketAddress("localhost", 9527), conf);
		
		String result = proxy.sayHi("tomcat-----------------------");
		System.out.println(result);
		RPC.stopProxy(proxy);
	}

}
