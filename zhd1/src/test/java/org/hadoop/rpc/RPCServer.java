package org.hadoop.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;

public class RPCServer implements BizProtocol {


	public String sayHi(String name) {
		return "Hi ~ " + name;
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Server server = new RPC.Builder(conf).setInstance(new RPCServer()).setProtocol(BizProtocol.class).setBindAddress("192.168.1.44").setPort(9527).build();
		
		server.start();
	}

	
}
