package org.hadoop.hdfs;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

public class HDFSDemo {

	FileSystem fs = null;
	
	@Before
	public void init() throws IOException, URISyntaxException, InterruptedException{
		fs = FileSystem.get(new URI("hdfs://192.168.119.5:9000"), new Configuration(), "root");
		System.out.println(fs);
	}
	
	@Test
	public void testUpload() throws IllegalArgumentException, IOException{
		fs.copyFromLocalFile(new Path("/root/jdk"), new Path("/jdk111"));
	}
	
	@Test
	public void testDownload() throws IllegalArgumentException, IOException{
		InputStream in = fs.open(new Path("/jdk.tar.gz"));
		OutputStream out = new FileOutputStream("/home/jjj");
		IOUtils.copyBytes(in, out, 4096, true);
	}
	
	@Test
	public void testMkdir() throws IllegalArgumentException, IOException{
		boolean flag = fs.mkdirs(new Path("123"));
		System.out.println(flag);
	}
	
	@Test
	public void testDel() throws IllegalArgumentException, IOException{
		boolean flag = fs.delete(new Path("/user"), true);
		System.out.println(flag);
	}
	
	public static void main(String[] args) throws Exception {
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.119.5:9000"), new Configuration());
		InputStream in = fs.open(new Path("/jdk.tar.gz"));
		OutputStream out = new FileOutputStream("/home/jjj");
		IOUtils.copyBytes(in, out, 4096, true);
	}
	
	
	
	
	
	
	
	
	

}
