package org.hadoop.mr.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 * 1.定义一个类，这个类是MR执行入口
 * 2.定义一个类，这个类要继承org.apache.hadoop.mapreduce.Mapper，指定泛型的类型（k1,v1和k2,v2），然后重新map方法，接收数据，实现具体的业务逻辑
 * 3.定义一个类，这个类要继承org.apache.hadoop.mapreduce.Reducer，指定泛型的类型(k2,v2和k3,v3)，然后重新reduce方法，接收数据(k2和v2s，已经分组)，实现具体的业务逻辑
 * 4.在main方法中将自定义的mapper和reducer组装起来，提交
 * 5.打包
 * @author zx
 *
 */
public class WordCount {

	public static void main(String[] args) throws Exception {
		// 构造一个Job对象，job对象要依赖Configuration
		Configuration conf = new Configuration();
		conf.set("mapreduce.framework.name", "yarn");
		conf.set("yarn.resourcemanager.hostname", "itcast01");
		Job job = Job.getInstance(conf);

		//注意：要将main所在的类设置一下
		job.setJarByClass(WordCount.class);
		
		//设置Mapper相关的属性
		job.setMapperClass(WCMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		
		//设置Reducer相关属性
		job.setReducerClass(WCReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setCombinerClass(WCReducer.class);
		
		//提交任务
		job.waitForCompletion(true);
		
		
		
	}
	
	

}
