package org.hadoop.mr;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DataCount {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(DataCount.class);
		
		job.setMapperClass(DCMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DataBean.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		
		job.setReducerClass(DCReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DataBean.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//job.setPartitionerClass(ServiceProviderPartitioner.class);
		
		job.setNumReduceTasks(Integer.parseInt(args[2]));
		
		job.waitForCompletion(true);
	}
	
	
	public static class DCMapper extends Mapper<LongWritable, Text, Text, DataBean>{

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			//接收数据
			String line = value.toString();
			//分割数据
			String[] fields = line.split("\t");
			//获取有效字段，封装到对象里面
			//手机号
			String telNo = fields[1];
			//上行流量
			long up = Long.parseLong(fields[8]);
			//下行流量
			long down = Long.parseLong(fields[9]);
			
			//封装数据，new DataBean
			DataBean bean = new DataBean(telNo, up, down);
			
			//输出
			context.write(new Text(telNo), bean);
		}
	}
	
	
	public static class DCReducer extends Reducer<Text, DataBean, Text, DataBean>{

		@Override
		protected void reduce(Text key, Iterable<DataBean> v2s, Context context)
				throws IOException, InterruptedException {
			//定义计数器
			long up_sum = 0;
			long down_sum = 0;
			
			//迭代v2s，进行求和
			for(DataBean bean : v2s){
				up_sum += bean.getUpPayLoad();
				down_sum += bean.getDownPayLoad();
			}
			
			//封装数据
			DataBean bean = new DataBean("", up_sum, down_sum);
			
			//输出
			context.write(key, bean);
		}
		
	}

	public static class ServiceProviderPartitioner extends Partitioner<Text, DataBean>{
	
		private static Map<String, Integer> providerMap = new HashMap<String, Integer>();
 		
		static {
			providerMap.put("139", 1);
			providerMap.put("138", 2);
			providerMap.put("159", 3);
		}
		
		@Override
		public int getPartition(Text key, DataBean value, int number) {
			String telNo = key.toString();
			String pcode = telNo.substring(0, 3);
			Integer p = providerMap.get(pcode);
			if(p == null){
				p = 0;
			}
			return p;
		}
		
	}
}
