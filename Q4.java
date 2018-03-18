package practice02;

import java.io.IOException;

import org.apache.commons.lang.ObjectUtils.Null;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Q4 {

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
//		conf.set("fs.defaultFS", "hdfs://hadoop02:9000");
//		System.setProperty("HADOOP_USER_NAME", "hadoop");
		FileSystem fs = FileSystem.get(conf);
		Job job = Job.getInstance(conf);
		
		
		job.setJarByClass(Q4.class);
		job.setMapperClass(MR_Mapper.class);
		job.setReducerClass(MR_Reducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		
		Path inputPath = new Path("D:\\bigdata\\flow\\input\\version");
		Path outputPath = new Path("D:\\bigdata\\flow\\output\\version");
		FileInputFormat.setInputPaths(job, inputPath);
		if(fs.exists(outputPath)){
			fs.delete(outputPath, true);
		}
		FileOutputFormat.setOutputPath(job, outputPath);
		
		
		boolean isDone = job.waitForCompletion(true);
		System.exit(isDone ? 0 : 1);
	}
	
	public static class MR_Mapper extends Mapper<LongWritable, Text, Text, Text>{
		/**
		 * 20170309,徐峥,光环斗地主,19,360手机助手,0.5版本,北京
		20170309,徐峥,光环斗地主,15,360手机助手,0.4版本,北京

		我的思考：
		1. map 输出
			key=a[1] value a[0]+a[2].....a[length-1]
		2. reduce 
			设一个存储版本currentversion
			1.遍历values
				切分value
				计数器为1的时候存入版本
				如果s[4]不等于currentversion
					加入字符串，输出key
				否则重新拼接key输出
				*/
		Text outKey = new Text();
		Text outValue = new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] split = value.toString().split(",");
			String name = split[1];
			outKey.set(name);
			String str = split[0]+","+split[2]+","+split[3]+","+split[4]+","+split[5]+","+split[6];
			outValue.set(str);
			context.write(outKey, outValue);
		}
		
	}
	/**
	 * 
	 * 徐峥,  20170309,光环斗地主,15,360手机助手,0.4版本,北京
	 */
	public static class MR_Reducer extends Reducer<Text, Text, Text, NullWritable>{
		Text outKey = new Text();
		Text outValue = new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String currentversion="";
			for(Text value: values){
				String[] split = value.toString().split(",");
				if(currentversion.equals("")){
					currentversion = split[4];
				}
				if(!currentversion.equals(split[4])){
					String k = split[0]+","+key.toString()+","+split[1]+","+split[2]+","+split[3]+","+split[4]+","+split[5]+","+currentversion;
					currentversion = split[4];
					outKey.set(k);
				}else{
					String k = split[0]+","+key.toString()+","+split[1]+","+split[2]+","+split[3]+","+split[4]+","+split[5];
					outKey.set(k);
				}
				context.write(outKey, NullWritable.get());
			}
		}
	}
}
