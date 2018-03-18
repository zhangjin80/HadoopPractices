package practice02;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import practice02.gc.NumberSrotGroupComparator;
import practice02.pojo.NumberSort;

public class Q6 {

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
//		conf.set("fs.defaultFS", "hdfs://hadoop02:9000");
//		System.setProperty("HADOOP_USER_NAME", "hadoop");
		FileSystem fs = FileSystem.get(conf);
		Job job = Job.getInstance(conf);
		
		
		job.setJarByClass(Q6.class);
		job.setMapperClass(MR_Mapper.class);
		job.setReducerClass(MR_Reducer.class);
		
		job.setMapOutputKeyClass(NumberSort.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(NumberSort.class);
		
		job.setGroupingComparatorClass(NumberSrotGroupComparator.class);
		
		Path inputPath = new Path("D:\\bigdata\\flow\\input\\number");
		Path outputPath = new Path("D:\\bigdata\\flow\\output\\number");
		FileInputFormat.setInputPaths(job, inputPath);
		if(fs.exists(outputPath)){
			fs.delete(outputPath, true);
		}
		FileOutputFormat.setOutputPath(job, outputPath);
		
		
		boolean isDone = job.waitForCompletion(true);
		System.exit(isDone ? 0 : 1);
	}
	
	public static class MR_Mapper extends Mapper<LongWritable, Text, NumberSort, NullWritable>{
		ArrayList<Integer> list = new ArrayList<>();
		NumberSort numberSort = new NumberSort();
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			int v = Integer.parseInt(value.toString());
			list.add(v);
		}
		
		@Override
		public void run(Context context)throws IOException, InterruptedException {
			setup(context);
		    try {
		      while (context.nextKeyValue()) {
		        map(context.getCurrentKey(), context.getCurrentValue(), context);
		        
		        //把每个mapTask完成后对list进行排序，在内存中进行排序
		        Collections.sort(list);
		        //把排好序的list封装进对象进行分组和排序
		        for(int i=0;i<list.size();i++){
		        	//按序号分组
		        	numberSort.setGroup(i);
		        	//按数字排序
		        	numberSort.setNum(list.get(i));
		        }
		        context.write(numberSort, NullWritable.get());
		        list.clear();
		        
		      }
		    } finally {
		      cleanup(context);
		    }
		}
	}
	
	public static class MR_Reducer extends Reducer<NumberSort, NullWritable, IntWritable, NumberSort>{
		//第一列序号
		int count =0;
		IntWritable outKey = new IntWritable();
		@Override
		protected void reduce(NumberSort key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
			for(NullWritable nvl: values){
				count++;
				outKey.set(count);
				//key是numberGroup对象
				context.write(outKey, key);
			}
		}
	}
}
