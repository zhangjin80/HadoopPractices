package practice02;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.mortbay.jetty.servlet.Context;

public class Q3_1 {
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"Q3_1");
		job.setJarByClass(Q3_1.class);
		
		job.setMapperClass(MyCFMapper.class);
		job.setReducerClass(MyCFReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path("d:/bigdata/flow/input/grad"));
		FileOutputFormat.setOutputPath(job, new Path("d:/bigdata/flow/output/grad"));
		
		boolean completion = job.waitForCompletion(true);
		System.exit(completion?0:1);
		
	}
	
	public static class MyCFMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{
		/**
		 *algorithm,huangzitao,85,86,41,75,93,42,85,75
		 *1、统计每门课程的参考人数和课程平均分
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] split = value.toString().split(",");
			String course = split[0];
			double avg = 0;
			int count = 0;
			int sum = 0;
			for(int i = 2;i<split.length;i++){
				count++;
				sum +=Integer.parseInt(split[i]);
			}
			avg = sum/(double)count;
			context.write(new Text(course), new DoubleWritable(avg));
			
		}
	}
	
	public static class MyCFReducer extends Reducer<Text, DoubleWritable, Text, Text>{
		@Override
		protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)	throws IOException, InterruptedException {
			int count = 0 ;
			double sumAvg = 0;
			double endAvg = 0;
			for(DoubleWritable avg:values){
				count++;
				sumAvg += avg.get();
			}
			endAvg = sumAvg/count;
			context.write(key, new Text(count+"\t"+endAvg));
		}
	}
	
}
