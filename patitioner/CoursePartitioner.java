package practice02.patitioner;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import practice02.pojo.Course;

public class CoursePartitioner extends Partitioner<Course, NullWritable>{

	@Override
	public int getPartition(Course key, NullWritable value, int arg2) {
		String cname = key.getName();
		if(cname.equals("computer"))
			return 0;
		else if(cname.equals("english"))
			return 1;
		else if(cname.equals("algorithm"))
			return 2;
		else return 3;
	}

}
