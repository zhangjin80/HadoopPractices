package practice02.gc;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import practice02.pojo.CourseScore;

public class ClazzScoreGroupComparator extends WritableComparator {
	//默认构造器需要调用父类的构造方法
	public ClazzScoreGroupComparator() {
		super(CourseScore.class,true);
	}
	
	//实现分组比较器根据课程名分组就比较课程名
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		CourseScore cs1 = (CourseScore)a;
		CourseScore cs2 = (CourseScore)b;
		return cs1.getCourseName().compareTo(cs2.getCourseName());
	}
}
