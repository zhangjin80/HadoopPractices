package practice02.pojo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class CourseScore implements WritableComparable<CourseScore>{
	private String courseName;
	private String studentName;
	private double avgScore;
	public CourseScore() {
	}
	//根据最后的输出格式实现toString方法
	@Override
	public String toString() {
		return courseName + "\t" + studentName + "\t" + avgScore;
	}
	//对属性序列化
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(courseName);
		out.writeUTF(studentName);
		out.writeDouble(avgScore);
	}
	//反序列化
	@Override
	public void readFields(DataInput in) throws IOException {
		this.courseName = in.readUTF();
		this.studentName = in.readUTF();
		this.avgScore = in.readDouble();
	}
	//实现比较器
	@Override
	public int compareTo(CourseScore o) {
		//先比较课程名（分组的属性在前比较）
		int result = o.courseName.compareTo(this.courseName);
		//课程名相同，进行平均分的比较
		if(result==0){
			double temp = o.avgScore-this.avgScore;
			if(temp==0){
				return 0;
			}else{
				return temp>0?1:-1;
			}
		}else{
			return result;
		}
	}
	
	public String getCourseName() {
		return courseName;
	}
	public void setCourseName(String courseName) {
		this.courseName = courseName;
	}
	public String getStudentName() {
		return studentName;
	}
	public void setStudentName(String studentName) {
		this.studentName = studentName;
	}
	public double getAvgScore() {
		return avgScore;
	}
	public void setAvgScore(double avgScore) {
		this.avgScore = avgScore;
	}

}
