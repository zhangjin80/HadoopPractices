package practice02.pojo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * algorithm,huangzitao,85,86,41,75,93,42,85,75
 */
public class Course implements WritableComparable<Course>{
	private String name;
	private String student;
	private double average;
	
	public Course() {
	}
	
	public Course(String name, String student, double average) {
		super();
		this.name = name;
		this.student = student;
		this.average = average;
	}
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getStudent() {
		return student;
	}
	public void setStudent(String student) {
		this.student = student;
	}
	public double getAverage() {
		return average;
	}
	public void setAverage(double average) {
		this.average = average;
	}
	
	
	@Override
	public String toString() {
		return name + "\t" + student + "\t" + average;
	}

	/**
	 * 序列化输出
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(name);
		out.writeUTF(student);
		out.writeDouble(average);
	}
	/**
	 * 反序列化
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		name = in.readUTF();
		student = in.readUTF();
		average = in.readDouble();
	}

	@Override
	public int compareTo(Course o) {
		return (int) (o.average-this.average);
	}

}
