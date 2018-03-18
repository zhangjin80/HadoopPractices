package practice02.pojo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * algorithm,huangzitao,85,86,41,75,93,42,85,75
 */
public class Course2 implements WritableComparable<Course2>{
	private String name;
	private String student;
	private double average;
	private double maxScore;
	
	public Course2() {
	}
	
	
	public Course2(String name, String student, double average, double maxScore) {
		super();
		this.name = name;
		this.student = student;
		this.average = average;
		this.maxScore = maxScore;
	}

	public String getName() {
		return name;
	}


	public Course2 setName(String name) {
		this.name = name;
		return this;
	}


	public String getStudent() {
		return student;
	}


	public Course2 setStudent(String student) {
		this.student = student;
		return this;
	}


	public double getAverage() {
		return average;
	}


	public Course2 setAverage(double average) {
		this.average = average;
		return this;
	}


	public double getMaxScore() {
		return maxScore;
	}


	public Course2 setMaxScore(double maxScore) {
		this.maxScore = maxScore;
		return this;
	}


	@Override
	public String toString() {
		return name + "\t" + student + "\t" + average+"\t"+maxScore;
	}

	/**
	 * 序列化输出
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(name);
		out.writeUTF(student);
		out.writeDouble(average);
		out.writeDouble(maxScore);
	}
	/**
	 * 反序列化
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		name = in.readUTF();
		student = in.readUTF();
		average = in.readDouble();
		maxScore = in.readDouble();
	}

	@Override
	public int compareTo(Course2 o) {
		return (int) (o.average-this.average);
	}

}
