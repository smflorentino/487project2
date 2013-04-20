package kmeans;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;

import kmeans.VectorWritable;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class VectorWritable implements WritableComparable<VectorWritable> {

	private IntWritable _data;
	private IntWritable _centroid;
	private BooleanWritable _center;

	public VectorWritable() {
		_data = new IntWritable();
		_center= new BooleanWritable();
		_centroid = new IntWritable();
	}
	/**
	 * Construct a Data Point (1-Dimensional) (initial pass)
	 * @param i
	 */
	public VectorWritable(int i) {
		_data = new IntWritable(i);
		_center = new BooleanWritable(false);
		_centroid = new IntWritable();
	}
	
	
	/**
	 * Construct a Centroid 
	 * @param i
	 * @param b
	 */
	public VectorWritable(int i, boolean b) {
		_data = new IntWritable(i);
		_center = new BooleanWritable(b);
		_centroid = new IntWritable();
	}
	
	/**
	 * Construct a Data Point (1-Dimensional) (pass >=1)
	 * @param i
	 * @param b
	 */
	public VectorWritable(int i, int c) {
		_data = new IntWritable(i);
		_center = new BooleanWritable(false);
		_centroid=new IntWritable(c);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		_data.readFields(in);
		_center.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		_data.write(out);
		_center.write(out);
		
	}
	
	public int get() {
		return _data.get();
	}
	
	public boolean setCentroid(VectorWritable c) {
		if(_centroid.get()!=c.get()) {
			//centroid has changed
			_centroid.set(c.get());
			return true;
		} else {
			//no changes made
			return false;
		}
		
	}
	@Override
	public int compareTo(VectorWritable v1) {
		/*if(this.isCentroid() && !v1.isCentroid()) {
			return -1;
		}
		//modify the sort order, process centroids first.
		else if(!this.isCentroid() && v1.isCentroid()) {
			return 1;
		}*/
		
		
		if(this.get() < v1.get()) {
			return -1;
		}
		else if(this.get() > v1.get()) {
			return 1;
		}
		return 0;
	}
	
	public boolean isCentroid() {
		return _center.get();
	}
	
	public int getCentroid() {
		return _centroid.get();
	}

	public void setCentroid(int i) {
		_centroid.set(i);
	}
	
	public String toString() {
		if(_center.get()) {
			return "c" + _data.toString();
		}
		return _data.toString();
	}
	
	
	public boolean clusterEquals(VectorWritable v) {
		return _centroid.get() == v.getCentroid();
	}
	
	public static VectorWritable parseVector(String s) {
		System.out.println("Parsing: " + s);
		StringTokenizer st = new StringTokenizer(s);
		String current=null;
		String current2=null;
		while(st.hasMoreTokens()) {
			current = st.nextToken();
			if(current.startsWith("c")) {
				//System.out.println("Parsing Centroid:" + current);
				//found a centroid, we don't care about the second token
				return new VectorWritable(Integer.parseInt(current.substring(1)),true);
			}
			else {
				//we have ONE more token, the assigned centroid for that datapoint
				try {
					//all other iterations we have a cluster assigned, we parse starting from the character index 1 to avoid the 'c' in 'c21' 
					current2 = st.nextToken();
					//System.out.println("Parsing Datapoint...Current: " + current + "Current 2:" + current2);
					return new VectorWritable(Integer.parseInt(current), Integer.parseInt(current2.substring(1))); 
				}
				catch (NoSuchElementException e) {
					//this will happen for the FIRST iteration only, when no cluster is assigned
					return new VectorWritable(Integer.parseInt(current));
				}
			}
		}
		//malformed input string, return null
		return null;
	}
	
	public static String parsePoint(String s) {
		return "";
	}
}
