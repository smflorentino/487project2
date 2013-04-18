package kmeans;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class VectorWritable implements WritableComparable<VectorWritable> {

	private IntWritable _data;
	private boolean _center;
	
	/**
	 * Construct a Data Point (1-Dimensional)
	 * @param i
	 */
	public VectorWritable(int i) {
		_data = new IntWritable(i);
		_center = false;
	}
	
	
	/**
	 * Construct a Centroid
	 * @param i
	 * @param b
	 */
	public VectorWritable(int i, boolean b) {
		_data = new IntWritable(i);
		_center = b;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		_data.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		_data.write(out);
		
	}
	
	public int get() {
		return _data.get();
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
		return _center;
	}

	public String toString() {
		if(_center) {
			return "c" + _data.toString();
		}
		return _data.toString();
	}
}
