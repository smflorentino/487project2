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

/**
 * Class for Vector/Centroid Objects
 * An instance of this class can be a Vector OR a Centroid
 * @author Scott
 *
 */
public class VectorWritable implements WritableComparable<VectorWritable> {

	private IntWritable _data; //data value of the vector/centroid
	private IntWritable _centroid; //centroid of the vector
	private BooleanWritable _center; //whether or not this object is a centroid

	/**
	 * Default Constructor for VectorWritable
	 */
	public VectorWritable() {
		_data = new IntWritable();
		_center= new BooleanWritable();
		_centroid = new IntWritable();
	}
	
	/**
	 * Construct a Data Point (1-Dimensional) (initial pass)
	 * @param i The Datapoint Value
	 */
	public VectorWritable(int i) {
		_data = new IntWritable(i);
		_center = new BooleanWritable(false);
		_centroid = new IntWritable();
	}
	
	
	/**
	 * Construct a Centroid 
	 * @param i The Centroid Value
	 * @param b Whether or not this Vector is a Centroid (should always be true)
	 */
	public VectorWritable(int i, boolean b) {
		_data = new IntWritable(i);
		_center = new BooleanWritable(b);
		_centroid = new IntWritable();
	}
	
	/**
	 * Construct a Data Point (1-Dimensional) (pass >=1)
	 * @param i The Datapoint Value 
	 * @param c The Datapoint's Centroid's Value
	 */
	public VectorWritable(int i, int c) {
		_data = new IntWritable(i);
		_center = new BooleanWritable(false);
		_centroid=new IntWritable(c);
	}
	
	/**
	 * Read method for the Writable Object
	 * @param in
	 * @throws IOException
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		_data.readFields(in);
		_center.readFields(in);
	}

	/**
	 * Write Method for the Writable Object
	 * @param out
	 * @throws IOException
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		_data.write(out);
		_center.write(out);
		
	}
	
	/**
	 * Return the value of this Datapoint/Centroid
	 * @return
	 */
	public int get() {
		return _data.get();
	}
	
	/**
	 * Set the centroid of the current vector
	 * @param c The centroid to be set
	 * @return whether or not the centroid of the current vector CHANGED
	 */
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
	
	/**
	 * Compare method for other Vectors/Centroids (used in Hadoop Sorting)
	 * @param v1
	 * @return
	 */
	@Override
	public int compareTo(VectorWritable v1) {
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
	
	/**
	 * Return the String Representation of this Vector
	 * @return
	 */
	public String toString() {
		return _data.toString();
	}
	
	/**
	 * Create a new Centroid from the String passed in from the centers.txt file
	 * @param s1
	 * @return a new Centroid object
	 */
	public static VectorWritable parseCentroid(String s) {
		return new VectorWritable(Integer.parseInt(s));
	}
	
	/**
	 * Construct a new Vector from the String passed in from the input files to MR
	 * @param s
	 * @return a new Vector object
	 */
	public static VectorWritable parseVector(String s) {
		System.out.println("Parsing: " + s);
		StringTokenizer st = new StringTokenizer(s);
		String current=null;
		String current2=null;
		while(st.hasMoreTokens()) {
			current = st.nextToken();
			//we may have ONE more token, the assigned centroid for that datapoint
			try {
				//all other iterations we have a cluster assigned, we parse starting from the character index 1 to avoid the 'c' in 'c21' 
				current2 = st.nextToken();
				return new VectorWritable(Integer.parseInt(current), Integer.parseInt(current2)); 
			}
			catch (NoSuchElementException e) {
				//this will happen for the FIRST iteration only, when no cluster is assigned to the Vector
				return new VectorWritable(Integer.parseInt(current));
			}

		}
		
		//string with no Tokens, return null
		return null;
	}

}
