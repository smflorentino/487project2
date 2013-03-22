package org;
        
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.Map.Entry;
        
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.LineReader;

//import archive.TextPair;
//import archive.RelativeFrequencyPairs.LeftWordPartitioner;
        
public class RelativeFrequencyPairs {
        
	public static class Map extends Mapper<LongWritable, Text, TextPair, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		//private final static IntWritable none = new IntWritable(-1);
		private TextPair word = new TextPair();
		private String w;
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String n="";
			HashMap<String, Integer> hm = new HashMap<String, Integer>();
			//System.out.println(value.toString());
			String line = value.toString();
			String neighbors = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			StringTokenizer tokenizer2 = new StringTokenizer(neighbors);
			int wpos=0;
			int npos=0;
			HashMap<String, Integer> processedWords = new HashMap<String, Integer>();
			while(tokenizer.hasMoreTokens()) {
				w=tokenizer.nextToken();
				
				//In mapper combining
				if(hm.containsKey(w)) {
	        		hm.put(w, hm.get(w)+1);
	        	}
	        	else {
	        		hm.put(w,1);
	        	}
				
				//end in mapper combining
				
				
				while(tokenizer2.hasMoreTokens()) {
					n=tokenizer2.nextToken();
				//	System.out.println("Entering Nested While Loop. WPOS: " + wpos + " NPOS" + npos + "N:" + n + "W: " + w);
					if(npos!=wpos) { //this is our WORKING "neighbors" function
						if(n.equals(w)) {
							if(processedWords.get(n) == null) {
					//			System.out.println("Adding N:" + n +"," + "1");
								System.out.println("Emitting KV Pair: "+ w + "," + n);
								context.write(new TextPair(w,n),one);
								context.write(new TextPair(w,"*"), one);
							}
						} else {
							System.out.println("Emitting KV Pair: "+ w + "," + n);
							context.write(new TextPair(w,n),one);
							context.write(new TextPair(w,"*"), one);
						}
					}
					npos++;
				}
				processedWords.put(w, 1);
				npos=0;
				wpos++;
				tokenizer2=new StringTokenizer(neighbors);
			}
			
			//emit special pairs
			/*
			java.util.Map.Entry<String, Integer> pair;
	    	Iterator<Entry<String, Integer>> it = hm.entrySet().iterator();
	    	Text t; IntWritable i;
	    	while(it.hasNext()) {
	    		pair=it.next();
	    		t=new Text(pair.getKey());
	    		i = new IntWritable(pair.getValue());
	    		System.out.println("Emitting KV Pair: "+ t + ",* I:" + i);
	    		context.write(new TextPair(t, new Text("*")), i);
	    	}*/
	    	//end emit special map pairs

		}


	} 

	public static class TextPairComparator extends WritableComparator {
	    private static final String SPECIAL = "*";
	    private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();
	    
	    public TextPairComparator() {
	      super(TextPair.class);
	    }

	    @Override
	    public int compare(byte[] b1, int s1, int l1,
	                       byte[] b2, int s2, int l2) {
	      
	      try {
	        int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
	        int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
	        return TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
	      } catch (IOException e) {
	        throw new IllegalArgumentException(e);
	      }
	    }
	    
	    @Override
	    public int compare(WritableComparable a, WritableComparable b) {
	      if (a instanceof TextPair && b instanceof TextPair) {
	    	  TextPair A = (TextPair) a; TextPair B = (TextPair) b;
	    	  if(A.getFirst().compareTo(B.getFirst()) == 0) {
	    		  if(A.getSecond().equals(SPECIAL)) {
	    			  return -1;
	    		  }
	    		  else {
	    			  return 1;
	    		  }
	    	  } else {
	    		  return A.compareTo(B);
	    	  }
	    	  
	    	  
	        //return ((TextPair) a).first.compareTo(((TextPair) b).first);
	      }
	      return super.compare(a, b);
	    }
	  }
	
public static class LeftWordPartitioner extends Partitioner<TextPair, IntWritable> {

	public LeftWordPartitioner() {}
	@Override
	public int getPartition(TextPair key, IntWritable value, int numReduceTasks) {
		TextPair arg0 = key;
		Text sec = new Text(key.getFirst().toString());
		if(key.getSecond().charAt(0)== '*') {
			
			System.out.println("*Partitioning...:" + arg0.getFirst() + "," + arg0.getSecond() + " " + sec.hashCode());
			
			return (arg0.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
		}
		System.out.println("Partitioning...:" + arg0.getFirst() + "," + arg0.getSecond() + " "+ sec.hashCode());
		return (sec.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
	}
	 
 }
 public static class Reduce extends Reducer<TextPair, IntWritable, TextPair, FloatWritable> {
	 private static final char SPECIAL = '*';
	 private float _marginal = 0;
	 private float _sum=0;
	 
    public void reduce(TextPair key, Iterable<IntWritable> values, Context context) 
      throws IOException, InterruptedException {
       System.out.print("\nReducer Key: " + key.getFirst() + "," + key.getSecond());
       System.out.println("\nMarginal: " + _marginal + " Sum: " + _sum);
        if(key.getSecond().toString().charAt(0) == SPECIAL) {
        	_sum=0;
        	_marginal=0;
        	for (IntWritable val : values) {
                
        		System.out.print(" " + val.toString() + " ");
        		_marginal += val.get();
            }  
        	System.out.println("-");
        } else {
        	for (IntWritable val : values) {
        		System.out.print(" " + val.toString() + " ");
                _sum += val.get();
            }
          //  System.out.println("test");
            context.write(key, new FloatWritable(_sum/_marginal));
            //_marginal =0;
           _sum=0;
        }
        System.out.println("Marginal: " + _marginal + " Sum: " + _sum);
        System.out.println("\n\n------------------------------End Reducer-----------------------------");
    }
 }
        
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
        Job job = new Job(conf, "wordcount");
    
    //output of mappers
     job.setMapOutputKeyClass(TextPair.class);
     job.setMapOutputValueClass(IntWritable.class);
    //job.setGroupingComparatorClass(cls)
     // job.setSortComparatorClass(TextPairComparator.class);
     //output of reducers
   // job.setSortComparatorClass(TextPairComparator.class);
    job.setOutputKeyClass(TextPair.class);
    job.setOutputValueClass(FloatWritable.class);
    job.setPartitionerClass(LeftWordPartitioner.class);
    
    
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setInputFormatClass(ParagraphInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setJarByClass(RelativeFrequencyPairs.class);
    job.waitForCompletion(true);
 }
        
}

/**
 * @author scottflo
 * A class to store the output keys of the mappers/reducers
 * Adapted from: http://my.safaribooksonline.com/book/databases/hadoop/9780596521974/serialization/id3548156
 */
class TextPair implements WritableComparable<TextPair> {

	  private Text first;
	  private Text second;
	  
	  public TextPair() {
	    set(new Text(), new Text());
	  }
	  
	  public TextPair(String first, String second) {
	    set(new Text(first), new Text(second));
	  }
	  
	  public TextPair(Text first, Text second) {
	    set(first, second);
	  }
	  
	  public void set(Text first, Text second) {
	    this.first = first;
	    this.second = second;
	  }
	  
	  public Text getFirst() {
	    return first;
	  }

	  public Text getSecond() {
	    return second;
	  }

	  @Override
	  public void write(DataOutput out) throws IOException {
	    first.write(out);
	    second.write(out);
	  }

	  @Override
	  public void readFields(DataInput in) throws IOException {
	    first.readFields(in);
	    second.readFields(in);
	  }
	  
	  @Override
	  public int hashCode() {
	    return first.hashCode() * 163 + second.hashCode();
	  }
	  
	  @Override
	  public boolean equals(Object o) {
	    if (o instanceof TextPair) {
	      TextPair tp = (TextPair) o;
	      return first.equals(tp.first) && second.equals(tp.second);
	    }
	    return false;
	  }

	  @Override
	  public String toString() {
	    return first + "\t" + second;
	  }
	  
	  @Override
	  public int compareTo(TextPair tp) {
	    int cmp = first.compareTo(tp.first);
	    if (cmp != 0) {
	      return cmp;
	    }
	    return second.compareTo(tp.second);
	  }
	}




/** An {@link InputFormat} for plain text files.  Files are broken into lines.
 * Either linefeed or carriage-return are used to signal end of line.  Keys are
 * the position in the file, and values are the line of text.. */
class ParagraphInputFormat extends FileInputFormat<LongWritable, Text> {

  @Override
  public RecordReader<LongWritable, Text> 
    createRecordReader(InputSplit split,
                       TaskAttemptContext context) {
    return new ParagraphRecordReader();
  }

  @Override
  protected boolean isSplitable(JobContext context, Path file) {
    CompressionCodec codec = 
      new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
    return codec == null;
  }

}







/**
 * A class that provides a line reader from an input stream.
 */
class ParagraphReader {
  private static final int DEFAULT_BUFFER_SIZE = 64 * 1024;
  private int bufferSize = DEFAULT_BUFFER_SIZE;
  private InputStream in;
  private byte[] buffer;
  // the number of bytes of real data in the buffer
  private int bufferLength = 0;
  // the current position in the buffer
  private int bufferPosn = 0;

  private static final byte CR = '\r';
  private static final byte LF = '\n';

  /**
   * Create a line reader that reads from the given stream using the
   * default buffer-size (64k).
   * @param in The input stream
   * @throws IOException
   */
  public ParagraphReader(InputStream in) {
    this(in, DEFAULT_BUFFER_SIZE);
  }

  /**
   * Create a line reader that reads from the given stream using the 
   * given buffer-size.
   * @param in The input stream
   * @param bufferSize Size of the read buffer
   * @throws IOException
   */
  public ParagraphReader(InputStream in, int bufferSize) {
    this.in = in;
    this.bufferSize = bufferSize;
    this.buffer = new byte[this.bufferSize];
  }

  /**
   * Create a line reader that reads from the given stream using the
   * <code>io.file.buffer.size</code> specified in the given
   * <code>Configuration</code>.
   * @param in input stream
   * @param conf configuration
   * @throws IOException
   */
  public ParagraphReader(InputStream in, Configuration conf) throws IOException {
    this(in, conf.getInt("io.file.buffer.size", DEFAULT_BUFFER_SIZE));
  }

  /**
   * Close the underlying stream.
   * @throws IOException
   */
  public void close() throws IOException {
    in.close();
  }
  
  /**
   * Read one line from the InputStream into the given Text.  A line
   * can be terminated by one of the following: '\n' (LF) , '\r' (CR),
   * or '\r\n' (CR+LF).  EOF also terminates an otherwise unterminated
   * line.
   *
   * @param str the object to store the given line (without newline)
   * @param maxLineLength the maximum number of bytes to store into str;
   *  the rest of the line is silently discarded.
   * @param maxBytesToConsume the maximum number of bytes to consume
   *  in this call.  This is only a hint, because if the line cross
   *  this threshold, we allow it to happen.  It can overshoot
   *  potentially by as much as one buffer length.
   *
   * @return the number of bytes read including the (longest) newline
   * found.
   *
   * @throws IOException if the underlying stream throws
   */
  public int readLine(Text str, int maxLineLength,
                      int maxBytesToConsume) throws IOException {
	  boolean foundFirstLF = false;
    /* We're reading data from in, but the head of the stream may be
     * already buffered in buffer, so we have several cases:
     * 1. No newline characters are in the buffer, so we need to copy
     *    everything and read another buffer from the stream.
     * 2. An unambiguously terminated line is in buffer, so we just
     *    copy to str.
     * 3. Ambiguously terminated line is in buffer, i.e. buffer ends
     *    in CR.  In this case we copy everything up to CR to str, but
     *    we also need to see what follows CR: if it's LF, then we
     *    need consume LF as well, so next call to readLine will read
     *    from after that.
     * We use a flag prevCharCR to signal if previous character was CR
     * and, if it happens to be at the end of the buffer, delay
     * consuming it until we have a chance to look at the char that
     * follows.
     */
    str.clear();
    int txtLength = 0; //tracks str.getLength(), as an optimization
    int newlineLength = 0; //length of terminating newline
    boolean prevCharCR = false; //true of prev char was CR
    long bytesConsumed = 0;
    do {
      int startPosn = bufferPosn; //starting from where we left off the last time
      if (bufferPosn >= bufferLength) {
        startPosn = bufferPosn = 0;
        if (prevCharCR)
          ++bytesConsumed; //account for CR from previous read
        bufferLength = in.read(buffer);
        if (bufferLength <= 0)
          break; // EOF
      }
      for (; bufferPosn < bufferLength; ++bufferPosn) { //search for newline
    	if(buffer[bufferPosn] == LF && !foundFirstLF) {
    		foundFirstLF=true;
    	}
    	else if (buffer[bufferPosn] == LF && foundFirstLF) {
          newlineLength=2;
    	  //newlineLength = (prevCharCR) ? 2 : 1;
          ++bufferPosn; // at next invocation proceed from following byte
          break;
        }
    	else if(buffer[bufferPosn] != LF && foundFirstLF) {
    		//we only found 1 newline, replace it with a space
    		if(bufferPosn>0)
    			buffer[bufferPosn-1] = ' ';
    		foundFirstLF=false;
    	}
        if (prevCharCR) { //CR + notLF, we are at notLF
          newlineLength = 1;
          break;
        }
        prevCharCR = (buffer[bufferPosn] == CR);
      }
      int readLength = bufferPosn - startPosn;
      if (prevCharCR && newlineLength == 0)
        --readLength; //CR at the end of the buffer
      bytesConsumed += readLength;
      int appendLength = readLength - newlineLength;
      if (appendLength > maxLineLength - txtLength) {
        appendLength = maxLineLength - txtLength;
      }
      if (appendLength > 0) {
        str.append(buffer, startPosn, appendLength);
        txtLength += appendLength;
      } 
    } while (newlineLength == 0 && bytesConsumed < maxBytesToConsume);

    if (bytesConsumed > (long)Integer.MAX_VALUE)
      throw new IOException("Too many bytes before newline: " + bytesConsumed);    
    return (int)bytesConsumed;
  }

  /**
   * Read from the InputStream into the given Text.
   * @param str the object to store the given line
   * @param maxLineLength the maximum number of bytes to store into str.
   * @return the number of bytes read including the newline
   * @throws IOException if the underlying stream throws
   */
  public int readLine(Text str, int maxLineLength) throws IOException {
    return readLine(str, maxLineLength, Integer.MAX_VALUE);
}

  /**
   * Read from the InputStream into the given Text.
   * @param str the object to store the given line
   * @return the number of bytes read including the newline
   * @throws IOException if the underlying stream throws
   */
  public int readLine(Text str) throws IOException {
    return readLine(str, Integer.MAX_VALUE, Integer.MAX_VALUE);
  }

}












/**
 * Treats keys as offset in file and value as line. 
 */
class ParagraphRecordReader extends RecordReader<LongWritable, Text> {
  private static final Log LOG = LogFactory.getLog(ParagraphRecordReader.class);

  private CompressionCodecFactory compressionCodecs = null;
  private long start;
  private long pos;
  private long end;
  private ParagraphReader in;
  private int maxLineLength;
  private LongWritable key = null;
  private Text value = null;

  public void initialize(InputSplit genericSplit,
                         TaskAttemptContext context) throws IOException {
    FileSplit split = (FileSplit) genericSplit;
    Configuration job = context.getConfiguration();
    this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength",
                                    Integer.MAX_VALUE);
    start = split.getStart();
    end = start + split.getLength();
    final Path file = split.getPath();
    compressionCodecs = new CompressionCodecFactory(job);
    final CompressionCodec codec = compressionCodecs.getCodec(file);

    // open the file and seek to the start of the split
    FileSystem fs = file.getFileSystem(job);
    FSDataInputStream fileIn = fs.open(split.getPath());
    boolean skipFirstLine = false;
    if (codec != null) {
      in = new ParagraphReader(codec.createInputStream(fileIn), job);
      end = Long.MAX_VALUE;
    } else {
      if (start != 0) {
        skipFirstLine = true;
        --start;
        fileIn.seek(start);
      }
      in = new ParagraphReader(fileIn, job);
    }
    if (skipFirstLine) {  // skip first line and re-establish "start".
      start += in.readLine(new Text(), 0,
                           (int)Math.min((long)Integer.MAX_VALUE, end - start));
    }
    this.pos = start;
  }
  
  public boolean nextKeyValue() throws IOException {
    if (key == null) {
      key = new LongWritable();
    }
    key.set(pos);
    if (value == null) {
      value = new Text();
    }
    int newSize = 0;
    while (pos < end) {
      newSize = in.readLine(value, maxLineLength,
                            Math.max((int)Math.min(Integer.MAX_VALUE, end-pos),
                                     maxLineLength));
      if (newSize == 0) {
        break;
      }
      pos += newSize;
      if (newSize < maxLineLength) {
        break;
      }

      // line too long. try again
      LOG.info("Skipped line of size " + newSize + " at pos " + 
               (pos - newSize));
    }
    if (newSize == 0) {
      key = null;
      value = null;
      return false;
    } else {
      return true;
    }
  }

  @Override
  public LongWritable getCurrentKey() {
    return key;
  }

  @Override
  public Text getCurrentValue() {
    return value;
  }

  /**
   * Get the progress within the split
   */
  public float getProgress() {
    if (start == end) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (pos - start) / (float)(end - start));
    }
  }
  
  public synchronized void close() throws IOException {
    if (in != null) {
      in.close(); 
    }
  }
}