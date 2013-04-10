package org;
//4-10-2013
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
        
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
        
public class CooccurancePairs {
        
 public static class Map extends Mapper<LongWritable, Text, TextPair, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private TextPair word = new TextPair();
    private String w;
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	String n="";
    	//System.out.println(value.toString));
        String line = value.toString();
        String neighbors = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        StringTokenizer tokenizer2 = new StringTokenizer(neighbors);
        int wpos=0;
        int npos=0;
        HashMap<String, Integer> processedWords = new HashMap<String, Integer>();
        while(tokenizer.hasMoreTokens()) {
    	w=tokenizer.nextToken();
    	while(tokenizer2.hasMoreTokens()) {
    		n=tokenizer2.nextToken();
    		//System.out.println("Entering Nested While Loop. WPOS: " + wpos + " NPOS" + npos + "N:" + n + "W: " + w);
    		if(npos!=wpos) { //this is our WORKING "neighbors" function
    			if(n.equals(w)) {
    				if(processedWords.get(n) == null) {
    					//System.out.println("Adding N:" + n +"," + "1");
    					context.write(new TextPair(w,n),one);
    				}
    			} else {
    				context.write(new TextPair(w,n),one);
    			}
    		}
    		npos++;
    	}
    	processedWords.put(w, 1);
    	npos=0;
    	wpos++;
    	tokenizer2=new StringTokenizer(neighbors);
    }
        
        
        /*while(tokenizer.hasMoreTokens()) {
        	w=tokenizer.nextToken();
        	while(tokenizer2.hasMoreTokens()) {
        		n=tokenizer2.nextToken();
        		System.out.println("Entering Nested While Loop. WPOS: " + wpos + " NPOS" + npos + "N:" + n + "W: " + w);
        		if(npos!=wpos) { //this is our "neighbors" function
        			if(wpos >0 && !(w.equals(n))) {
        				context.write(new TextPair(w,n),one);
        				System.out.println("if1");//"WPOS: " + wpos + " NPOS: " + npos + w.toString() + "," + n.toString());
        			}
        			else if(wpos==0) {
        				context.write(new TextPair(w,n), one);
        				System.out.println("if2");//"WPOS: " + wpos + " NPOS: " + npos + w.toString() + "," + n.toString());
            		}
        		}
        		npos++;
        	}
        	npos=0;
        	wpos++;
        	tokenizer2=new StringTokenizer(neighbors);
        }*/
        
        /*boolean foundItself=false;
         
        while (tokenizer.hasMoreTokens()) {
        	w=tokenizer.nextToken();
           while(tokenizer2.hasMoreTokens()) {
        	   n=tokenizer2.nextToken();
        	   if(w.equals(n)) {
        		   if(!foundItself) {
        			   foundItself=true;
        		   } else {
        			   word= new TextPair(w,n);
        			   context.write(word, one);
        		   }
        	   } 
        	   else {
        		   word = new TextPair(w, n);
            	   context.write(word, one);
        	   }
        	   
           }
           tokenizer2= new StringTokenizer(neighbors);
           foundItself=false;
        }*/
    }
    
   
 } 
 
public static class LeftWordPartitioner extends Partitioner<TextPair, IntWritable> {

	public LeftWordPartitioner() {}
	@Override
	public int getPartition(TextPair arg0, IntWritable arg1, int numReduceTasks) {
		return (arg0.getFirst().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
	}
	 
 }
 public static class Reduce extends Reducer<TextPair, IntWritable, TextPair, IntWritable> {

    public void reduce(TextPair key, Iterable<IntWritable> values, Context context) 
      throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        //System.out.println("test");
        context.write(key, new IntWritable(sum));
    }
 }
        
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
        Job job = new Job(conf, "wordcount");
    
    job.setOutputKeyClass(TextPair.class);
    job.setOutputValueClass(IntWritable.class);
        
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setPartitionerClass(LeftWordPartitioner.class);
    job.setInputFormatClass(ParagraphInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setJarByClass(CooccurancePairs.class);
    TimeTracker tt = new TimeTracker();
    tt.writeStartTime();
    job.waitForCompletion(true);
    tt.writeEndTime();
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












/**ee
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