package graphs;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

//TODO: change so that keys are node ids and values are array writable representations of the node

/**
 * Treats keys as nodeId and value as array representing the node. 
 */
class GraphRecordReader extends RecordReader<LongWritable, ArrayWritable> {
  private static final Log LOG = LogFactory.getLog(GraphRecordReader.class);

  private CompressionCodecFactory compressionCodecs = null;
  private long start;
  private long pos;
  private long end;
  private LineReader in;
  private int maxLineLength;
  private LongWritable key = null;
  private Text currentLine = null;
  private ArrayWritable value = null;
  
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
      in = new LineReader(codec.createInputStream(fileIn), job);
      end = Long.MAX_VALUE;
    } else {
      if (start != 0) {
        skipFirstLine = true;
        --start;
        fileIn.seek(start);
      }
      in = new LineReader(fileIn, job);
    }
    if (skipFirstLine) {  // skip first line and re-establish "start".
      start += in.readLine(new Text(), 0,
                           (int)Math.min((long)Integer.MAX_VALUE, end - start));
    }
    this.pos = start;
  }
  
  //TODO: change to give correct keys and values
  public boolean nextKeyValue() throws IOException {
    if (key == null) {
      key = new LongWritable();
    }
//    key.set(pos); old from when key was set to the position in the file
    if (currentLine == null) {
      currentLine = new Text();
    }
    int newSize = 0;
    while (pos < end) {
      newSize = in.readLine(currentLine, maxLineLength,
                            Math.max((int)Math.min(Integer.MAX_VALUE, end-pos),
                                     maxLineLength));
      key.set(this.parseLineForNodeId(currentLine));//have key be nodeId
      value = this.parseLineForNodeArray(currentLine);
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
  public ArrayWritable getCurrentValue() {
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
  /**Get the nodeId from the line
   * 
   * @param cl - the current line
   * @return the id of the node represented by this line
   */
  public long parseLineForNodeId(Text cl){
	  String s = cl.toString().substring(0, 0);
	  String nodeIdString ="";
	  for(int i=0; i<s.length();i++){
		  if(s.charAt(i)=='\t'){
			  i=s.length()+1;
		  }else{
			  nodeIdString = nodeIdString + s.charAt(i);
		  }
	  }
	  
	  Integer nodeId = Integer.parseInt(nodeIdString);
	  
	  return (long) nodeId;
  }
  
  /**Get the node array from the line
   * @author Alyssa
   * @param cl - the current line
   * @return an array representation of the node
   */
  private ArrayWritable parseLineForNodeArray(Text cl) {
	  String s = cl.toString();
	  String[] nodeArray = s.split("\t");
	  return new ArrayWritable(nodeArray);
  }
}