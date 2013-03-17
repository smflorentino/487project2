package org;

import java.io.IOException;
import java.util.*;

import org.TwitterCounter.Map;
import org.TwitterCounter.Reduce;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class TwitterCounter {
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		
/**Mapper method
 * @author Alyssa
 * 
 * @param key - the file name
 * @param value - the whole file as Text object
 * @param output - the OutputCollecter used for this job
 * @param reporter 
 */
		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			//tokenize value (whole file) into individual tweets
			//then tokenize each tweet for word count
			String data = value.toString();
			String lines[] = data.split("[\\r\\n]+");
			String tweet;
			StringTokenizer stringTokenizer;
			TweetTokenizer tweetTokenizer = new TweetTokenizer();
			//while (tweetTokenizer.hasMoreElements()) {
			for(int i=0; i<lines.length;i++){
				tweet = tweetTokenizer.getTweet(lines[i]);
				if(tweet.length()>0){
					stringTokenizer = new StringTokenizer(tweet);
					while (stringTokenizer.hasMoreTokens()) {
						word.set(stringTokenizer.nextToken());
						output.collect(word, one);
					}
				}
			}
		}
	}


/**Reducer class
 */
	public static class Reduce extends MapReduceBase 
	implements Reducer<Text, IntWritable, Text, IntWritable> {
		
/**Reducer method
 * @param key - the key being reduced
 * @param values -an iterator over the values associated with that key
 * @param output
 * @param reporter
 * 		
 */
		public void reduce(Text key, Iterator<IntWritable> values, 
				OutputCollector<Text, IntWritable> output, Reporter reporter) 
						throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));

		}
	}
	
/**Begins the job
 * 
 * @author Scott
 * 
 * @param args
 * @throws Exception
 */
	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(TwitterCounter.class);
		conf.setJobName("twittercounter");
		conf.setJarByClass(TwitterCounter.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		//input/output paths given in args
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
	}

/**Class to handle tokenizing of the tweet data
 */
	public static class TweetTokenizer {
		
/**Get the tweet from a line of input
 * @author Alyssa
 * 
 * @param line - the line of the input file containing the tweet
 * @return the tweet 
 */
		public String getTweet(String line){
			for(int i=0; i<line.length()-4;i++){
				if(line.charAt(i)=='<'&&line.charAt(i+1)=='t'&&line.charAt(i+2)=='t'
						&&line.charAt(i+3)=='>'){
					//found opening tag <tt>
					for(int j=i+4;j<line.length()-5;j++){
						if(line.charAt(j)=='<'&&line.charAt(j+1)=='/'&&
								line.charAt(j+2)=='t'&&line.charAt(j+3)=='t'&&
								line.charAt(j+4)=='>'){
							//found closing tag </tt>
							return line.substring(i+4,j).toLowerCase();
						}
					}
				}
			}
			return "";
		}
		
	/**Get all mentions (using @) from the given tweet
	 * @author Alyssa
	 * 
	 * @param tweetText - a given tweet
	 * @return all of the mentions within that tweet
	 */
		public ArrayList<String> getMentions(String tweetText){
			ArrayList<String> mentions = new ArrayList<String>();
			for(int i=0; i<tweetText.length();i++){
				//find @
				if(tweetText.charAt(i)==64){
					//get the rest of the mention
					for(int j=i+1;j<tweetText.length();j++){
						//if space, colon or end of the tweet
						if(tweetText.charAt(j)==32|| tweetText.charAt(j)==58 || j==tweetText.length()-1){
							mentions.add(tweetText.substring(i,j));
							i=j;
							j=tweetText.length();
						}
					}
				}
			}
			return mentions;
		}
/**Get all tags (using #) from a given tweet
 * @author Alyssa
 * 
 * @param tweetText - the given tweet
 * @return all of the tags in the tweet
 */
		public ArrayList<String> getTags(String tweetText){
			ArrayList<String> tags = new ArrayList<String>();
			for(int i=0; i<tweetText.length();i++){
				//find #
				if(tweetText.charAt(i)==35){
					//get the rest of the tag
					for(int j=i+1;j<tweetText.length();j++){
						//if space or end of the tweet, complete the tag
						if(tweetText.charAt(j)==32|| j==tweetText.length()-1){
							tags.add(tweetText.substring(i,j));
							i=j;
							j=tweetText.length();
						}
					}
				}
			}
			return tags;
		}
		
	/**Get the number of words in a given tweet
	 * @author Alyssa
	 * 
	 * @param tweet - the given tweet
	 * @return the number of words in the given tweet
	 */
		public int getTweetWordCount(String tweet){
			StringTokenizer stringTokenizer = new StringTokenizer(tweet);
			int count = 0;
			while (stringTokenizer.hasMoreTokens()) {
				stringTokenizer.nextToken();
				count++;
			}
			return count;
		}

	}
}