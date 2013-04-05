package org;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Scanner;
import java.util.StringTokenizer;

import javax.swing.JOptionPane;

/**
 * 
 * @author scottflo
 * Removes special characters from tweets, effectively filtering out non-english words
 */
public class FileCleaner {
	
	private static HashMap<String, Integer> STOPWORDS = new HashMap<String, Integer>();
	public static void main(String[] args) {
		FileCleaner f = new FileCleaner();
		try {
			f.start();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void start() throws IOException {
		String fileName = JOptionPane.showInputDialog("Enter BASE File name to clean: (i.e. for tweets.log.20 enter tweets.log)");
		int start = Integer.parseInt(JOptionPane.showInputDialog("Enter STARTING log file extension (i.e. for .1 enter 1"));
		int end = Integer.parseInt(JOptionPane.showInputDialog("Enter ENDING log file extension (i.e. for .20 enter 20"));
		for(int i=start;i<=end;i++) {
			System.out.println("Processing file " + i + "...");
			this.startCleaning(fileName + "." + i);
		}
		JOptionPane.showMessageDialog(null, "Processing Complete!");
	}
	private void startCleaning(String fileName) throws IOException {
		this.getStopWords();
		//String fileName = JOptionPane.showInputDialog("Enter file name to clean");
		BufferedReader sc = new BufferedReader(new FileReader(fileName));
		FileWriter fs=  new FileWriter(fileName+"clean.txt");
		BufferedWriter out = new BufferedWriter(fs);
		
		String line;
		String line2;
		String line3;
		int linenum=0;
		while(true) {
			//System.out.println("Line:" + linenum);
			line=sc.readLine();
			if(line == null) {
				break;
			}
			if(this.isValidLine(line)) {
				line2 = "";
				char c;
				for(int i=0;i<line.length();i++) {
					c=line.charAt(i);
					if(isAlphaNumericOrWhiteSpace(Character.toLowerCase(c))) {
						line2+=c;
					}
				}
				//non alphanumeric/whitespace characters removed, now remove stop words:
				
				line3=this.removeStopWords(line2);
				out.write(line3+'\n');
			}
			else {
			//	System.out.println("Found invalid line" + line);
			}
			linenum++;
		}
		out.close();
	}
	
	private boolean isValidLine(String s) {
		char c;
		for(int i=0;i<s.length();i++) {
			c=s.charAt(i);
			if(!(this.isValidCharacter(c))) {
				
				return false;
			}
		}
		//System.out.println("found valid line:");
		return true;
	}
	
	private boolean isValidCharacter(char c) {
		//.out.println(c+0);
		if(c >=0 && c<=126) {
			//System.out.println("Special");
			return true;
		}
		return false;
	}
	
	private String removeStopWords(String line) {
		String ret="";
		char c;
		String word="";
		for(int i =0;i<line.length();i++) {
			c=line.charAt(i);
			if(this.isWhiteSpaceOrNumberic(c)) {
				//JOptionPane.showMessageDialog(null, word);	
				if(word.length() > 0 && STOPWORDS.get(word) == null) {
					ret+=word;
					ret+=c;
				}
				word="";
				//ret+=c;
			} else {
				word+=c;
			}
		}
		
		return ret;
	}
	
	public void getStopWords() {
		  STOPWORDS=new HashMap<String,Integer>();
		  BufferedReader sc = null;
		//sc = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream("/config/stopwords.txt")));
		try {
			sc = new BufferedReader(new FileReader("config/stopwords.txt"));
		} catch (FileNotFoundException e1) {
			JOptionPane.showMessageDialog(null, "Cannot find stopwords.txt");
			e1.printStackTrace();
		}

		  String line="";
		  String[] words;
		  while(true) {
			  //System.out.println("Line:" + linenum);
			  try {
				  line=sc.readLine();
			  } catch (IOException e) {
				  e.printStackTrace();
			  }
			  if(line == null) {
				  break;
			  } 
			  words = line.split(",");
			  for(int i =0;i<words.length;i++) {
				  System.out.println("Adding stop word:" + words[i]);
				  STOPWORDS.put(words[i],0);
			  }
		  }
	  }
	private boolean isAlphaNumericOrWhiteSpace(char c) {
		return (c>='a' && c<='z') || (c>='0' && c<='9') || (c==' '|| c== '\n' || c=='\t');
	}
	
	private boolean isWhiteSpaceOrNumberic(char c) {
		return (c==' ' || c=='\n' || c=='\t') || (c>='0' && c<='9');
	}
}
