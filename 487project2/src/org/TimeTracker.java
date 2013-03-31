package org;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TimeTracker {
	FileWriter _fstream;
	BufferedWriter _out;
	DateFormat _df;
	private long _startTime;
	private long _endTime;
	public TimeTracker() {
		
	}
	public void writeStartTime() {
		try {
			_fstream = new FileWriter("jobtime.txt");
			_out = new BufferedWriter(_fstream);
			_df = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
			Date start = new Date();
			_startTime=start.getTime();
				_out.append("Starting MR Job: " + _df.format(start)+"\n");
				System.out.println("**********Start: " + _df.format(start));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	
	}
	
	public void writeEndTime() {
		try{
			Date end = new Date();
			_endTime=end.getTime();
			long totaltime = (_endTime - _startTime) / 1000;
		_out.append("Ending MR Job: " + _df.format(end)+"\n" + "Total seconds taken: " + totaltime + "\n");
		System.out.println("**********End: " + _df.format(end));
		_out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/*public static void main(String[] args) {
		TimeTracker tt = new TimeTracker();
		tt.writeStartTime();
		tt.writeEndTime();
	}*/
}
