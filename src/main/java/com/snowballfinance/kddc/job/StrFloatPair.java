package com.snowballfinance.kddc.job;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;

import org.apache.hadoop.io.WritableComparable;

@SuppressWarnings("rawtypes")
public class StrFloatPair implements WritableComparable {

	private String candidate;
	private float score;
	
	// required methods
	public StrFloatPair() {
		candidate = "";
		score = 0f;
	}
	
	public String getCandidate() {
		return candidate;
	}

	public void setCandidate(String candidate) {
		this.candidate = candidate;
	}

	public float getScore() {
		return score;
	}

	public void setScore(float score) {
		this.score = score;
	}

	public StrFloatPair(final String candidate, final float score) {
		this.candidate = candidate;
		this.score = score;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		candidate = in.readLine();
		score = in.readFloat();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeChars(candidate + "\n");
		// "/n" needed here so that the readLine() calls in readFields will
		// read the correct input
		out.writeFloat(score);
	}

	@Override
	public int compareTo(Object arg0) {
		StrFloatPair pair = (StrFloatPair) arg0;
		if(arg0 == null)
			return 1;
		else if(this.score == pair.score)
			return 0;
		else if(this.score > pair.score)
			return -1;
		else
			return 1;
	}

	static public class StrFloatPariComp implements Comparator<StrFloatPair> {

		@Override
		public int compare(StrFloatPair o1, StrFloatPair o2) {
			if(o1 != null && o2 == null)
				return 1;
			else if(o1 == null && o2 != null)
				return -1;
			else
				return o1.compareTo(o2);
		}
		
	}
	
}
