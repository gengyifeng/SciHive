package org.apache.hadoop.hive.contrib.fileformat.netcdf;

import java.io.Serializable;
import java.util.HashMap;

class ValueRange implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private double min=-Double.MAX_VALUE;
	private double max=Double.MAX_VALUE;
	public ValueRange(double a,double b){
		min=a;
		max=b;
	}
	public double getMin(){
		return min;
	}
	public double getMax(){
		return max;
	}
}
public class VariableIndex implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public HashMap<Integer,ValueRange> hs;
	public int blockSize=0;
	public VariableIndex(int size){
		blockSize=size;
		hs=new HashMap<Integer,ValueRange>();
	}
}
