package com.xspark.varyingdag.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;


public class Utils
{
	  public static List<Integer> createRandomIntegerList(int size) {
		  
		  List<Integer> res = new ArrayList<Integer>(size);
		  Random r = new Random();
		  while(size>0){
			  res.add(r.nextInt());
			  size--;
		  }
		  
		  return res;
	  }
}
