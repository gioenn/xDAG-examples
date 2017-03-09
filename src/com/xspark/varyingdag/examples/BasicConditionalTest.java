package com.xspark.varyingdag.examples;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;


/* 
 * The program creates a random RDD of 100 Integers (variable x) and saves in y the even numbers (note that at this point no action is reached so no computation is performed).
 * At line 28 the first job is created (count). If the number of even number is less than 50 another job is created and performed (line 30).
 * Then at line 33 another job is created in which from a random RDD of 1000 Intenger the multiplier of 3 are counted. 
 * At 34 a loop starts, until the count is greater than 320 the same job is executed
 */


@SuppressWarnings("resource")
public class BasicConditionalTest {
  public static void main(String[] args) {
	  
	SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local[4]");
	JavaSparkContext sc = new JavaSparkContext(conf);
        
    JavaRDD<Integer> x = sc.parallelize(createRandomIntegerList(100));
    JavaRDD<Integer> y = x.filter(a -> a % 2 == 0);
    
    if (y.count() < 50){
        JavaRDD<Integer> z = sc.parallelize(createRandomIntegerList(100));
        z.map(a -> a * 5).aggregate(0, (a, b)-> a + b, (a, b)-> a + b).doubleValue();
    }

    long count = sc.parallelize(createRandomIntegerList(1000)).filter(a -> a % 3 == 0).count();
    while(count > 320){
    	count = sc.parallelize(createRandomIntegerList(1000)).filter(a -> a % 3 == 0).count();
    }

    try {
		Thread.sleep(1000*60*60);
	}
	catch (InterruptedException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
    
    sc.stop();
  }
  
  private static List<Integer> createRandomIntegerList(int size) {
	  
	  List<Integer> res = new ArrayList<Integer>(size);
	  Random r = new Random();
	  while(size>0){
		  res.add(r.nextInt());
		  size--;
	  }
	  
	  return res;
  }
  
  
}