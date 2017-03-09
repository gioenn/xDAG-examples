package com.xspark.varyingdag.examples;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import com.xspark.varyingdag.utils.Utils;



/* 
 * The program creates a random RDD of 100 Integers (variable x) and saves in y the even numbers (note that at this point no action is reached so no computation is performed).
 * At line 27 the first job is created (count). If the number of even number is less than 50 another job is created and performed (line 29).
 * Then at line 32 another job is created in which from a random RDD of 1000 Intenger the multiplier of 3 are counted. 
 * At 33 a loop starts, until the count is greater than 320 the same job is executed
 */


@SuppressWarnings("resource")
public class VaryingJobSet {
  public static void main(String[] args) {
	  
	SparkConf conf = new SparkConf().setAppName("VaryingJobSet").setMaster("local[4]");
	JavaSparkContext sc = new JavaSparkContext(conf);
        
    JavaRDD<Integer> x = sc.parallelize(Utils.createRandomIntegerList(100));
    JavaRDD<Integer> y = x.filter(a -> a % 2 == 0);
    
    if (y.count() < 50){
        JavaRDD<Integer> z = sc.parallelize(Utils.createRandomIntegerList(100));
        z.map(a -> a * 5).aggregate(0, (a, b)-> a + b, (a, b)-> a + b).doubleValue();
    }

    long count = sc.parallelize(Utils.createRandomIntegerList(1000)).filter(a -> a % 3 == 0).count();
    while(count > 320){
    	count = sc.parallelize(Utils.createRandomIntegerList(1000)).filter(a -> a % 3 == 0).count();
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

}