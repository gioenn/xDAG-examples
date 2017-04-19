package com.xspark.varyingdag.examples.prototypes;

import java.util.Random;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.xspark.varyingdag.examples.utils.Utils;

/* 
 * The DAG of this program is composed by a single stage (single 'collect' job). The operations of this stage change according to the condition at line 25. 
 */

@SuppressWarnings("resource")
public class VaryingStage
{
	public static void main(String[] args) {
		  
		SparkConf conf = new SparkConf().setAppName("VaryingStage").setMaster("local[4]");
		JavaSparkContext sc = new JavaSparkContext(conf);
	        
	    JavaRDD<Integer> x = sc.parallelize(Utils.createRandomIntegerList(1000));

		Random r = new Random();

	    if (r.nextInt() % 2 == 0){
	       x = x.map(a -> a*a);
	    }
	    else {
	    	x = x.filter(a -> a % 4 == 0).map(a -> a * a * a);
	    }

	    x.collect();
	    

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
