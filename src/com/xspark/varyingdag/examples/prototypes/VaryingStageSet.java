package com.xspark.varyingdag.examples.prototypes;

import java.util.Random;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import com.xspark.varyingdag.examples.utils.Utils;

/* 
 * The DAG of this program is composed by either a single stage or two stages if the predicate at line 26 is evaluated to true (note that reduceByKey is a shuffle operation).
 */

@SuppressWarnings("resource")
public class VaryingStageSet
{
	public static void main(String[] args) {
		  
		SparkConf conf = new SparkConf().setAppName("VaryingStageSet").setMaster("local[4]");
		JavaSparkContext sc = new JavaSparkContext(conf);
	        
	    JavaPairRDD<String, Integer> x = sc.parallelize(Utils.createRandomIntegerList(1000)).mapToPair(a ->  new Tuple2<String, Integer>(a.toString(), a*a));

		Random r = new Random();

	    if (r.nextInt() % 2 == 0){
	       x = x.reduceByKey((v1, v2) -> v1+v2);
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
