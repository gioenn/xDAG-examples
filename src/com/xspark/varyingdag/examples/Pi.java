package com.xspark.varyingdag.examples;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


@SuppressWarnings("resource")
public class Pi
{

	public void run(double allowedError, int maxIteration){
		
		SparkConf conf = new SparkConf().setAppName("Pi").setMaster("local[4]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		int slices = 1, it = 0;
		double error, pi;
		
		do {
			
		int n = 100000 * slices;
		
		List<Integer> l = new ArrayList<>(n);
		for (int i = 0; i < n; i++) {
			l.add(i);
		}

		JavaRDD<Integer> dataSet = sc.parallelize(l, slices);

		int count = dataSet.map(integer -> {
			double x = Math.random() * 2 - 1;
			double y = Math.random() * 2 - 1;
			return (x * x + y * y <= 1) ? 1 : 0;
		}).reduce((integer, integer2) -> integer + integer2);
		
		pi = 4.0*count/n;
		slices+=10;
		it++;
		
		error = Math.abs(3.14159265358979323846-pi);

		
		} while (error > allowedError && it < maxIteration);
		
		System.out.println("Pi is roughly " + pi + " error: "+error);

		try {
			Thread.sleep(1000*60*60);
		}
		catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		sc.stop();
	}

	public static void main(String[] args) {
		new Pi().run(0.0001, 5);
	}

}
