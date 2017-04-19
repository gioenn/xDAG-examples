package com.xspark.varyingdag.examples.common;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import com.google.common.collect.Iterables;

@SuppressWarnings("resource")
public class NaivePageRank
{

	public void run(String textFile, Integer numIterations) {

		SparkConf conf = new SparkConf().setAppName("NaivePageRank").setMaster("local[4]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> lines = sc.textFile(textFile);

		// Loads all URLs from input file and initialize their neighbors.
		JavaPairRDD<String, Iterable<String>> links = lines.mapToPair(s -> {
			String[] parts = Pattern.compile("\\s+").split(s);
			return new Tuple2<>(parts[0], parts[1]);
		}).distinct().groupByKey().cache();


		// Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
		JavaPairRDD<String, Double> ranks = links.mapValues(rs -> 1.0);

		// Calculates and updates URL ranks continuously using PageRank algorithm.
		for (int current = 0; current < numIterations; current++) {

			// Calculates URL contributions to the rank of other URLs.
			JavaPairRDD<String, Double> contribs = links.join(ranks).values().flatMapToPair(s -> {
				int urlCount = Iterables.size(s._1());
				List<Tuple2<String, Double>> results = new ArrayList<>();
				for (String n : s._1) {
					results.add(new Tuple2<>(n, s._2() / urlCount));
				}
				return results.iterator();
			});

			// Re-calculates URL ranks based on neighbor contributions.
			ranks = contribs.reduceByKey((a, b) -> a + b).mapValues(sum -> 0.15 + sum * 0.85);
		}

		// Collects all URL ranks and dump them to console.
		List<Tuple2<String, Double>> output = ranks.collect();

		for (Tuple2<?,?> tuple : output) {
			System.out.println(tuple._1() + " has rank: " + tuple._2() + ".");
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

	public static void main(String[] args) {

		new NaivePageRank().run("data/pagerank_data.txt", 10);

	}

}