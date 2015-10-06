package spark.tests;

import java.util.Arrays;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class WordCount {
	public static void main(String[] args) throws Exception {
		String master = "spark://localhost:18080";
		JavaSparkContext sc = new JavaSparkContext("local", "wordcount", System.getenv("SPARK_HOME"),
				System.getenv("JARS"));
		JavaRDD<String> rdd = sc.textFile("testFile.txt");

		FlatMapFunction<String, String> firstFunction = new FlatMapFunction<String, String>() {
			public Iterable<String> call(String x) {
				return Arrays.asList(x.split(" "));
			}
		};

		PairFunction<String, String, Integer> secondFunction = new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String x) {
				return new Tuple2(x, 1);
			}
		};
		
		Function2<Integer,Integer,Integer> thirdFunction=	new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer x, Integer y) {
				return x + y;
			}
		};

		JavaPairRDD<String, Integer> counts = rdd.flatMap(firstFunction).mapToPair(secondFunction)
				.reduceByKey(thirdFunction);
	System.out.println("______> "+counts.count());
		counts.saveAsTextFile("result.txt");
	}
}
