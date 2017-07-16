package com.tresata.spark.skewjoin.api.java;

import static org.junit.Assert.assertTrue;

import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.tresata.spark.skewjoin.SparkSuite$;
import com.tresata.spark.skewjoin.api.java.CompanionHashers.CMSHasherInteger;
import java.io.Serializable;
import java.util.List;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;
import scala.Tuple2;

public class SkewJoinOperationsSuite implements Serializable {

  private JavaSparkContext jsc() {
    return SparkSuite$.MODULE$.javaSparkContext();
  }

  private <X, Y> Tuple2<X, Y> tuple2(X x, Y y) {
    return new Tuple2<X, Y>(x, y);
  }

  @Test
  public void testSkewJoinOperation() {
    List<Tuple2<Integer, Integer>> pairs1 = Lists.newArrayList(tuple2(1, 1), tuple2(1, 1), tuple2(2, 1), tuple2(3, 1), tuple2(4, 1));
    List<Tuple2<Integer, Integer>> pairs2 = Lists.newArrayList(tuple2(1, 2), tuple2(1, 2), tuple2(6, 2), tuple2(4, 2), tuple2(5, 2));

    JavaPairRDD<Integer, Integer> rdd1 = jsc().parallelizePairs(pairs1);
    JavaPairRDD<Integer, Integer> rdd2 = jsc().parallelizePairs(pairs2);

    SkewJoinOperations<Integer, Integer> sjRDD1 = new SkewJoinOperations<>(rdd1, Ordering.natural(), new CMSHasherInteger());
    SkewJoinOperations<Integer, Integer> sjRDD2 = new SkewJoinOperations<>(rdd2, Ordering.natural(), new CMSHasherInteger());

    List<Tuple2<Integer, Tuple2<Integer, Integer>>> result = sjRDD1.skewJoin(sjRDD2).sortByKey().collect();
    List<Tuple2<Integer, Tuple2<Integer, Integer>>> answer = Lists.newArrayList(tuple2(1, tuple2(1, 2)), tuple2(1, tuple2(1, 2)), tuple2(1, tuple2(1, 2)), tuple2(1, tuple2(1, 2)), tuple2(4, tuple2(1, 2)));

    for (int i = 0; i < answer.size(); i++) {
      System.out.println(result.get(i) + " == " + answer.get(i));
      assertTrue(result.get(i)._1.equals(answer.get(i)._1));
      assertTrue(result.get(i)._2._1.equals(answer.get(i)._2._1));
      assertTrue(result.get(i)._2._2.equals(answer.get(i)._2._2));
    }
  }

}
