package com.tresata.spark.skewjoin.api.java

import java.util.Comparator

import scala.reflect.ClassTag
import org.apache.spark.api.java.JavaPairRDD
import com.tresata.spark.skewjoin.Dsl._
import com.tresata.spark.skewjoin.{SkewJoinOperations => SSkewJoinOperations}
import com.twitter.algebird.CMSHasher
import org.apache.spark.rdd.RDD

object SkewJoinOperations {
  private case class ComparatorOrdering[T](comparator: Comparator[T]) extends Ordering[T] {
    def compare(x: T, y: T) = comparator.compare(x, y)
  }

  private def comparatorToOrdering[T](comparator: Comparator[T]): Ordering[T] = new ComparatorOrdering(comparator)

  private def fakeClassTag[T]: ClassTag[T] = ClassTag.AnyRef.asInstanceOf[ClassTag[T]]

  private def skewJoin[K, V](javaPairRDD: JavaPairRDD[K, V], keyComparator : Comparator[K], keyOrdering : CMSHasher[K]): SSkewJoinOperations[K, V] = {
    implicit def kClassTag: ClassTag[K] = javaPairRDD.kClassTag
    implicit def vClassTag: ClassTag[V] = javaPairRDD.vClassTag
    implicit def kOrdering: Ordering[K] = comparatorToOrdering(keyComparator)
    implicit def CMSHasher: CMSHasher[K] = keyOrdering
    SSkewJoinOperations(javaPairRDD.rdd)
  }
}

class SkewJoinOperations[K, V] private (sSkewJoinOperations: SSkewJoinOperations[K, V]) extends Serializable {
  def this(javaPairRDD: JavaPairRDD[K, V], keyComparator : Comparator[K], keyOrdering : CMSHasher[K]) =
    this(SkewJoinOperations.skewJoin(javaPairRDD, keyComparator, keyOrdering))

  import SkewJoinOperations._

  def getRDD: RDD[(K, V)] = sSkewJoinOperations.getRDD

  def skewJoin[W](that: SkewJoinOperations[K, W]): JavaPairRDD[K, (V, W)] = {
    implicit def kClassTag: ClassTag[K] = fakeClassTag[K]
    implicit def vClassTag: ClassTag[V] = fakeClassTag[V]
    implicit def wClassTag: ClassTag[W] = fakeClassTag[W]
    new JavaPairRDD(sSkewJoinOperations.skewJoin(that.getRDD))
  }
}

abstract class CMSHasherWrapper[T] extends CMSHasher[T]