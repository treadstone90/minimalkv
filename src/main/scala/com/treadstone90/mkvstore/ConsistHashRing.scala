package com.treadstone90.mkvstore

import com.google.common.hash.Hashing
import com.google.common.primitives.{Ints, Longs}

import scala.collection.mutable

//  how to identify a node
// lets not call it a node
// it should be called Bucket.
// and we can have virtual buckets.
// NodeBucket is one type of bucket.
// All we need from a bucket is a way to get the identify of a bucket.
// Which is defined by
trait ConsistentHashRing {
  def addBucket(bucket: Bucket, virtualCount: Int)
  def remove(bucket: Bucket)
  def hash(key: Array[Byte]): Bucket
}

class ConsistHashRingImpl extends ConsistentHashRing {
  val treeMap = new mutable.TreeMap[Long, Bucket]()(java.lang.Long.compareUnsigned)
  var map = Map[Long, Int]().withDefaultValue(0)


   def addBucket(bucket: Bucket, virtualCount: Int): Unit = {
     (1 to virtualCount).foreach { id =>
       val virtualBucket = VirtualBucket(id, bucket)
       val hashCode =  Hashing.murmur3_128().hashBytes(virtualBucket.id).asLong()
       treeMap.put(hashCode, virtualBucket)
     }
     map = map.updated(Longs.fromByteArray(bucket.id), virtualCount)
   }

  def getVirtualCount(bucket: Bucket): Option[Int] = {
    map.get(Longs.fromByteArray(bucket.id))
  }

   def remove(bucket: Bucket): Unit = {
     getVirtualCount(bucket) match {
       case Some(count) =>
         (0 to count).foreach { id =>
           val virtualBucket = VirtualBucket(id, bucket)
           val hashCode =  Hashing.murmur3_128().hashBytes(virtualBucket.id).asLong()
           treeMap.remove(hashCode)
         }
     }
     map = map - Longs.fromByteArray(bucket.id)
   }

   def hash(key: Array[Byte]): Bucket = {
     assert(treeMap.nonEmpty)
     val hashCode =  Hashing.murmur3_128().hashBytes(key).asLong()
     println(s"Hashcode is ${java.lang.Long.toUnsignedString(hashCode)}")
     treeMap.from(hashCode).headOption.getOrElse(treeMap.head)._2
   }
}

trait Bucket {
  def id: Array[Byte]
}

case class VirtualBucket(virtualId: Int, bucket: Bucket) extends Bucket {
   val id: Array[Byte] = bucket.id ++ (Ints.toByteArray(virtualId))
}
case class InstanceBucket(instanceName: String) extends Bucket {
  val id = instanceName.getBytes()
}


