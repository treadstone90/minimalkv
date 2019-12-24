package com.treadstone90.mkvstore

import java.nio.ByteBuffer
import java.nio.charset.Charset

import com.google.common.hash.{Funnel, PrimitiveSink}
import com.google.common.primitives.{Ints, Longs}

trait Sliceable[T] {
  val keyOrdering: Ordering[InternalKey[T]] = {
    (x: InternalKey[T], y: InternalKey[T]) => {
      if (ordering.equiv(x.key, y.key)) {
        Ordering.Long.compare(y.sequenceId, x.sequenceId)
      } else {
        ordering.compare(x.key, y.key)
      }
    }
  }
  def asByteBuffer: T => ByteBuffer
  def ordering: Ordering[T]
  def fromByteBuffer: ByteBuffer => T
  def fromByteArray: Array[Byte] => T
  def funnel: Funnel[T]
}

object Sliceable {
  implicit val long2Slice :Sliceable[Long] = {
    new Sliceable[Long] {
      def asByteBuffer: Long => ByteBuffer = long => ByteBuffer.allocate(java.lang.Long.BYTES).putLong(long)
      val ordering: Ordering[Long] = Ordering.Long
      def fromByteBuffer: ByteBuffer => Long = buf => buf.getLong
      val funnel = (from: Long, into: PrimitiveSink) => into.putLong(from)
      def fromByteArray: Array[Byte] => Long = Longs.fromByteArray
    }
  }

  implicit val str2Slice: Sliceable[String] = {
    new Sliceable[String] {
      def asByteBuffer: String => ByteBuffer = {
        str =>
          val bytes = str.getBytes
          ByteBuffer.allocate(bytes.length).put(bytes)
      }
      val ordering: Ordering[String] = Ordering.String
      def fromByteBuffer: ByteBuffer => String = {
        buf => new String(buf.array())
      }
      val funnel = (from: String, into: PrimitiveSink) => into.putString(from, Charset.defaultCharset())

      def fromByteArray: Array[Byte] => String = (b: Array[Byte]) => new String(b)
    }
  }

  implicit val int2Slice :Sliceable[Int] = {
    new Sliceable[Int] {
      def asByteBuffer: Int => ByteBuffer = {
        (x: Int) => ByteBuffer.allocate(java.lang.Integer.BYTES).putInt(x)
      }
      val ordering: Ordering[Int] = Ordering.Int
      def fromByteBuffer: ByteBuffer => Int = buf => buf.getInt

      val funnel = (from: Int, into: PrimitiveSink) => {
        into.putInt(from)
      }

      def fromByteArray: Array[Byte] => Int = Ints.fromByteArray
    }
  }

}
