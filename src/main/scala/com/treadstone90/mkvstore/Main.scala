package com.treadstone90.mkvstore

import java.security.SecureRandom
import java.util

import com.twitter.util.Stopwatch

import scala.util.Random

object Main {

  def time[T](label: String)(f: => T): T = {
    val elapsed = Stopwatch.start()
    val result: T = f
    val end = elapsed()
    println(s"[$label] took $end")
    result
  }

  val random = new SecureRandom()

  def generateValue(): Array[Byte]  = {
    val valueSize = random.nextInt(1024*1024)
    val value = new Array[Byte](valueSize)
    random.nextBytes(value)
    value
  }

  def randomInt: Int = {
    random.nextInt(1024*1024)
  }

  def randomLong: Long = {
    random.nextLong()
  }

  def randomString: String = {
    val bytes = new Array[Byte](5)
    random.nextBytes(bytes)
    new String(bytes)
  }

  def main(args: Array[String]): Unit = {
    val store = KVStore.open[Int]("/tmp/blockStore")
    var deletedKeys = Seq.empty[Int]

    val keyValues = (0 to 500).map { i =>
      val key = randomInt
      val value = generateValue()
      time("Writes " + key.toString) {
        store.write(key, value)
      }
      assert(util.Arrays.equals(store.get(key), value))
      (key, value)
    }

    deletedKeys = Random.shuffle(keyValues).take(250).map(_._1)
    val retainedKeys = Random.shuffle(keyValues).takeRight(250)

    time("Reads") {
      retainedKeys.foreach { needle =>
        println(s"GOing to shuffle read mode for ${needle._1}")
        val readData = time(needle._1.toString) {
          store.get(needle._1)
        }
        assert(util.Arrays.equals(readData, needle._2))
      }
    }

    time("Deletes") {
      deletedKeys.foreach { key =>
        store.delete(key)
        assert(store.get(key).isEmpty)
      }
    }

    val key = randomInt
    val value = generateValue()
    val op1 = PutOperation(key, value)
    val op2 = DeleteOperation(key)

    store.writeBatch(WriteBatch(op1, op2))

    assert(store.get(key).isEmpty)

    KVStore.close(store)
  }
}
