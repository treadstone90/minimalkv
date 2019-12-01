package com.treadstone90.mkvstore

import java.security.SecureRandom
import java.util

import scala.util.Random

object Main extends Utils {
  val random = new SecureRandom()

  def generateValue()  = {
    val valueSize = random.nextInt(1024*1024)
    val value = new Array[Byte](valueSize)
    random.nextBytes(value)
    Needle(valueSize, value)
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
    val store = KVStore.apply[Long]("/tmp/blockStore")

    val keyValues = (0 to 200).map { i =>
      val key = randomInt
      val value = generateValue()
      time("Writes " + key.toString) {
        store.put(key, value)
      }
      assert(util.Arrays.equals(store.get(key).get.data, value.data))
      (key, value)
    }

    val randomShuffles = Random.shuffle(keyValues)
    time("Reads") {
      randomShuffles.foreach { needle =>
        println(s"GOing to shuffle read mode for ${needle._1}")
        val readData = time(needle._1.toString) {
          store.get(needle._1).get.data
        }
        assert(util.Arrays.equals(readData, needle._2.data))
      }
    }
  }
}
