package com.treadstone90.mkvstore

import java.security.SecureRandom
import java.util
import java.util.Base64

import scala.util.Random

// write a KV store with memtable and Ss table implementation
// and a simple WAL.

object Main extends Utils {
  val random = new SecureRandom()

  def generateNeedle(): Needle = {
    val k1 = new Array[Byte](16);
    random.nextBytes(k1)
    val valueSize = random.nextInt(1024*1024)
    val value = new Array[Byte](valueSize)
    random.nextBytes(value)
    Needle(k1, valueSize, value)
  }

  def main(args: Array[String]): Unit = {
    val store = new KVStoreImpl(new SSTableManagerImpl("/tmp/blockStore"))
    val needles = (0 to 200).flatMap { i =>
      val needle = generateNeedle()
      if ( needle.size > 0) {
        time(i.toString) {
          store.write(needle)
        }
        assert(util.Arrays.equals(store.get(needle.key).get.data, needle.data))
        Some(needle)
      } else {
        None
      }
    }
    val randomShuffles = Random.shuffle(needles)

    time("Reads") {
      randomShuffles.foreach { needle =>
        val readData = time(Base64.getUrlEncoder.encodeToString(needle.key)) {
          store.get(needle.key).get.data
        }
        assert(util.Arrays.equals(readData, needle.data))
      }
    }
  }
}
