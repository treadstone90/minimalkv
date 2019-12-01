package com.treadstone90.mkvstore

object FileReaderApp {
  def main(args: Array[String]): Unit = {
    val sstFileReader = new SSTFileReader[Long](Sliceable.long2Slice)
    sstFileReader.sstFilePath("/tmp/blockStore/part-2.sst", true)
  }
}
