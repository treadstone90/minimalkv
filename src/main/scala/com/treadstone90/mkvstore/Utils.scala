package com.treadstone90.mkvstore

trait Utils {
  def time[T](label: String)(f: => T): T = {
    val start = System.nanoTime()
    val result: T = f
    val end = System.nanoTime()
    println(s"[$label] took ${1.0*(end - start)/1000000} ms")
    result
  }
}
