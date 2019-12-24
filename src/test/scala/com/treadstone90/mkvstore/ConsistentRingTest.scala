package com.treadstone90.mkvstore

import collection.mutable.Stack
import org.scalatest._

class ConsistentRingTest extends FlatSpec with Matchers {

   "CR Ring" should "" in {

     val crRing = new ConsistHashRingImpl
     crRing.addBucket(InstanceBucket("service.1"), 1)
     crRing.addBucket(InstanceBucket("service.2"), 1)
     crRing.addBucket(InstanceBucket("service.3"), 1)
     println(crRing.hash("Asdasd".getBytes()))
     println(crRing.hash("Asdasdadasd".getBytes()))

     println(crRing.hash("qwewqe".getBytes()))

     println(crRing.treeMap.map{case(k,v) => java.lang.Long.toUnsignedString(k) -> v })
     crRing.getVirtualCount(InstanceBucket("service.3")) should be(Some(1))
   }

  "A Stack" should "pop values in last-in-first-out order" in {
    val stack = new Stack[Int]
    stack.push(1)
    stack.push(2)
    stack.pop() should be(2)
    stack.pop() should be(1)
  }

  it should "throw NoSuchElementException if an empty stack is popped" in {
    val emptyStack = new Stack[Int]
    a[NoSuchElementException] should be thrownBy {
      emptyStack.pop()
    }
  }
}
