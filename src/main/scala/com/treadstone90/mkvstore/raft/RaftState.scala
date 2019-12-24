package com.treadstone90.mkvstore.raft

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicLong, AtomicReference}

trait RaftState {
  def quorum: Int
  def currentTerm(): Long
  def setCurrentTerm(term: Long): Boolean
  def incrementCurrentTerm(): Long
  def voteFor(term: Long, candidateId: String)
  def votedFor(term: Long): Option[String]
  def withExclusiveLock[T](f: => T): T
}

// a term always has to have a vote.
// without a vote there is no term.
// it can even just be a self vote.

class RaftStateImpl(val quorum: Int,
                    initialVotingTerm: (Long, String)) extends RaftState {
  val _currentVotingTerm = new AtomicReference[(Long, String)](initialVotingTerm)

  val _currentTerm = new AtomicReference[Long]()(initialCurrentTerm)
  // map of string to who you have voted for.
  val voteMap = new ConcurrentHashMap[Long, String]()
  var vote: Option[String] = None

  def currentTerm(): Long = _currentVotingTerm.get()._1

  // we need to ensure terms are always monotonically increasing
  def setCurrentTerm(term: Long): Boolean = {
    _currentTerm.set(term)
    vote = None
    true
  }

  def incrementCurrentTerm(candidateId: String): Long = {
    _currentVotingTerm.updateAndGet()
    vote = None
    _currentTerm.incrementAndGet()
  }

  def voteFor(term: Long, candidateId: String): Unit = {
    assert(term == _currentTerm.get())
    vote = Some(candidateId)
  }

  def voteIfEmpty(term: Long, candidateId: String): Boolean = {
    Option(voteMap.putIfAbsent(term, candidateId)) match {
      case None => true
      case Some(_) => false
    }
  }

  def votedFor(term: Long): Option[String] = {
    Option(voteMap.get(term))
  }

  def withExclusiveLock[T](f: => T): T = {
    _currentTerm.getAndUpdate()
  }

  def monotonicUpdate(term: Long): Option[Long] = {

  }

  // if return None then vote was sucess
  // if return term == requested term, then vote sucess
  // if return term > requested term, vote fail.
  def atomicVoteFor(term: Long, candidate: String): Option[(Long, String)]
}
