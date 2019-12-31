package com.treadstone90.mkvstore.raft

import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicBoolean

import com.google.common.eventbus.EventBus
import com.treadstone90.mkvstore.WriteAheadLog
import com.twitter.util._
import com.twitter.util.logging.Logging

class CandidateActor(eventBus: EventBus,
                     timer: Timer,
                     raftState: RaftState,
                     candidateSettings: CandidateSettings,
                     writeAheadLog: WriteAheadLog[RaftLogEntry],
                     raftClients: Map[String, RaftClient]) extends Actor with Logging {

  var currentTimerTask: Option[TimerTask] = None
  val  isTimerLocked = new AtomicBoolean(false)

  override def startUp(): Unit = {
    currentTimerTask = initiateElection()
  }

  def initiateElection(): Option[TimerTask] = {
    val currentTerm = raftState.incrementTerm()
    val walLastIndex = writeAheadLog.length - 1
    val requestVoteRequest = RequestVoteRequest(currentTerm, raftState.processId,
      walLastIndex, writeAheadLog.get(walLastIndex).get.term)
    val requestVoteResponse = raftClients.map(_._2.requestVote(requestVoteRequest))
    val responses = Await.result(Future.collectToTry(requestVoteResponse.toSeq))
    val (success, failures) = responses.partition(_.isReturn)
    val count = success.count(_.get().voteGranted)

    info(s"Received $count votes in term $currentTerm")

    warn(s"Requests failed for ${failures.map(_.throwable.getMessage)}")

    if (count + 1 >= raftState.quorum) {
      eventBus.post(TransitionStateRequest(Leader))
      None
    } else {
      Some(timeoutTask())
    }
  }

  def timeoutTask(): TimerTask = {
    val randomSalt = ThreadLocalRandom.current().nextLong(0, 200)
    val timerTask = timer.schedule(Time.now.plus(Duration.fromMilliseconds(candidateSettings.electionTimeoutMillis + randomSalt))) {
      if (isTimerLocked.compareAndSet(false, true)) {
        currentTimerTask = initiateElection()
        isTimerLocked.set(false)
      } else {
        warn("Failed to acquire timer lock")
      }
    }
    info("Scheduling next timeout")
    timerTask
  }

  def handleAppendEntriesRequest(appendEntriesRequest: AppendEntriesRequest): AppendEntriesResponse = {
    while (!isTimerLocked.compareAndSet(false, true)) {}
    currentTimerTask.foreach(_.cancel())

    val response = raftState.monotonicUpdate(appendEntriesRequest.leaderTerm, None) match {
        // "leader" term is lesser than curent term
      case (false, Some((term, candidateId))) =>
        warn(s"Failed to update term. Current term is $term and voted candidate is $candidateId")
        AppendEntriesResponse(term, false, raftState.processId)
      // "leader" term is greater than or equal to current term.
      case (true, _) =>
        info(s"Term has been updated so transitioning to follower")
        val response = AppendEntriesResponse(appendEntriesRequest.leaderTerm, true, raftState.processId)
        eventBus.post(TransitionStateRequest(Follower))
        response
    }
    isTimerLocked.set(false)
    timeoutTask()
    response
  }

  def handleRequestVoteRequest(requestVoteRequest: RequestVoteRequest): RequestVoteResponse = {
    // if it is greater it is only going to get greater.
    raftState.monotonicUpdate(requestVoteRequest.term, Some(requestVoteRequest.candidateId)) match {
      case (false, Some((term, candidateId))) =>
        println(s"Failed to update term to ${requestVoteRequest.term}. Current term is $term and voted candidate is $candidateId")
        RequestVoteResponse(term, false)
      case (true, Some((term, candidateId))) =>
        // the candidate term is greater than or equal to current term.
        if(requestVoteRequest.lastLogTerm > term || requestVoteRequest.lastLogIndex >= writeAheadLog.length - 1) {
          val response = RequestVoteResponse(requestVoteRequest.term, true)
          info(s"Term has been updated and hence vote is granted")
          eventBus.post(TransitionStateRequest(Follower))
          response
        } else {
          info(s"The candidate log length is smaller and less upto date. Not granting vote.")
          RequestVoteResponse(requestVoteRequest.term, false)
        }
    }
  }

  def shutDown(): Unit = {
    while(!isTimerLocked.compareAndSet(false, true)) {}
    currentTimerTask.foreach(_.cancel())
  }
}

case class CandidateSettings(electionTimeoutMillis: Long)
