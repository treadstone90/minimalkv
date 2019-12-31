package com.treadstone90.mkvstore.raft

import java.util.concurrent.atomic.AtomicBoolean

import com.google.common.eventbus.EventBus
import com.treadstone90.mkvstore.WriteAheadLog
import com.twitter.inject.Logging
import com.twitter.util._

class LeaderActor(eventBus: EventBus,
                  timer: Timer,
                  raftState: RaftState,
                  leaderSettings: LeaderSettings,
                  raftClients: Map[String, RaftClient],
                  writeAheadLog: WriteAheadLog[RaftLogEntry],
                  tracker: Tracker) extends Actor  with Logging {

  var currentTimerTask: Option[TimerTask] = None
  val isTimerLocked = new AtomicBoolean(false)
  // this is the last entry that was appended by a client.

  override def startUp(): Unit = {
    tracker.startUp()
    currentTimerTask = sendHeartbeatRPC()
  }

  def sendHeartbeatRPC(): Option[TimerTask] = {
    AppendEntriesRequest(raftState.currentTerm.get, raftState.processId,
      Some(RaftLogMetadata(writeAheadLog.length - 1, raftState.currentTerm.get)),
      Seq.empty, raftState.commitIndex)
    val heartBeatEvent = HeartBeatEvent(raftState.currentTerm.get, raftState.processId, Time.now.inMilliseconds)
    val heartbeatResponse = raftClients.map(_._2.sendHeartBeat(heartBeatEvent))
    val tryResponses = Await.result(Future.collectToTry(heartbeatResponse.toSeq))

    info(s"The responses for heartbeat were $tryResponses")

    val wrongTerms = tryResponses.filter(t => t.isReturn && !t.get().success)

    if(wrongTerms.nonEmpty) {
      warn(s"Received responses with hearbeat failure. $wrongTerms")
      val mostRecentProcess = wrongTerms.maxBy(t => t.get().currentTerm).get()
      raftState.monotonicUpdate(mostRecentProcess.currentTerm, None)
      eventBus.post(TransitionStateRequest(Follower))
      None
    } else {
      info("Scheduling the next heartbeat event on the leader")
      Some(timeoutTask())
    }
  }

  def timeoutTask(): TimerTask = {
    timer.schedule(Time.now.plus(Duration.fromMilliseconds(leaderSettings.hearbeatIntervalMillis))) {
      if (isTimerLocked.compareAndSet(false, true)) {
        currentTimerTask = sendHeartbeatRPC()
        isTimerLocked.set(false)
      } else {
        error("Failed to schedule heartbeat event")
      }
    }
  }

  def handleAppendEntriesRequest(appendEntriesRequest: AppendEntriesRequest): AppendEntriesResponse = {
    val response: AppendEntriesResponse = if (appendEntriesRequest.entries.isEmpty) {
      info(s"Received heartbeat from ${appendEntriesRequest.leaderId}")
      raftState.monotonicUpdate(appendEntriesRequest.leaderTerm, None) match {
        case (false, Some((term, candidateId))) =>
          println(s"Failed to update term. Current term is $term and voted candidate is $candidateId")
          AppendEntriesResponse(term, false, raftState.processId)
        case (true, _) =>
          info(s"Term has been updated so transitioning to follower")
          val response = AppendEntriesResponse(appendEntriesRequest.leaderTerm, true, raftState.processId)
          eventBus.post(TransitionStateRequest(Follower))
          response
      }
    } else {
      println("received a non heart beat event.")
      AppendEntriesResponse(-1, true, raftState.processId)
    }
    response
  }

  def handleRequestVoteRequest(requestVoteRequest: RequestVoteRequest): RequestVoteResponse = {
    // if it is greater it is only going to get greater.
    raftState.monotonicUpdate(requestVoteRequest.term, Some(requestVoteRequest.candidateId)) match {
      case (false, Some((term, candidateId))) =>
        println(s"Failed to update term. Current term is $term and voted candidate is $candidateId")
        RequestVoteResponse(term, false)
      case (true, _) =>
        val response = RequestVoteResponse(requestVoteRequest.term, true)
        warn(s"Term has been updated and leader is stepping down. moving to follower")
        eventBus.post(TransitionStateRequest(Follower))
        response
    }
  }

  def shutDown(): Unit = {
    while(!isTimerLocked.compareAndSet(false, true)) {}
    currentTimerTask.foreach(_.cancel())
  }

  def appendEntry(logEntry: RaftLogEntry): Future[Unit] = {
    val index = writeAheadLog.appendEntry(logEntry)
    tracker.trackRequest(index)
  }
}

case class LeaderSettings(hearbeatIntervalMillis: Long)
