package com.treadstone90.mkvstore.raft

import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicBoolean

import com.google.common.eventbus.EventBus
import com.treadstone90.mkvstore.WriteAheadLog
import com.twitter.inject.Logging
import com.twitter.util.{Duration, Time, Timer, TimerTask}

class FollowerActor(eventBus: EventBus,
                    timer: Timer,
                    followerSettings: FollowerSettings,
                    writeAheadLog: WriteAheadLog[RaftLogEntry],
                    raftState: RaftState) extends Actor with Logging {

  var currentTimerTask: Option[TimerTask] = None
  val isTimerLocked = new AtomicBoolean(false)

  def timeoutTask(): TimerTask = {
    val randomSalt = ThreadLocalRandom.current().nextLong(0, 200)
    timer.schedule(Time.now.plus(Duration.fromMilliseconds(followerSettings.electionTimeoutMillis + randomSalt))) {
      if(isTimerLocked.compareAndSet(false, true)) {
        //  here I need to transition into a candidate state
        // infact I can stop accepting requests to the follower.
        eventBus.post(TransitionStateRequest(Candidate))
        isTimerLocked.set(false)
      }
    }
  }

  def handleAppendEntriesRequest(appendEntriesRequest: AppendEntriesRequest): AppendEntriesResponse = {
    while(!isTimerLocked.compareAndSet(false, true)) {}
    currentTimerTask.foreach(_.cancel())

    val response = raftState.monotonicUpdate(appendEntriesRequest.leaderTerm, None) match {
      case (false, Some((term, candidateId))) =>
        warn(s"Failed to update term. Current term is $term and voted candidate is $candidateId")
        AppendEntriesResponse(term, false, raftState.processId)
      case (true, _) =>
        info(s"Term from AppendRPC is valid.")
        if(appendEntriesRequest.entries.isEmpty) {
          raftState.commitIndex = appendEntriesRequest.leaderCommitIndex
          AppendEntriesResponse(appendEntriesRequest.leaderTerm, true, raftState.processId)
        } else {
          handleAppendEntriesEvent(appendEntriesRequest)
        }

      //already follower no need to transition
    }
    currentTimerTask = Some(timeoutTask())
    isTimerLocked.set(false)
    response
  }


  def handleAppendEntriesEvent(appendEntriesEvent: AppendEntriesRequest): AppendEntriesResponse = {
    appendEntriesEvent.prevLogMeta match {
      case None =>
        writeAheadLog.appendEntries(appendEntriesEvent.entries)
        raftState.commitIndex = Math.min(appendEntriesEvent.entries.length - 1, appendEntriesEvent.leaderCommitIndex)
        AppendEntriesResponse(raftState.currentTerm.get, true, raftState.processId)
      case Some(prevLogMeta) =>
        writeAheadLog.get(prevLogMeta.index) match {
          case None =>
            AppendEntriesResponse(raftState.currentTerm.get, false, raftState.processId)
          case Some(walEntry) if walEntry.term != appendEntriesEvent.leaderTerm =>
            AppendEntriesResponse(raftState.currentTerm.get, false, raftState.processId)
          case Some(walEntry) =>
            val startIndex = prevLogMeta.index + 1
            val endIndex = startIndex + appendEntriesEvent.entries.length
            if(writeAheadLog.get(startIndex).isEmpty) {
              writeAheadLog.appendEntries(appendEntriesEvent.entries)
            } else {
              appendEntriesEvent.entries.zip(startIndex to endIndex - 1).foreach { case(entry, index) =>
                val walEntry = writeAheadLog.get(index)
                if(walEntry.exists(entry => entry.term != appendEntriesEvent.leaderTerm)) {
                  // keep till index - 1
                  writeAheadLog.truncate(index - 1)
                } else if(walEntry.isEmpty) {
                  writeAheadLog.appendEntry(entry)
                }
              }
            }
            raftState.commitIndex = Math.min(endIndex, appendEntriesEvent.leaderCommitIndex)
            AppendEntriesResponse(raftState.currentTerm.get, true, raftState.processId)
        }
    }
  }

  def handleRequestVoteRequest(requestVoteRequest: RequestVoteRequest): RequestVoteResponse = {
    while(!isTimerLocked.compareAndSet(false, true)) {}
    currentTimerTask.foreach(_.cancel())
    // if it is greater it is only going to get greater.
      val response = raftState.monotonicUpdate(requestVoteRequest.term, Some(requestVoteRequest.candidateId)) match {
        case (false, Some((term, candidateId))) =>
          println(s"Failed to update term. Current term is $term and voted candidate is $candidateId")
          RequestVoteResponse(term, false)
        case (true, _) =>
          println(s"Term has been updated and hence vote us granted")
          RequestVoteResponse(requestVoteRequest.term, true)
          // already follower no need to transition
      }
    currentTimerTask = Some(timeoutTask())
    isTimerLocked.set(false)
    response
  }

  override def shutDown(): Unit = {
    while(!isTimerLocked.compareAndSet(false, true)) {}
    info("Attempting to cancel the follower and cancelling the timer")
    currentTimerTask.foreach(_.cancel())
  }

  override def startUp(): Unit = {
    currentTimerTask = Some(timeoutTask())
  }
}

case class FollowerSettings(electionTimeoutMillis: Long)