package com.treadstone90.mkvstore.raft

import com.google.common.util.concurrent.AbstractIdleService

trait Actor extends AbstractIdleService {
  def handleAppendEntriesRequest(appendEntriesRequest: AppendEntriesRequest): AppendEntriesResponse
  def handleRequestVoteRequest(requestVoteRequest: RequestVoteRequest): RequestVoteResponse
  def stop(): Unit =  {
    stopAsync()
    awaitTerminated()
  }

  def start(): Unit =  {
    startAsync()
    awaitRunning()
  }
}

case class TransitionStateRequest(toState: ActorState)

sealed trait ActorState
case object Follower extends ActorState
case object Candidate extends ActorState
case object Leader extends ActorState
