package com.treadstone90.mkvstore.raft

import java.util.concurrent.atomic.AtomicReference

import com.google.common.util.concurrent.AbstractIdleService

trait StateManager {
  def validStateTransitions: Map[ActorState, Set[ActorState]]
  def actorFactory: ActorFactory
  def currentStateRef: AtomicReference[(ActorState, Actor)]

  def transitionTo(to: ActorState): Unit = {
    Option(currentStateRef.get()) match {
      case Some((actorState: ActorState, actor: Actor)) =>
        val currentState = currentStateRef.get()._1
        if(validStateTransitions(currentState).contains(actorState)) {
          actor.stop()
          val nextActor = actorFactory.createActor(to)
          nextActor.start()
          currentStateRef.set((to, nextActor))
          // old actor will get GCed.
        } else {
          println("Invalid transition from . Exiting")
          throw new RuntimeException("Starting in a non follower state")
        }
      case None =>
        to match {
          case Follower =>
            val follower = actorFactory.createActor(Follower)
            currentStateRef.set((Follower, follower))
            follower.start()
          case _ =>
            println("Invalid transition from booting up. Exiting")
            throw new RuntimeException("Starting in a non follower state")
        }
    }
  }

  def handleAppendEntries(appendEntriesRequest: AppendEntriesRequest): AppendEntriesResponse = {
    currentStateRef.get()._2.handleAppendEntriesRequest(appendEntriesRequest)
  }
}


sealed trait ActorState
case object Follower extends ActorState
case object Candidate extends ActorState
case object Leader extends ActorState

case class TransitionStateRequest(toState: ActorState)


trait ActorFactory {
  def raftState: RaftState
  def createActor(actorState: ActorState): Actor
}

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
