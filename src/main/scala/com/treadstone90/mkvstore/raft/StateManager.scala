package com.treadstone90.mkvstore.raft

import java.util.concurrent.atomic.AtomicReference

import com.google.common.eventbus.Subscribe
import com.google.inject.Inject
import com.twitter.inject.Logging

trait StateManager {
  def validStateTransitions: Map[ActorState, Set[ActorState]]
  def currentStateRef: AtomicReference[(ActorState, Actor)]
  def transitionTo(to: ActorState): Unit

  def processTransitionRequest(transitionStateRequest: TransitionStateRequest)

  def handleAppendEntries(appendEntriesRequest: AppendEntriesRequest): AppendEntriesResponse = {
    currentStateRef.get()._2.handleAppendEntriesRequest(appendEntriesRequest)
  }

  def handleRequestVote(requestVoteRequest: RequestVoteRequest): RequestVoteResponse = {
    currentStateRef.get()._2.handleRequestVoteRequest(requestVoteRequest)
  }
}

class StateManagerImpl @Inject() (actorFactory: ActorFactory) extends StateManager with Logging {

  val validStateTransitions: Map[ActorState, Set[ActorState]] = {
    Map(
      Follower -> Set(Candidate),
      Candidate -> Set(Follower, Leader),
      Leader -> Set(Follower, Leader)
    )
  }
  val currentStateRef = new AtomicReference[(ActorState, Actor)]()

  @Subscribe
  def processTransitionRequest(transitionStateRequest: TransitionStateRequest): Unit = {
    transitionTo(transitionStateRequest.toState)
  }

  def transitionTo(to: ActorState): Unit = {
    Option(currentStateRef.get()) match {
      case Some((actorState: ActorState, actor: Actor)) =>
        if(validStateTransitions(actorState).contains(to)) {
          val nextActor = actorFactory.createActor(to)
          info(s"Transitioning to $to from $actorState")
          // here we need to somehow deal with intermediate states.
          nextActor.start()
          currentStateRef.set((to, nextActor))
          // here I can send a leader elected notification event.
          // and a leader unelected notification event.
          actor.stop()
          // old actor will get GCed.
        } else {
          error("Invalid transition from . Exiting")
          throw new RuntimeException("Starting in a non follower state")
        }
      case None =>
        to match {
          case Follower =>
            val follower = actorFactory.createActor(Follower)
            currentStateRef.set((Follower, follower))
            follower.start()
          case _ =>
            error("Invalid transition from booting up. Exiting")
            throw new RuntimeException("Starting in a non follower state")
        }
    }
  }
}
