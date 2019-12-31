package com.treadstone90.mkvstore.raft;

import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

class StateManagerTest extends AnyFlatSpec with MockitoSugar with BeforeAndAfterEach {
  var actorFactory: ActorFactory = _
  var stateManager: StateManager = _

  override def beforeEach(): Unit = {
    actorFactory = mock[ActorFactory]
    stateManager = new StateManagerImpl(actorFactory)
  }

  "StateManager" should "initially start the follower actor and set its state" in {
    val followerActor = mock[FollowerActor]
    when(actorFactory.createActor(Follower)).thenReturn(followerActor)
    stateManager.transitionTo(Follower)
    stateManager.currentStateRef.get()._1 should be (Follower)
  }

  "StateManager" should "Not allow invalid state transitions" in {
    val followerActor = mock[FollowerActor]
    when(actorFactory.createActor(Follower)).thenReturn(followerActor)
    a [RuntimeException] should be thrownBy {
      stateManager.transitionTo(Leader)
    }
  }

  "StateManager" should "allow transition from Follower to candidate" in {
    val followerActor = mock[FollowerActor]
    when(actorFactory.createActor(Follower)).thenReturn(followerActor)
    stateManager.transitionTo(Follower)
    when(actorFactory.createActor(Candidate)).thenReturn(mock[CandidateActor])
    stateManager.processTransitionRequest(TransitionStateRequest(Candidate))
  }

  "StateManager" should "allow transition from Candidate to Leader" in {
    val followerActor = mock[FollowerActor]
    when(actorFactory.createActor(Follower)).thenReturn(followerActor)
    stateManager.transitionTo(Follower)
    when(actorFactory.createActor(Candidate)).thenReturn(mock[CandidateActor])
    stateManager.processTransitionRequest(TransitionStateRequest(Candidate))
    when(actorFactory.createActor(Leader)).thenReturn(mock[LeaderActor])
    stateManager.processTransitionRequest(TransitionStateRequest(Leader))
  }

  "StateManager" should "allow transition from Leader to Follower" in {
    val followerActor = mock[FollowerActor]
    when(actorFactory.createActor(Follower)).thenReturn(followerActor)
    stateManager.transitionTo(Follower)
    when(actorFactory.createActor(Candidate)).thenReturn(mock[CandidateActor])
    stateManager.processTransitionRequest(TransitionStateRequest(Candidate))
    when(actorFactory.createActor(Leader)).thenReturn(mock[LeaderActor])
    stateManager.processTransitionRequest(TransitionStateRequest(Leader))
    stateManager.processTransitionRequest(TransitionStateRequest(Follower))
  }

}