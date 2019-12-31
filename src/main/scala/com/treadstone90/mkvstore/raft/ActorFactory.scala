package com.treadstone90.mkvstore.raft

import com.google.common.eventbus.EventBus
import com.treadstone90.mkvstore.WriteAheadLog
import com.twitter.finagle.util.DefaultTimer

trait ActorFactory {
  def raftState: RaftState
  def createActor(actorState: ActorState): Actor
  def leaderSettings: LeaderSettings
  def followerSettings: FollowerSettings
  def candidateSettings: CandidateSettings
}

class ActorFactoryImpl(val raftState: RaftState,
                       eventBus: EventBus,
                       raftClients: Map[String,RaftClient],
                       val leaderSettings: LeaderSettings,
                       val followerSettings: FollowerSettings,
                       val candidateSettings: CandidateSettings,
                       writeAheadLog: WriteAheadLog[RaftLogEntry],
                       tracker: Tracker) extends ActorFactory {
  def createActor(actorState: ActorState): Actor = {
    actorState match {
      case Follower =>
        new FollowerActor(eventBus, DefaultTimer, followerSettings, writeAheadLog, raftState)
      case Leader =>
        new LeaderActor(eventBus, DefaultTimer, raftState, leaderSettings, raftClients, writeAheadLog, tracker)
      case Candidate =>
        new CandidateActor(eventBus, DefaultTimer, raftState, candidateSettings, writeAheadLog, raftClients)
    }
  }
}
