package com.treadstone90.mkvstore.raft

import com.google.common.eventbus.EventBus
import com.google.common.net.HostAndPort
import com.google.inject.{Provides, Singleton}
import com.twitter.app.Flag
import com.twitter.finatra.json.FinatraObjectMapper
import com.twitter.inject.TwitterModule

object RaftStateModule extends TwitterModule {
  val raftStatePath: Flag[String] = flag(name="raft.state.path",
    default = "/tmp/raft_state",
    help = "Path to persist and load raftState")
  val replicas: Flag[Int] = flag(
    name = "raft.replicas", default = 3, help = "The majority serves required.")

  val processId: Flag[String] = flag("raft.process.id",
    default = "process1",
    help = "The name of the process participating in raft")

  val connectionString: Flag[String] = flag(
    name = "raft.connection.string",
    default = "process1;localhost:8888,process2;localhost:8889,process3;localhost:8887",
    help = "Comma separated host and port pairs for the cluster. processId1;localhost:8888,processId2;localhost:8889"
  )

  val heartbeatIntervalMillis: Flag[Long] = flag(
    name="raft.leader.heartbeat.millis",
    default=1000,
    help = "Interval to send the hearbeat from the leader."
  )

  val electionTimeoutMillis: Flag[Long] = flag(
    name="raft.follower.election.timeout.millis",
    default=1200,
    help = "Interval to send the hearbeat from the leader."
  )

  @Provides
  @Singleton
  def provideRaftState(): RaftState = {
    new RaftStateImpl(replicas(), ???, processId(), raftStatePath())
  }


  @Provides
  @Singleton
  def provideRaftClients(finatraObjectMapper: FinatraObjectMapper): Map[String, RaftClient] = {
    val currentProcessId = processId()
    connectionString().split(",").filterNot(_.startsWith(currentProcessId)).map { pairs =>
      val Array(processId, hostAndPort) = pairs.split(";")
      processId -> new RaftClient(HostAndPort.fromString(hostAndPort), finatraObjectMapper)
    }.toMap
  }

  @Provides
  @Singleton
  def provideStateManager(eventBus: EventBus,
                          raftState: RaftState,
                          raftClients: Map[String,RaftClient]): StateManager = {
    val leaderSettings  = LeaderSettings(heartbeatIntervalMillis())
    val followerSettings = FollowerSettings(electionTimeoutMillis())
    val candidateSettings = CandidateSettings(electionTimeoutMillis())
    val actorFactoryImpl = new ActorFactoryImpl(raftState, eventBus, raftClients, leaderSettings, followerSettings,
      candidateSettings, ???, ???)
    val stateManager = new StateManagerImpl(actorFactoryImpl)
    eventBus.register(stateManager)
    stateManager
  }
}
