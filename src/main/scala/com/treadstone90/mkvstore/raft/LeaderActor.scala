//package com.treadstone90.mkvstore.raft
//
//import java.util.concurrent.ConcurrentLinkedQueue
//
//import com.google.common.util.concurrent.AbstractIdleService
//import com.treadstone90.mkvstore.WriteAheadLog
//
//class LeaderActor(val eventQueue: ConcurrentLinkedQueue[Event],
//                  initialTerm: Int, votedFor: Option[Int],
//                  writeAheadLog: WriteAheadLog,
//                  raftState: RaftState,
//                  raftClients: Map[String, RaftClient]) extends AbstractIdleService
