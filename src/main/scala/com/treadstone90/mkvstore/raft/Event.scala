package com.treadstone90.mkvstore.raft

sealed trait Event

final case object ElectionTimeoutEvent extends Event
case class HeartBeatEvent(leaderTerm: Int, leaderId: String, timestamp: Long) extends Event

final case class AppendEntriesEvent(leaderTerm: Int, leaderId: String, prevLogEntry: RaftLogEntry,
                                    entries: Seq[RaftLogEntry], leaderCommitIndex: Long) extends Event

final case class RequestVoteRequest(term: Long, candidateId: String, lastLogIndex: Long, lastLogTerm: Long)

final case class AppendEntriesRequest(appendEntriesEvent: Option[AppendEntriesEvent],
                                      heartBeatEvent: Option[HeartBeatEvent])

