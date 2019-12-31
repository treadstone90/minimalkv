package com.treadstone90.mkvstore.raft

sealed trait Event

case class HeartBeatEvent(leaderTerm: Long, leaderId: String, timestamp: Long) extends Event

final case class AppendEntriesRequest(leaderTerm: Long, leaderId: String, prevLogMeta: Option[RaftLogMetadata],
                                      entries: Seq[RaftLogEntry], leaderCommitIndex: Long) extends Event

final case class RequestVoteRequest(term: Long, candidateId: String, lastLogIndex: Long, lastLogTerm: Long)

final case class AppendEntriesRequestWrapper(appendEntriesEvent: Option[AppendEntriesRequest],
                                             heartBeatEvent: Option[HeartBeatEvent])



case class RaftLogMetadata(index: Long, term: Long)