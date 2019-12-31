package com.treadstone90.mkvstore.raft

import com.google.inject.Inject
import com.twitter.finatra.http.Controller


class RaftController @Inject()(stateManager: StateManager) extends Controller {

  post[AppendEntriesRequest, AppendEntriesResponse]("/appendEntries") { request: AppendEntriesRequest =>
    stateManager.handleAppendEntries(request)
  }

  post[RequestVoteRequest, RequestVoteResponse]("/requestVote") { request: RequestVoteRequest =>
    stateManager.handleRequestVote(request)
  }
}
