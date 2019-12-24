package com.treadstone90.mkvstore.raft

import com.twitter.finagle.http.Request
import com.twitter.finatra.http.Controller
import javax.inject.Inject

class RaftController @Inject()() extends Controller {

  post("/appendEntries") { request: Request =>
    "bar"
  }

  post("/requestVote") { request: Request =>
    "bar"
  }
}
