package com.treadstone90.mkvstore.raft

import com.google.common.net.HostAndPort
import com.twitter.finagle.http.Request
import com.twitter.finagle.{Http, Service, http}
import com.twitter.finatra.json.FinatraObjectMapper
import com.twitter.util.Future

class RaftClient(hostAddress: HostAndPort, finatraObjectMapper: FinatraObjectMapper) {
  val client: Service[http.Request, http.Response] =
    Http.newService(hostAddress.toString)

  def appendEntries(appendEntriesEvent: AppendEntriesEvent): Future[AppendEntriesResponse] = {
    val request = Request(http.Method.Post, "/appendEntries")
    request.host(hostAddress.toString)
    request.setContentTypeJson()
    request.setContentString(finatraObjectMapper.writeValueAsString(appendEntriesEvent))
    client.apply(request).map { response =>
      finatraObjectMapper.parse[AppendEntriesResponse](response.getContentString())
    }
  }

  def sendHeartBeat(heartBeatEvent: HeartBeatEvent): Future[AppendEntriesResponse] = {
    val appendEntriesRequest = AppendEntriesRequest(None, Some(heartBeatEvent))
    val request = Request(http.Method.Post, "/appendEntries")
    request.host(hostAddress.toString)
    request.setContentTypeJson()
    request.setContentString(finatraObjectMapper.writeValueAsString(appendEntriesRequest))
    client.apply(request).map { response =>
      finatraObjectMapper.parse[AppendEntriesResponse](response.getContentString())
    }
  }

  def requestVote(requestVoteRequest: RequestVoteRequest): Future[RequestVoteResponse] = {
    val request = Request(http.Method.Post, "/requestVote")
    request.host(hostAddress.toString)
    request.setContentTypeJson()
    request.setContentString(finatraObjectMapper.writeValueAsString(requestVoteRequest))
    client.apply(request).map { response =>
      finatraObjectMapper.parse[RequestVoteResponse](response.getContentString())
    }
  }
}


case class AppendEntriesResponse(currentTerm: Long, success: Boolean)
case class RequestVoteResponse(currentTerm: Long, voteGranted: Boolean)