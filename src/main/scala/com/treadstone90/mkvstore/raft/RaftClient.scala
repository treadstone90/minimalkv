package com.treadstone90.mkvstore.raft

import com.google.common.net.HostAndPort
import com.google.inject.Inject
import com.twitter.conversions.DurationOps._
import com.twitter.finagle.http.Request
import com.twitter.finagle.liveness.{FailureAccrualFactory, FailureAccrualPolicy}
import com.twitter.finagle.service.Backoff
import com.twitter.finagle.{Http, Service, http}
import com.twitter.finatra.json.FinatraObjectMapper
import com.twitter.util.Future

class RaftClient @Inject() (hostAddress: HostAndPort,
                            finatraObjectMapper: FinatraObjectMapper) {
  val client: Service[http.Request, http.Response] =
    Http.client.configured(FailureAccrualFactory.Param(() => FailureAccrualPolicy.consecutiveFailures(
      numFailures = Integer.MAX_VALUE,
      markDeadFor = Backoff.const(100.milliseconds)
    )))
      .newService(hostAddress.toString)

  def appendEntries(appendEntriesEvent: AppendEntriesRequest): Future[AppendEntriesResponse] = {
    val request = Request(http.Method.Post, "/appendEntries")
    request.host(hostAddress.getHost)
    request.setContentTypeJson()
    request.setContentString(finatraObjectMapper.writeValueAsString(appendEntriesEvent))
    client.apply(request).map { response =>
      finatraObjectMapper.parse[AppendEntriesResponse](response.getContentString())
    }
  }

  def sendHeartBeat(heartBeatEvent: HeartBeatEvent): Future[AppendEntriesResponse] = {
    val appendEntriesRequest = AppendEntriesRequestWrapper(None, Some(heartBeatEvent))
    val request = Request(http.Method.Post, "/appendEntries")
    request.host(hostAddress.getHost)
    request.setContentTypeJson()
    request.setContentString(finatraObjectMapper.writeValueAsString(appendEntriesRequest))
    client.apply(request).map { response =>
      finatraObjectMapper.parse[AppendEntriesResponse](response.getContentString())
    }
  }

  def requestVote(requestVoteRequest: RequestVoteRequest): Future[RequestVoteResponse] = {
    val request = Request(http.Method.Post, "/requestVote")
    request.host(hostAddress.getHost)
    request.setContentTypeJson()
    request.setContentString(finatraObjectMapper.writeValueAsString(requestVoteRequest))
    client.apply(request).map { response =>
      finatraObjectMapper.parse[RequestVoteResponse](response.getContentString())
    }
  }
}


case class AppendEntriesResponse(currentTerm: Long, success: Boolean, processId: String)
case class RequestVoteResponse(currentTerm: Long, voteGranted: Boolean)