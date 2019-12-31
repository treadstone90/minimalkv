package com.treadstone90.mkvstore.raft

import java.util.concurrent.Executors

import com.google.common.eventbus.{AsyncEventBus, EventBus}
import com.google.inject.{Module, Provides, Singleton}
import com.twitter.finagle.http.{Request, Response}
import com.twitter.finatra.http.HttpServer
import com.twitter.finatra.http.filters.{CommonFilters, LoggingMDCFilter, TraceIdMDCFilter}
import com.twitter.finatra.http.routing.HttpRouter
import com.twitter.inject.TwitterPrivateModule

object RaftSeverMain extends RaftSever

class RaftSever extends HttpServer {


object EventBusModule extends TwitterPrivateModule {
  override def configure(): Unit = {
    bind[EventBus].to[AsyncEventBus]
    expose[EventBus]
  }

  @Provides
  @Singleton
  def provideEventbus: AsyncEventBus = {
    new AsyncEventBus(Executors.newFixedThreadPool(1))
  }
}
  override def modules: Seq[Module] = Seq(
    RaftStateModule,
    EventBusModule
  )

  override def configureHttp(router: HttpRouter): Unit = {
    router
      .filter[LoggingMDCFilter[Request, Response]]
      .filter[TraceIdMDCFilter[Request, Response]]
      .filter[CommonFilters]
      .add[RaftController]
  }

  override protected def setup(): Unit = {
    val raftState = injector.instance[RaftState]
    raftState.startAsync()
    raftState.awaitRunning()
    val eventBus = injector.instance[EventBus]
    val stateManager = injector.instance[StateManager]
    eventBus.register(stateManager)
  }

  override protected def start(): Unit = {
    val stateManager: StateManager = injector.instance[StateManager]
    println("transitionng to follower")
    stateManager.transitionTo(Follower)
  }
}
