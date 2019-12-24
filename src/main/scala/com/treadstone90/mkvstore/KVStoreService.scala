package com.treadstone90.mkvstore
import com.twitter.finagle.http.Request
import com.twitter.finatra.http.routing.HttpRouter
import com.twitter.finatra.http.{Controller, HttpServer}

object KVStoreServiceMain extends KVStoreService

class KVStoreService extends HttpServer {
  val controller = new KVStoreController(KVStore.open(???))
  override protected def configureHttp(router: HttpRouter): Unit = {
    router.add(controller)
  }
}

class KVStoreController(kvStore: KVStore[String]) extends Controller {
  get("/get") { request: Request =>
    val key = request.getParam("key")
    kvStore.get(key)
  }

  put("/put") { request: KVPutRequest =>
    val valueBody = request.payload.getBytes()
    kvStore.write(request.key, valueBody)
  }
}

case class KVPutRequest(key: String, payload: String)