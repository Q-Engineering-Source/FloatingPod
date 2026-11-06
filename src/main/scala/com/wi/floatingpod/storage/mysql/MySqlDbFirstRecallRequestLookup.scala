package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.domain.RecallRequestNamespace
import java.util.UUID

/** 当前DATA的只读查询结果；NotFound不发行absence、fresh接纳或重执行资格。
  * namespace不匹配是正常拒绝，不以异常污染原Main读通道。
  */
private[floatingpod] enum MySqlDbFirstRecallRequestLookup:
  case NamespaceMismatch(actual:RecallRequestNamespace)
  case NotFound(actual:RecallRequestNamespace)
  case Found(actual:RecallRequestNamespace,original:MySqlDbFirstRecallRequestRecord)

private[floatingpod] object MySqlDbFirstRecallRequestLookup:
  private def decide(actual:RecallRequestNamespace,requested:RecallRequestNamespace)(
      read: => Option[MySqlDbFirstRecallRequestRecord]):MySqlDbFirstRecallRequestLookup =
    require(actual!=null && requested!=null,"actual and supplied recall namespace required")
    if actual!=requested then MySqlDbFirstRecallRequestLookup.NamespaceMismatch(actual)
    else read match
      case Some(entry)=>MySqlDbFirstRecallRequestLookup.Found(actual,entry)
      case None=>MySqlDbFirstRecallRequestLookup.NotFound(actual)
  private[mysql] def client(actual:RecallRequestNamespace,requested:RecallRequestNamespace,request:UUID,
      index:MySqlDbFirstRecallRequestIndex):MySqlDbFirstRecallRequestLookup =
    require(index!=null && request!=null && request!=new UUID(0L,0L),"actual index and nonzero client request required")
    decide(actual,requested) { index.client(actual.bindingUuid,actual.actorUuid,request) }
  private[mysql] def original(actual:RecallRequestNamespace,requested:RecallRequestNamespace,operation:UUID,
      index:MySqlDbFirstRecallRequestIndex):MySqlDbFirstRecallRequestLookup =
    require(index!=null && operation!=null && operation!=new UUID(0L,0L),"actual index and nonzero original operation required")
    decide(actual,requested) { index.original(actual.bindingUuid,actual.actorUuid,operation) }
