package com.wi.floatingpod.domain

import java.util.UUID

/** 稳定绑定与认证actor的请求命名空间；客户端持有此值本身不证明认证或SQL权限。 */
final case class RecallRequestNamespace(bindingUuid:UUID,actorUuid:UUID):
  require(bindingUuid!=null && actorUuid!=null && bindingUuid!=RecallRequestNamespace.Zero &&
    actorUuid!=RecallRequestNamespace.Zero,"nonzero recall binding and actor required")

object RecallRequestNamespace:
  private val Zero=new UUID(0L,0L)
