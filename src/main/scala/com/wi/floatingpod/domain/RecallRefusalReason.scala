package com.wi.floatingpod.domain

/** 原Main确定的pre-capture业务拒绝原因；持久ID独立于network wire，禁止从Throwable文本推导。 */
enum RecallRefusalReason(val canonicalId:Int):
  case NotOwner extends RecallRefusalReason(1)
  case VersionMismatch extends RecallRefusalReason(2)
  case NotActive extends RecallRefusalReason(3)
  case WrongDimension extends RecallRefusalReason(4)
  case UnknownPod extends RecallRefusalReason(5)
  case Busy extends RecallRefusalReason(6)
