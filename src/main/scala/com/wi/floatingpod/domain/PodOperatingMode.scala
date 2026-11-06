package com.wi.floatingpod.domain

/** 服务端权威的行为模式；不从所有权或活动状态推导。 */
enum PodOperatingMode:
  case Normal, WorldIdle

  /** SQLite 及迁移包使用各自固定、显式的 wire token。 */
  def databaseValue: String = this match
    case PodOperatingMode.Normal => "NORMAL"
    case PodOperatingMode.WorldIdle => "WORLD_IDLE"

  def exportValue: String = this match
    case PodOperatingMode.Normal => "Normal"
    case PodOperatingMode.WorldIdle => "WorldIdle"

object PodOperatingMode:
  def fromDatabase(value: String): PodOperatingMode = value match
    case "NORMAL" => PodOperatingMode.Normal
    case "WORLD_IDLE" => PodOperatingMode.WorldIdle
    case _ => throw new IllegalStateException(s"Unknown persisted FloatingPod operating mode $value")

  def fromExportValue(value: String): PodOperatingMode = value match
    case "Normal" => PodOperatingMode.Normal
    case "WorldIdle" => PodOperatingMode.WorldIdle
    case _ => throw new IllegalArgumentException(s"Unknown migration operating_mode $value")
