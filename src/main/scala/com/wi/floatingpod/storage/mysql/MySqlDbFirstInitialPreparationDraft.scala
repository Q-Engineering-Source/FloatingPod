package com.wi.floatingpod.storage.mysql

import java.sql.SQLDataException
import java.time.LocalDateTime

/** 原Work登记后才分配的准备DATA草稿。domainConstruction尚缺原O/R/W，不是完整事务target或proof。 */
private[mysql] final class MySqlDbFirstInitialPreparationDraft private[mysql] (
    val reservation: MySqlDbFirstInitialPreparationReservation,
    val root: MySqlFormat3ReadBudget, val plannedAt: LocalDateTime):
  private var construction:Vector[MySqlFormat3RelationSnapshot]=null
  private var originalBefore:MySqlScriptSchemaSnapshot=null
  private var completed=false

  private[mysql] def before:MySqlScriptSchemaSnapshot =
    reservation.core.requireExecutingPreparationDraft(this)
    require(completed && originalBefore != null,"original preparation DATA construction incomplete")
    originalBefore

  private[mysql] def domainConstruction:Vector[MySqlFormat3RelationSnapshot] =
    reservation.core.requireExecutingPreparationDraft(this)
    require(completed && construction != null,"original preparation DATA construction incomplete")
    construction

  private[mysql] def construct(source:MySqlScriptSchemaSnapshot):Unit =
    reservation.core.requireExecutingPreparationDraft(this)
    require(construction == null && !completed && plannedAt != null && plannedAt.getYear>=1000 &&
      plannedAt.getYear<=9999 && plannedAt.getNano%1000==0,"one-shot preparation and exact DATETIME6 required")
    reservation.core.requirePreparationDraftSource(this,source)
    require(source != null && !source.allocationBudget.isClosed && source.allocationBudget.sharesAllocationLedger(root),
      "original preparation DATA source must remain live on shared ledger")
    root.requireScriptDataInputs(source.inputs)
    MySqlRecoveryCanonicalBytes.withAllocationBudget(root) {
      root.reserveRetainedMemory(Math.addExact(4096L,Math.multiplyExact(source.rowCount,256L)),
        "preparation complete before containers BEFORE allocation")
      construction=source.relations.map { relation =>
        val rows=relation.rows.map { row =>
          root.reserveRetainedMemory(256L+96L*row.size,"preparation copied row BEFORE allocation")
          row.map {
            case MySqlFormat3Cell.Binary(bytes)=>
              MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(bytes.copyBytes(root,"preparation original before binary copy"),root))
            case cell @ MySqlFormat3Cell.Text(value)=>
              root.reserveRetainedMemory(MySqlFormat3Utf8.byteLength(value),"preparation retained before text")
              cell
            case cell=>cell
          }
        }
        MySqlFormat3RelationSnapshot(relation.table,rows)
      }
      originalBefore=MySqlScriptSchemaSnapshot.captureMaterialized(source.schema,source.connection,source.observationScope,
        source.maxAllowedPacketBytes,source.inputs,construction,root)
      construction=MySqlDbFirstInitialPreparationRows.domain(originalBefore,reservation.request,
        reservation.tag.operationId,plannedAt,root)
      completed=true
    }

private[mysql] object MySqlDbFirstInitialPreparationDraft:
  val HolderBytes=512L
