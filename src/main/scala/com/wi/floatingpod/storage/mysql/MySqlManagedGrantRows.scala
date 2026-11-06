package com.wi.floatingpod.storage.mysql

import java.sql.SQLDataException

/** 仅将封闭SOURCE授权步骤映射到独立发布预期行，不读取或修改数据库权限。 */
private[mysql] object MySqlManagedGrantRows:
  /** semantic 会规范权限列的行序；JDBC 描述、值与重数仍须精确相同。 */
  def sameSecurityData(expected: Vector[MySqlAuditResultSet], actual: Vector[MySqlAuditResultSet]): Boolean =
    expected.size == actual.size && expected.zip(actual).forall { (before, after) =>
      before.id == after.id && before.columns == after.columns && before.rows.size == after.rows.size &&
        // 不生成去重集合，避免隐藏重复/缺失行；有界行数内逐行比对重数，也不新增排序副本。
        before.rows.forall(row => before.rows.count(_ == row) == after.rows.count(_ == row))
    }

  def forStep(sql: String, expected: Vector[Vector[Vector[String]]]): Vector[Vector[Vector[String]]] =
    val pattern = "(?s)^GRANT (.+) ON (.+) TO '([A-Za-z0-9_]{1,32})'@'([A-Za-z0-9_.:%-]{1,255})'$".r
    sql match
      case pattern(privText, target, user, host) =>
        val account = s"'$user'@'$host'"
        val columnUpdate = "^UPDATE \\(([^)]*)\\)$".r
        val (privileges, columns) = privText match
          case columnUpdate(names) =>
            val selected = names.split(",",-1).map(_.trim).toVector
            if selected.exists(!_.matches("[a-zA-Z_][a-zA-Z0-9_]*")) || selected.distinct.size != selected.size then
              throw new SQLDataException("managed column GRANT differs from the closed plan syntax")
            (Set("UPDATE"),Some(selected.toSet))
          case _ => (privText.split(",",-1).map(_.trim).toSet,None)
        val direct = Vector.tabulate(4) { setIndex =>
          expected(setIndex).filter { row =>
            val rowGrantee = row.headOption.getOrElse("")
            val rowPrivilege = setIndex match
              case 0 => row.lift(4).getOrElse("")
              case 1 => row.lift(5).getOrElse("")
              case 2 => row.lift(3).getOrElse("")
              case _ => row.lift(2).getOrElse("")
            val rowSchema = setIndex match
              case 0 => row.lift(2).getOrElse("")
              case 1 => row.lift(2).getOrElse("")
              case 2 => row.lift(2).getOrElse("")
              case _ => ""
            val rowTable = setIndex match
              case 0 => row.lift(3).getOrElse("")
              case 1 => row.lift(3).getOrElse("")
              case _ => ""
            val targetMatches = target match
              case s"`$schema`.*" => setIndex == 2 && rowSchema == schema
              case s"`$schema`.`$table`" => (setIndex == 0 || setIndex == 1) && rowSchema == schema && rowTable == table
              case "*.*" => setIndex == 3
              case _ => false
            // I_S 每个 COLUMN_NAME 各一行；列级授权不能由表级 UPDATE 代替，反向也不成立。
            val scopeMatches = columns match
              case Some(selected) => setIndex == 1 && selected.contains(row.lift(4).getOrElse(""))
              case None => setIndex != 1
            rowGrantee == account && privileges.contains(rowPrivilege) && targetMatches && scopeMatches
          }
        }
        direct
      case _ => throw new SQLDataException("managed GRANT differs from the closed original plan syntax")

