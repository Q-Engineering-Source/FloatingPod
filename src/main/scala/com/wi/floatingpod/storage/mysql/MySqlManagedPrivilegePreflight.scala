package com.wi.floatingpod.storage.mysql

import java.io.Reader
import java.sql.{ResultSet, SQLDataException}
import scala.util.Using

/** 在受管 schema 的任何变更前检查安装主体权限；不签发 Ready，也不修改账号或对象。 */
private[mysql] object MySqlManagedPrivilegePreflight:
  private val MaxGrantRows = 256
  private val MaxGrantChars = 8192
  private val MaxGrantTotalChars = 262144
  private val MaxIdentityChars = 512
  private val MaxPrivilegesPerGrant = 64
  private val MaxColumnsPerPrivilege = 256

  /** 只接受发布策略明示的完整管理代理元组，不从数据库观测反推批准。 */
  def requireApprovedAdministrativeProxy(profile: MySqlDeploymentReceiptSchemaProfile.Inputs,
      user: String, host: String, proxiedUser: String, proxiedHost: String,
      withGrant: java.lang.Integer): Unit =
    if profile == null then throw new IllegalArgumentException("explicit managed proxy policy required")
    if user == null || host == null || proxiedUser == null || proxiedHost == null || withGrant == null ||
      (withGrant.intValue() != 0 && withGrant.intValue() != 1) then
      reject("managed installation found malformed administrative proxy data")
    val approved = profile.trustedAdministrativeProxyRows.exists(row =>
      row.user == user && row.host == host && row.proxiedUser == proxiedUser &&
        row.proxiedHost == proxiedHost && row.withGrant == (withGrant.intValue() == 1))
    if !approved then reject("managed installation found proxy delegation outside the exact administrative allowlist")

  final class Result private[MySqlManagedPrivilegePreflight] (
      val effectivePrincipal: String,
      val observedGrantRows: Int,
      val requiredGrantChecks: Int):
    require(effectivePrincipal != null && effectivePrincipal.nonEmpty && observedGrantRows > 0 && requiredGrantChecks > 0,
      "managed privilege preflight result must be complete")
    override def toString: String = s"ManagedPrivilegePreflight(principalChecked=true,grants=$observedGrantRows,checks=$requiredGrantChecks)"

  private enum Scope:
    case Global, Schema, Table, Procedure

  private final case class Privilege(name: String, columns: Vector[String])
  private final case class Grant(scope: Scope, schema: String, objectName: String,
      principal: String, privileges: Vector[Privilege], grantable: Boolean)
  private final case class Requirement(scope: Scope, schema: String, objectName: String,
      privilege: String, columns: Vector[String], grantOptionRequired: Boolean)

  private val StaticPrivilegeNames = Set(
    "ALL PRIVILEGES", "ALTER", "ALTER ROUTINE", "CREATE", "CREATE ROLE", "CREATE ROUTINE", "CREATE TABLESPACE",
    "CREATE TEMPORARY TABLES", "CREATE USER", "CREATE VIEW", "DELETE", "DROP", "DROP ROLE", "EVENT",
    "EXECUTE", "FILE", "INDEX", "INSERT", "LOCK TABLES", "PROCESS", "PROXY", "REFERENCES", "RELOAD",
    "REPLICATION CLIENT", "REPLICATION SLAVE", "SELECT", "SHOW DATABASES", "SHOW VIEW", "SHUTDOWN", "TRIGGER",
    "UPDATE", "USAGE"
  )

  /** 必须由初始化 owner 的原线程、在持有目标锁时调用。 */
  def inspect(owner: ManagedMySqlConnectionOwner, rendered: MySqlDbFirstSchemaProfile.Rendered): Result =
    if owner == null || rendered == null then throw new IllegalArgumentException("managed privilege preflight inputs are required")
    val plan = MySqlManagedSchemaPlan.build(rendered)
    owner.requireHeldInitializationLock()
    val (roles, principal) = readSessionIdentity(owner)
    if roles != "NONE" then reject("Managed installer has active roles; effective role closure is not supported")
    val expectedPrincipal = owner.identity().effectiveUser
    if principal != expectedPrincipal then reject("Managed installer effective account changed during privilege preflight")

    owner.requireHeldInitializationLock()
    val grants = readShowGrants(owner)
    owner.requireHeldInitializationLock()
    verifyPlan(plan, principal, roles, grants)

  private def readSessionIdentity(owner: ManagedMySqlConnectionOwner): (String, String) =
    val connection = owner.connection()
    Using.resource(connection.prepareStatement("SELECT CURRENT_ROLE(), CURRENT_USER()")) { statement =>
      statement.setQueryTimeout(owner.queryTimeoutSeconds())
      Using.resource(statement.executeQuery()) { rows =>
        if rows.getMetaData.getColumnCount != 2 then reject("Managed installer identity query returned an unexpected shape")
        if !rows.next() then reject("Managed installer identity is missing")
        val roles = readBounded(rows, 1, MaxIdentityChars, "effective roles")
        val principal = readBounded(rows, 2, MaxIdentityChars, "effective account")
        if rows.next() then reject("Managed installer identity is ambiguous")
        (roles, principal)
      }
    }

  private def readShowGrants(owner: ManagedMySqlConnectionOwner): Vector[String] =
    val connection = owner.connection()
    Using.resource(connection.createStatement()) { statement =>
      statement.setQueryTimeout(owner.queryTimeoutSeconds())
      Using.resource(statement.executeQuery("SHOW GRANTS")) { rows =>
        if rows.getMetaData.getColumnCount != 1 then reject("SHOW GRANTS returned an unexpected shape")
        val grants = Vector.newBuilder[String]
        var count = 0
        var total = 0
        while rows.next() do
          count += 1
          if count > MaxGrantRows then reject("SHOW GRANTS exceeds the row bound")
          val text = readBounded(rows, 1, MaxGrantChars, "grant row")
          total = Math.addExact(total, text.length)
          if total > MaxGrantTotalChars then reject("SHOW GRANTS exceeds the total text bound")
          grants += text
        if count == 0 then reject("SHOW GRANTS is empty")
        grants.result()
      }
    }

  private def readBounded(rows: ResultSet, column: Int, maximum: Int, label: String): String =
    val reader = rows.getCharacterStream(column)
    if reader == null then reject("Managed installer " + label + " is NULL")
    Using.resource(reader) { input =>
      val text = new java.lang.StringBuilder(math.min(maximum, 512))
      val block = new Array[Char](512)
      try
        var size = 0
        var read = input.read(block)
        while read != -1 do
          if read == 0 then reject("Managed installer " + label + " reader made no progress")
          size = Math.addExact(size, read)
          if size > maximum then reject("Managed installer " + label + " exceeds its text bound")
          text.append(block, 0, read)
          read = input.read(block)
        text.toString
      finally java.util.Arrays.fill(block, '\u0000')
    }

  private[mysql] def verifyObserved(
      rendered: MySqlDbFirstSchemaProfile.Rendered,
      expectedPrincipal: String,
      currentRole: String,
      observed: Vector[String]): Result =
    if rendered == null then
      throw new IllegalArgumentException("managed privilege observation is incomplete")
    verifyPlan(MySqlManagedSchemaPlan.build(rendered), expectedPrincipal, currentRole, observed)

  private def verifyPlan(
      plan: MySqlManagedSchemaPlan.Plan,
      expectedPrincipal: String,
      currentRole: String,
      observed: Vector[String]): Result =
    if expectedPrincipal == null || expectedPrincipal.isEmpty || currentRole == null || observed == null then
      throw new IllegalArgumentException("managed privilege observation is incomplete")
    if currentRole != "NONE" then reject("Managed installer has active roles; effective role closure is not supported")
    if observed.isEmpty || observed.size > MaxGrantRows then reject("SHOW GRANTS is empty or exceeds the row bound")
    var total = 0
    observed.foreach { row =>
      if row == null || row.length > MaxGrantChars then reject("SHOW GRANTS contains a missing or overlong row")
      total = Math.addExact(total, row.length)
      if total > MaxGrantTotalChars then reject("SHOW GRANTS exceeds the total text bound")
    }
    val grants = observed.map(parseGrant(_, expectedPrincipal))
    val requirements = requirementsFor(plan)
    requirements.foreach { required =>
      if !grants.exists(granted => covers(granted, required)) then
        val suffix = if required.grantOptionRequired then " with GRANT OPTION for the renderer grant" else ""
        reject("Managed installer lacks " + required.privilege + " at its required scope" + suffix)
    }
    new Result(expectedPrincipal, grants.size, requirements.size)

  private def requirementsFor(plan: MySqlManagedSchemaPlan.Plan): Vector[Requirement] =
    val schema = plan.schema
    val localDdl = Vector("CREATE", "ALTER", "INDEX", "REFERENCES", "TRIGGER", "CREATE ROUTINE", "ALTER ROUTINE", "EXECUTE")
      .map(privilege => Requirement(Scope.Schema, schema, "*", privilege, Vector.empty, grantOptionRequired = false))
    // MySQL 以 CREATE USER 权限控制账号创建及本计划随后对 runtime 账号执行的 ALTER USER。
    val accountManagement = Vector("CREATE USER").map(privilege =>
      Requirement(Scope.Global, "*", "*", privilege, Vector.empty, grantOptionRequired = false))
    val definer = Vector(Requirement(Scope.Global, "*", "*", "SET_ANY_DEFINER", Vector.empty, grantOptionRequired = false))
    val rendererGrants = plan.steps.filter(_.kind == MySqlManagedSchemaPlan.Kind.PrivilegeGrant).flatMap { step =>
      val grant = parseGrant(step.sql, expectedPrincipal = null, allowOtherPrincipal = true)
      grant.privileges.map(privilege => Requirement(grant.scope, grant.schema, grant.objectName,
        privilege.name, privilege.columns, grantOptionRequired = true))
    }
    (localDdl ++ accountManagement ++ definer ++ rendererGrants).distinct

  /** 仅解析封闭 renderer plan 的实际 GRANT 步骤，不扫描或筛选任意 SQL 子串。 */
  private[mysql] def parseGrantForTest(line: String): (String, Vector[(String, Vector[String])], Boolean) =
    val grant = parseGrant(line, expectedPrincipal = null, allowOtherPrincipal = true)
    val scope = grant.scope match
      case Scope.Global => "GLOBAL"
      case Scope.Schema => "SCHEMA:" + grant.schema
      case Scope.Table => "TABLE:" + grant.schema + "." + grant.objectName
      case Scope.Procedure => "PROCEDURE:" + grant.schema + "." + grant.objectName
    (scope, grant.privileges.map(value => value.name -> value.columns), grant.grantable)

  private def parseGrant(line: String, expectedPrincipal: String, allowOtherPrincipal: Boolean = false): Grant =
    if line == null || line.length > MaxGrantChars then reject("SHOW GRANTS contains an unsupported or overlong row")
    val parser = new GrantLine(line)
    val grant = parser.read()
    if !allowOtherPrincipal && grant.principal != expectedPrincipal then reject("SHOW GRANTS contains a different effective account")
    grant

  private def covers(source: Grant, required: Requirement): Boolean =
    if required.grantOptionRequired && !source.grantable then return false
    val targetMatches = source.scope match
      case Scope.Global => true
      case Scope.Schema => source.schema == required.schema && required.scope != Scope.Global
      case Scope.Table => required.scope == Scope.Table && source.schema == required.schema && source.objectName == required.objectName
      case Scope.Procedure => required.scope == Scope.Procedure && source.schema == required.schema && source.objectName == required.objectName
    if !targetMatches then return false
    source.privileges.exists { granted =>
      val privilegeMatches = granted.name == required.privilege ||
        granted.name == "ALL PRIVILEGES" && !isDynamicPrivilege(required.privilege)
      if !privilegeMatches then false
      else if required.columns.isEmpty then granted.columns.isEmpty
      else granted.columns.isEmpty || required.columns.forall(granted.columns.contains)
    }

  private def isDynamicPrivilege(name: String): Boolean = name.matches("[A-Z][A-Z0-9_]*") && name.contains('_')

  private def splitTopLevel(value: String, delimiter: Char, maximumParts: Int): Vector[String] =
    val parts = Vector.newBuilder[String]
    var count = 0
    var start = 0
    var depth = 0
    var quote: Char = 0.toChar
    var index = 0
    while index < value.length do
      val character = value.charAt(index)
      if character == '\\' then reject("SHOW GRANTS backslash escapes are unsupported")
      if quote != 0.toChar then
        if character == quote then
          if index + 1 < value.length && value.charAt(index + 1) == quote then index += 1
          else quote = 0.toChar
      else if character == '`' || character == '\'' then quote = character
      else if character == '(' then depth += 1
      else if character == ')' then
        depth -= 1
        if depth < 0 then reject("SHOW GRANTS has unmatched parentheses")
      else if character == delimiter && depth == 0 then
        count += 1
        if count >= maximumParts then reject("SHOW GRANTS item count exceeds its bound")
        parts += value.substring(start, index).trim
        start = index + 1
      index += 1
    if quote != 0.toChar || depth != 0 then reject("SHOW GRANTS has unclosed quoting or parentheses")
    count += 1
    if count > maximumParts then reject("SHOW GRANTS item count exceeds its bound")
    parts += value.substring(start).trim
    val result = parts.result()
    if result.exists(_.isEmpty) then reject("SHOW GRANTS contains an empty privilege item")
    result

  private def parsePrivilege(value: String): Privilege =
    val trimmed = value.trim
    val open = trimmed.indexOf('(')
    val (name, columns) = if open >= 0 then
      if !trimmed.endsWith(")") || trimmed.indexOf('(', open + 1) >= 0 then reject("SHOW GRANTS column privilege syntax is unsupported")
      val privilege = trimmed.substring(0, open).trim.toUpperCase(java.util.Locale.ROOT)
      if privilege != "UPDATE" && privilege != "SELECT" && privilege != "INSERT" && privilege != "REFERENCES" then
        reject("SHOW GRANTS column privilege is unsupported")
      val columnText = trimmed.substring(open + 1, trimmed.length - 1)
      val parsed = splitTopLevel(columnText, ',', MaxColumnsPerPrivilege).map(parseIdentifier)
      if parsed.isEmpty || parsed.size > MaxColumnsPerPrivilege || parsed.distinct.size != parsed.size then
        reject("SHOW GRANTS column list is invalid")
      (privilege, parsed)
    else (trimmed.toUpperCase(java.util.Locale.ROOT), Vector.empty[String])
    val safeStaticToken = name.matches("[A-Z]+(?: [A-Z]+)*")
    if !StaticPrivilegeNames(name) && !safeStaticToken && !isDynamicPrivilege(name) then
      reject("SHOW GRANTS contains an unsupported privilege token")
    if columns.nonEmpty && name != "UPDATE" && name != "SELECT" && name != "INSERT" && name != "REFERENCES" then
      reject("SHOW GRANTS column privilege is unsupported")
    Privilege(name, columns)

  private def parseIdentifier(value: String): String =
    val text = value.trim
    val result = if text.matches("[A-Za-z0-9_]{1,64}") then text
    else if text.length >= 3 && text.head == '`' && text.last == '`' then text.substring(1, text.length - 1)
    else reject("SHOW GRANTS identifier quoting is unsupported")
    if result.isEmpty || result.length > 256 || result.exists(c => c == '`' || c == '\\' || Character.isISOControl(c)) then
      reject("SHOW GRANTS identifier is invalid")
    result

  private def reject(message: String): Nothing = throw new SQLDataException(message)

  private final class GrantLine(input: String):
    private var at = 0
    private def whitespace(): Unit =
      while at < input.length && input.charAt(at).isWhitespace do at += 1
    private def consume(character: Char): Boolean =
      whitespace()
      if at < input.length && input.charAt(at) == character then
        at += 1
        true
      else false
    private def keyword(value: String): Unit =
      whitespace()
      val end = at + value.length
      if end > input.length || !input.regionMatches(true, at, value, 0, value.length) ||
          end < input.length && (input.charAt(end).isLetterOrDigit || input.charAt(end) == '_') then
        reject("SHOW GRANTS syntax is unsupported")
      at = end
    private def identifier(account: Boolean, schemaPattern: Boolean = false): String =
      whitespace()
      if at >= input.length then reject("SHOW GRANTS identifier is missing")
      val quote = input.charAt(at)
      if quote != '`' && (!account || quote != '\'') then reject("SHOW GRANTS quoted identifier is required")
      at += 1
      val output = new java.lang.StringBuilder()
      var ended = false
      while at < input.length && !ended do
        val character = input.charAt(at)
        at += 1
        if character == '\\' then
          // Schema-level grants use MySQL patterns. Preserve only literal wildcard escapes
          // until the target is confirmed as schema.*, never for accounts or object names.
          if !schemaPattern || at >= input.length || (input.charAt(at) != '_' && input.charAt(at) != '%') then
            reject("SHOW GRANTS backslash escape is unsupported at this target")
          output.append(character).append(input.charAt(at))
          at += 1
        else if character == quote then
          if at < input.length && input.charAt(at) == quote then
            output.append(quote)
            at += 1
          else ended = true
        else output.append(character)
        if output.length() > 256 then reject("SHOW GRANTS identifier exceeds its bound")
      if !ended then reject("SHOW GRANTS contains an unterminated quoted identifier")
      if output.length() == 0 then reject("SHOW GRANTS identifier is empty")
      output.toString
    private def dottedIdentifier(): (String, String) =
      val left = identifier(account = false)
      if !consume('.') then reject("SHOW GRANTS target is malformed")
      val right = identifier(account = false)
      (left, right)

    def read(): Grant =
      keyword("GRANT")
      val start = at
      var end = -1
      var cursor = at
      var depth = 0
      var quote: Char = 0.toChar
      while cursor < input.length && end < 0 do
        val character = input.charAt(cursor)
        if character == '\\' then reject("SHOW GRANTS backslash escapes are unsupported")
        if quote != 0.toChar then
          if character == quote then
            if cursor + 1 < input.length && input.charAt(cursor + 1) == quote then cursor += 1
            else quote = 0.toChar
        else if character == '`' || character == '\'' then quote = character
        else if character == '(' then depth += 1
        else if character == ')' then depth -= 1
        else if depth == 0 && cursor + 2 <= input.length && input.regionMatches(true, cursor, "ON", 0, 2) &&
            (cursor == 0 || input.charAt(cursor - 1).isWhitespace) &&
            (cursor + 2 == input.length || input.charAt(cursor + 2).isWhitespace) then end = cursor
        cursor += 1
      if end < 0 || quote != 0.toChar || depth != 0 then
        reject("SHOW GRANTS role grants and unsupported syntax are not accepted")
      val privileges = splitTopLevel(input.substring(start, end), ',', MaxPrivilegesPerGrant).map(parsePrivilege)
      at = end
      keyword("ON")
      whitespace()
      var scope = Scope.Table
      var schema = ""
      var objectName = ""
      if at < input.length && input.charAt(at) == '*' then
        at += 1
        if !consume('.') || !consume('*') then reject("SHOW GRANTS global target is malformed")
        scope = Scope.Global
        schema = "*"
        objectName = "*"
      else if input.regionMatches(true, at, "PROCEDURE", 0, 9) &&
          (at + 9 == input.length || input.charAt(at + 9).isWhitespace) then
        keyword("PROCEDURE")
        val (database, routine) = dottedIdentifier()
        scope = Scope.Procedure
        schema = database
        objectName = routine
      else
        val database = identifier(account = false, schemaPattern = true)
        if !consume('.') then reject("SHOW GRANTS target is malformed")
        if consume('*') then
          scope = Scope.Schema
          schema = database.replace("\\_", "_").replace("\\%", "%")
          objectName = "*"
        else
          if database.contains('\\') then reject("SHOW GRANTS pattern escape is only supported for schema-level grants")
          schema = database
          objectName = identifier(account = false)
      keyword("TO")
      val user = identifier(account = true)
      if !consume('@') then reject("SHOW GRANTS account target is malformed")
      val host = identifier(account = true)
      val principal = user + "@" + host
      whitespace()
      var grantable = false
      val terminatedWithoutOption = consume(';')
      if !terminatedWithoutOption && at < input.length then
        keyword("WITH")
        keyword("GRANT")
        keyword("OPTION")
        grantable = true
        consume(';')
      whitespace()
      if at != input.length then reject("SHOW GRANTS trailing syntax is unsupported")
      if privileges.isEmpty || privileges.exists(_.name == "USAGE") && privileges.size != 1 then
        reject("SHOW GRANTS privilege record is invalid")
      Grant(scope, schema, objectName, principal, privileges, grantable)
