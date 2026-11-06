package com.wi.floatingpod.storage.mysql

import java.sql.SQLDataException
import java.util.Locale

/** 仅比较封闭 schema6 recipe 的 CHECK/generated expression 结构，不执行 SQL 或签发验收资格。 */
private[mysql] object MySqlManagedCheckExpression:
  import MySqlManagedStaticRecipe.Token
  import MySqlManagedStaticRecipe.TokenKind

  sealed trait Expression
  final case class Identifier(name: String) extends Expression
  final case class NumberLiteral(source: String) extends Expression
  final case class CharacterLiteral(characterSet: String, source: String) extends Expression
  case object NullLiteral extends Expression
  case object TrueLiteral extends Expression
  case object FalseLiteral extends Expression
  final case class FunctionCall(name: String, arguments: Vector[Expression]) extends Expression
  final case class Unary(operator: String, operand: Expression) extends Expression
  final case class Binary(operator: String, left: Expression, right: Expression) extends Expression
  final case class IsNull(operand: Expression, negated: Boolean) extends Expression
  final case class InList(operand: Expression, values: Vector[Expression], negated: Boolean) extends Expression
  final case class Between(operand: Expression, low: Expression, high: Expression, negated: Boolean) extends Expression
  final case class Cast(operand: Expression, targetType: String) extends Expression

  private val LiteralCharacterSet = "utf8mb4"
  private val OrPrecedence = 1
  private val AndPrecedence = 2
  private val NotPrecedence = 3
  private val ComparisonPrecedence = 4
  private val AdditivePrecedence = 5
  private val MultiplicativePrecedence = 6
  private val UnaryPrecedence = 7

  /** 解析 SOURCE token；裸字符串使用显式创建环境的字符集。schema6 目前仅批准 utf8mb4。 */
  def parseSource(tokens: Vector[Token], characterSet: String = LiteralCharacterSet): Expression =
    if tokens == null || tokens.isEmpty then reject("CHECK/generated expression tokens are required")
    val effectiveCharacterSet = canonicalCharacterSet(characterSet)
    if tokens.exists(_.kind == TokenKind.Comment) then reject("comments are outside the managed expression comparison grammar")
    new Parser(tokens, effectiveCharacterSet).parse()

  def parse(tokens: Vector[Token]): Expression = parseSource(tokens)

  def parseSql(sql: String, characterSet: String = LiteralCharacterSet): Expression =
    parseSource(MySqlManagedStaticRecipe.tokenize(sql), characterSet)

  /** 只接受 recipe 中的命名 CHECK 约束，并由括号平衡提取完整表达式。 */
  def parseCheckConstraint(tokens: Vector[Token], characterSet: String = LiteralCharacterSet): Expression =
    if tokens == null || tokens.size < 6 || !isWord(tokens.head, "CONSTRAINT") ||
        !isIdentifier(tokens(1)) || !isWord(tokens(2), "CHECK") || tokens(3) != symbol("(") then
      reject("compiled table CHECK constraint is outside the named CHECK grammar")
    val close = matchingParen(tokens, 3)
    if close != tokens.size - 1 then reject("compiled table CHECK has trailing unsupported syntax")
    parseSource(tokens.slice(4, close), characterSet)

  /** Main validator seam: recipe SOURCE expression versus one observed I_S expression. */
  def equivalent(expected: Vector[Token], actualSql: String, characterSet: String = LiteralCharacterSet): Boolean =
    parseSource(expected, characterSet) == parseSource(MySqlManagedStaticRecipe.tokenizeCheckCatalog(actualSql), characterSet)

  def equivalentSql(left: String, right: String): Boolean =
    parseSql(left) == parseSql(right)

  // MySQL 目录会按三值逻辑将 NOT(比较 AND/OR 比较) 下推；仅规范这些精确结构。
  // 不消除任意双重 NOT，不把普通数值操作数当作布尔值，不交换或重排操作数。
  // https://dev.mysql.com/doc/refman/9.7/en/logical-operators.html
  private def negated(value: Expression): Expression = value match
    case Binary("AND", left, right) => Binary("OR", negated(left), negated(right))
    case Binary("OR", left, right) => Binary("AND", negated(left), negated(right))
    case Binary("=", left, right) => Binary("<>", left, right)
    case Binary("<>", left, right) => Binary("=", left, right)
    case _ => Unary("NOT", value)

  private final class Parser(tokens: Vector[Token], characterSet: String):
    private var position = 0

    def parse(): Expression =
      val result = expression(0)
      if position != tokens.size then reject(s"unsupported expression token at position $position")
      result

    private def expression(minimumPrecedence: Int): Expression =
      var left = prefix()
      var continue = true
      while continue && position < tokens.size do
        infix match
          case Some((precedence, operator, width)) if precedence >= minimumPrecedence =>
            operator match
              case "IS NULL" | "IS NOT NULL" =>
                position += width
                left = IsNull(left, operator == "IS NOT NULL")
              case "IN" | "NOT IN" =>
                position += width
                left = InList(left, expressionList(), operator == "NOT IN")
              case "BETWEEN" | "NOT BETWEEN" =>
                position += width
                val low = expression(ComparisonPrecedence + 1)
                expectWord("AND")
                val high = expression(ComparisonPrecedence + 1)
                left = Between(left, low, high, operator == "NOT BETWEEN")
              case _ =>
                position += width
                val right = expression(precedence + 1)
                left = Binary(operator, left, right)
          case _ => continue = false
      left

    private def prefix(): Expression =
      if position >= tokens.size then reject("expression ended while an operand was required")
      val token = tokens(position)
      if token == symbol("(") then
        position += 1
        val nested = expression(0)
        expectSymbol(")")
        nested
      else if isWord(token, "NOT") then
        position += 1
        negated(expression(NotPrecedence))
      else if token.kind == TokenKind.Symbol && Set("+", "-", "!", "~").contains(token.value) then
        position += 1
        Unary(token.value, expression(UnaryPrecedence))
      else if token.kind == TokenKind.Number then
        position += 1
        NumberLiteral(token.value)
      else if token.kind == TokenKind.StringLiteral then
        position += 1
        CharacterLiteral(characterSet, token.value)
      else if isWord(token, "_UTF8MB4") then
        if position + 1 >= tokens.size || tokens(position + 1).kind != TokenKind.StringLiteral then
          reject("only the fixed _utf8mb4 string introducer is supported")
        position += 2
        CharacterLiteral(characterSet, tokens(position - 1).value)
      else if token.kind == TokenKind.Word && token.value.startsWith("_") &&
          position + 1 < tokens.size && tokens(position + 1).kind == TokenKind.StringLiteral then
        reject("non-utf8mb4 character-set introducer is unsupported")
      else if isWord(token, "NULL") then
        position += 1
        NullLiteral
      else if isWord(token, "TRUE") then
        position += 1
        TrueLiteral
      else if isWord(token, "FALSE") then
        position += 1
        FalseLiteral
      else if isWord(token, "CAST") then parseCast()
      else if isIdentifier(token) then parseIdentifierOrCall()
      else reject(s"unsupported expression token at position $position")

    private def parseIdentifierOrCall(): Expression =
      val token = tokens(position)
      val name = identifier(token)
      position += 1
      if takeSymbol("(") then
        // MySQL 目录使用 LENGTH/SUBSTR 同义内建函数；仅规范未引用函数调用，保留参数。
        // https://dev.mysql.com/doc/refman/9.7/en/string-functions.html#function_octet-length
        val functionName = if token.kind == TokenKind.Word then name match
          case "octet_length" => "length"
          case "substr" => "substring"
          case _ => name
        else name
        FunctionCall(functionName, arguments())
      else Identifier(name)

    private def parseCast(): Expression =
      position += 1
      expectSymbol("(")
      val operand = expression(0)
      expectWord("AS")
      if position >= tokens.size || tokens(position).kind != TokenKind.Word then
        reject("CAST target type must be an unquoted type keyword")
      val target = tokens(position).value.toUpperCase(Locale.ROOT)
      position += 1
      val canonicalTarget = target match
        case "BINARY" | "UNSIGNED" => target
        case "CHAR" =>
          // MySQL 的生成列目录将无长度 AS BINARY 呈现为 AS CHAR CHARSET BINARY。
          // 仅接受该精确形式，不能合并普通 CHAR、其他字符集或有截断/填充语义的长度。
          expectWord("CHARSET")
          expectWord("BINARY")
          "BINARY"
        case _ => reject("CAST target type is outside the actual schema6 expression grammar")
      expectSymbol(")")
      Cast(operand, canonicalTarget)

    private def arguments(): Vector[Expression] =
      if takeSymbol(")") then Vector.empty
      else
        val values = Vector.newBuilder[Expression]
        values += expression(0)
        while takeSymbol(",") do values += expression(0)
        expectSymbol(")")
        values.result()

    private def expressionList(): Vector[Expression] =
      expectSymbol("(")
      if takeSymbol(")") then reject("IN list must contain at least one expression")
      val values = Vector.newBuilder[Expression]
      values += expression(0)
      while takeSymbol(",") do values += expression(0)
      expectSymbol(")")
      values.result()

    private def infix: Option[(Int, String, Int)] =
      if position >= tokens.size then None
      else
        val token = tokens(position)
        if isWord(token, "OR") then Some((OrPrecedence, "OR", 1))
        else if isWord(token, "AND") then Some((AndPrecedence, "AND", 1))
        else if isWord(token, "IS") then
          if position + 1 < tokens.size && isWord(tokens(position + 1), "NOT") &&
              position + 2 < tokens.size && isWord(tokens(position + 2), "NULL") then Some((ComparisonPrecedence, "IS NOT NULL", 3))
          else if position + 1 < tokens.size && isWord(tokens(position + 1), "NULL") then Some((ComparisonPrecedence, "IS NULL", 2))
          else reject("IS expression must be followed by NULL or NOT NULL")
        else if isWord(token, "NOT") then
          if position + 1 < tokens.size && isWord(tokens(position + 1), "IN") then Some((ComparisonPrecedence, "NOT IN", 2))
          else if position + 1 < tokens.size && isWord(tokens(position + 1), "BETWEEN") then Some((ComparisonPrecedence, "NOT BETWEEN", 2))
          else None
        else if isWord(token, "IN") then Some((ComparisonPrecedence, "IN", 1))
        else if isWord(token, "BETWEEN") then Some((ComparisonPrecedence, "BETWEEN", 1))
        else token.kind match
          case TokenKind.Symbol if Set("=", "<>", "!=", "<=>", ">", ">=", "<", "<=").contains(token.value) =>
            Some((ComparisonPrecedence, token.value, 1))
          case TokenKind.Symbol if Set("+", "-").contains(token.value) => Some((AdditivePrecedence, token.value, 1))
          case TokenKind.Symbol if Set("*", "/", "%").contains(token.value) => Some((MultiplicativePrecedence, token.value, 1))
          case _ => None

    private def expectWord(value: String): Unit =
      if position >= tokens.size || !isWord(tokens(position), value) then reject(s"expected SQL keyword $value")
      position += 1

    private def expectSymbol(value: String): Unit =
      if position >= tokens.size || tokens(position) != symbol(value) then reject(s"expected SQL symbol $value")
      position += 1

    private def takeSymbol(value: String): Boolean =
      if position < tokens.size && tokens(position) == symbol(value) then { position += 1; true }
      else false

  private def matchingParen(tokens: Vector[Token], open: Int): Int =
    if open < 0 || open >= tokens.size || tokens(open) != symbol("(") then reject("SQL opening parenthesis is absent")
    var depth = 0
    var index = open
    while index < tokens.size do
      if tokens(index) == symbol("(") then depth += 1
      else if tokens(index) == symbol(")") then
        depth -= 1
        if depth == 0 then return index
        if depth < 0 then reject("SQL expression parentheses are malformed")
      index += 1
    reject("SQL expression has an unterminated parenthesis")

  private def identifier(token: Token): String =
    if isIdentifier(token) then token.value else reject("SQL identifier required")

  private def canonicalCharacterSet(value: String): String =
    if value == null then reject("CHECK expression character set is required")
    val normalized = value.map(c => if c >= 'A' && c <= 'Z' then (c + 32).toChar else c)
    if normalized != LiteralCharacterSet then reject("only the fixed schema6 utf8mb4 expression character set is supported")
    normalized

  private def isIdentifier(token: Token): Boolean = token.kind == TokenKind.Word || token.kind == TokenKind.QuotedWord
  private def isWord(token: Token, value: String): Boolean = token.kind == TokenKind.Word && token.value == value.toLowerCase(Locale.ROOT)
  private def symbol(value: String): Token = Token(TokenKind.Symbol, value)
  private def reject(message: String): Nothing = throw new SQLDataException(message)
