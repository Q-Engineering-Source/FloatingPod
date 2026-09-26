/*
 * FloatingPod 对 Molang 求值表面的独立实现。
 * 因上游 mclib 的许可证无法取得，本文件未复制任何第三方代码。
 */
package com.wi.floatingpod.vendored.geckolib3.core.molang;

import com.google.gson.JsonElement;
import com.wi.floatingpod.vendored.mclib.math.IValue;
import com.wi.floatingpod.vendored.mclib.math.Variable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.DoubleBinaryOperator;
import java.util.function.DoubleUnaryOperator;

/**
 * 解析并延迟求值 GeckoLib 动画链使用的 Molang 数值表达式。
 */
public final class MolangParser {

    /** 已登记变量按名称索引，表达式捕获变量对象以便读取后续更新。 */
    private final Map<String, Variable> variables = new HashMap<>();

    /**
     * 登记变量。重名变量会导致表达式绑定不确定，因此立即失败。
     *
     * @param variable 待登记变量
     */
    public void register(Variable variable) {
        Variable nonNullVariable = Objects.requireNonNull(variable, "variable");
        Variable existing = variables.putIfAbsent(nonNullVariable.getName(), nonNullVariable);
        if (existing != null) {
            throw new IllegalArgumentException("Duplicate Molang variable: " + nonNullVariable.getName());
        }
    }

    /**
     * 更新变量。首次写入的名称会显式登记为变量，供渲染链提供预登记集合外的 query 值。
     *
     * @param name 变量名称
     * @param value 新数值
     */
    public void setValue(String name, double value) {
        String nonNullName = Objects.requireNonNull(name, "name");
        Variable variable = variables.get(nonNullName);
        if (variable == null) {
            Variable candidate = new Variable(nonNullName, value);
            Variable existing = variables.putIfAbsent(nonNullName, candidate);
            variable = existing == null ? candidate : existing;
        }
        variable.set(value);
    }

    /**
     * 从 JSON 字符串原语解析 Molang 表达式。
     *
     * @param element 包含表达式的 JSON 值
     * @return 可在变量更新后重新求值的表达式
     * @throws MolangException 输入不是字符串或表达式不合法
     */
    public IValue parseJson(JsonElement element) throws MolangException {
        if (element == null || element.isJsonNull() || !element.isJsonPrimitive()
                || !element.getAsJsonPrimitive().isString()) {
            throw new MolangException("Molang expression must be a JSON string");
        }
        return new ExpressionReader(element.getAsString()).parse();
    }

    /** 表示可反复求值的语法树节点。 */
    @FunctionalInterface
    private interface Expression {

        /**
         * 计算当前节点的数值。
         *
         * @return 当前节点值
         */
        double evaluate();
    }

    /**
     * 使用递归下降策略读取单个 Molang 表达式。
     */
    private final class ExpressionReader {

        /** 原始表达式文本，用于输出定位错误。 */
        private final String source;

        /** 当前读取位置。 */
        private int index;

        /**
         * 创建表达式读取器。
         *
         * @param source 原始表达式文本
         */
        private ExpressionReader(String source) {
            this.source = Objects.requireNonNull(source, "source");
        }

        /**
         * 解析完整表达式并拒绝尾随字符。
         *
         * @return 延迟求值的表达式
         * @throws MolangException 表达式语法或名称不合法
         */
        private IValue parse() throws MolangException {
            Expression expression = parseLogicalOr();
            skipWhitespace();
            if (!isAtEnd()) {
                throw error("Unexpected token");
            }
            return expression::evaluate;
        }

        /**
         * 解析逻辑或运算。
         *
         * @return 逻辑或节点
         * @throws MolangException 表达式不合法
         */
        private Expression parseLogicalOr() throws MolangException {
            Expression expression = parseLogicalAnd();
            while (match("||")) {
                Expression left = expression;
                Expression right = parseLogicalAnd();
                expression = () -> truthy(left.evaluate()) || truthy(right.evaluate()) ? 1.0D : 0.0D;
            }
            return expression;
        }

        /**
         * 解析逻辑与运算。
         *
         * @return 逻辑与节点
         * @throws MolangException 表达式不合法
         */
        private Expression parseLogicalAnd() throws MolangException {
            Expression expression = parseEquality();
            while (match("&&")) {
                Expression left = expression;
                Expression right = parseEquality();
                expression = () -> truthy(left.evaluate()) && truthy(right.evaluate()) ? 1.0D : 0.0D;
            }
            return expression;
        }

        /**
         * 解析相等性比较运算。
         *
         * @return 相等性节点
         * @throws MolangException 表达式不合法
         */
        private Expression parseEquality() throws MolangException {
            Expression expression = parseComparison();
            while (true) {
                if (match("==")) {
                    Expression left = expression;
                    Expression right = parseComparison();
                    expression = () -> left.evaluate() == right.evaluate() ? 1.0D : 0.0D;
                } else if (match("!=")) {
                    Expression left = expression;
                    Expression right = parseComparison();
                    expression = () -> left.evaluate() != right.evaluate() ? 1.0D : 0.0D;
                } else {
                    return expression;
                }
            }
        }

        /**
         * 解析大小比较运算。
         *
         * @return 比较节点
         * @throws MolangException 表达式不合法
         */
        private Expression parseComparison() throws MolangException {
            Expression expression = parseTerm();
            while (true) {
                if (match("<=")) {
                    Expression left = expression;
                    Expression right = parseTerm();
                    expression = () -> left.evaluate() <= right.evaluate() ? 1.0D : 0.0D;
                } else if (match(">=")) {
                    Expression left = expression;
                    Expression right = parseTerm();
                    expression = () -> left.evaluate() >= right.evaluate() ? 1.0D : 0.0D;
                } else if (match("<")) {
                    Expression left = expression;
                    Expression right = parseTerm();
                    expression = () -> left.evaluate() < right.evaluate() ? 1.0D : 0.0D;
                } else if (match(">")) {
                    Expression left = expression;
                    Expression right = parseTerm();
                    expression = () -> left.evaluate() > right.evaluate() ? 1.0D : 0.0D;
                } else {
                    return expression;
                }
            }
        }

        /**
         * 解析加减运算。
         *
         * @return 加减节点
         * @throws MolangException 表达式不合法
         */
        private Expression parseTerm() throws MolangException {
            Expression expression = parseFactor();
            while (true) {
                if (match("+")) {
                    expression = binary(expression, parseFactor(), (left, right) -> left + right);
                } else if (match("-")) {
                    expression = binary(expression, parseFactor(), (left, right) -> left - right);
                } else {
                    return expression;
                }
            }
        }

        /**
         * 解析乘除与取模运算。
         *
         * @return 乘除节点
         * @throws MolangException 表达式不合法
         */
        private Expression parseFactor() throws MolangException {
            Expression expression = parseUnary();
            while (true) {
                if (match("*")) {
                    expression = binary(expression, parseUnary(), (left, right) -> left * right);
                } else if (match("/")) {
                    expression = binary(expression, parseUnary(), (left, right) -> left / right);
                } else if (match("%")) {
                    expression = binary(expression, parseUnary(), (left, right) -> left % right);
                } else {
                    return expression;
                }
            }
        }

        /**
         * 解析一元符号。
         *
         * @return 一元节点
         * @throws MolangException 表达式不合法
         */
        private Expression parseUnary() throws MolangException {
            if (match("+")) {
                return parseUnary();
            }
            if (match("-")) {
                return unary(parseUnary(), value -> -value);
            }
            if (match("!")) {
                return unary(parseUnary(), value -> truthy(value) ? 0.0D : 1.0D);
            }
            return parsePrimary();
        }

        /**
         * 解析常量、变量、函数和括号表达式。
         *
         * @return 基础节点
         * @throws MolangException 表达式不合法
         */
        private Expression parsePrimary() throws MolangException {
            skipWhitespace();
            if (match("(")) {
                Expression expression = parseLogicalOr();
                require(")", "Expected ')' after expression");
                return expression;
            }
            if (isNumberStart()) {
                return parseNumber();
            }
            if (isIdentifierStart(peek())) {
                String identifier = parseIdentifier();
                if (match("(")) {
                    return parseFunction(identifier);
                }
                return parseIdentifierValue(identifier);
            }
            throw error("Expected number, variable, function, or '('");
        }

        /**
         * 解析十进制或科学计数法数字。
         *
         * @return 数值节点
         * @throws MolangException 数字格式不合法
         */
        private Expression parseNumber() throws MolangException {
            int start = index;
            consumeDigits();
            if (peek() == '.') {
                index++;
                consumeDigits();
            }
            if (peek() == 'e' || peek() == 'E') {
                index++;
                if (peek() == '+' || peek() == '-') {
                    index++;
                }
                int exponentStart = index;
                consumeDigits();
                if (exponentStart == index) {
                    throw error("Exponent requires digits");
                }
            }
            double value = Double.parseDouble(source.substring(start, index));
            return () -> value;
        }

        /**
         * 解析函数实参并建立函数节点。
         *
         * @param name 函数名称
         * @return 函数节点
         * @throws MolangException 参数或函数名称不合法
         */
        private Expression parseFunction(String name) throws MolangException {
            List<Expression> arguments = new ArrayList<>();
            if (!match(")")) {
                do {
                    arguments.add(parseLogicalOr());
                } while (match(","));
                require(")", "Expected ')' after function arguments");
            }
            return function(name, arguments);
        }

        /**
         * 将标识符绑定为已登记变量或数学常量。
         *
         * @param name 标识符名称
         * @return 已绑定的节点
         */
        private Expression parseIdentifierValue(String name) {
            if ("math.pi".equals(name)) {
                return () -> Math.PI;
            }
            if ("math.e".equals(name)) {
                return () -> Math.E;
            }
            return () -> {
                Variable variable = variables.get(name);
                if (variable == null) {
                    throw new IllegalStateException("Unknown Molang variable: " + name);
                }
                return variable.get();
            };
        }

        /**
         * 建立受支持数学函数的节点，同时严格校验参数数量。
         *
         * @param name 函数名称
         * @param arguments 已解析参数
         * @return 函数节点
         * @throws MolangException 函数未知或参数数量错误
         */
        private Expression function(String name, List<Expression> arguments) throws MolangException {
            switch (name) {
                case "math.abs":
                    return unaryFunction(name, arguments, Math::abs);
                case "math.acos":
                    return unaryFunction(name, arguments, value -> Math.toDegrees(Math.acos(value)));
                case "math.asin":
                    return unaryFunction(name, arguments, value -> Math.toDegrees(Math.asin(value)));
                case "math.atan":
                    return unaryFunction(name, arguments, value -> Math.toDegrees(Math.atan(value)));
                case "math.ceil":
                    return unaryFunction(name, arguments, Math::ceil);
                case "math.cos":
                    return unaryFunction(name, arguments, value -> Math.cos(Math.toRadians(value)));
                case "math.exp":
                    return unaryFunction(name, arguments, Math::exp);
                case "math.floor":
                    return unaryFunction(name, arguments, Math::floor);
                case "math.ln":
                    return unaryFunction(name, arguments, Math::log);
                case "math.round":
                    return unaryFunction(name, arguments, value -> Math.floor(value + 0.5D));
                case "math.sin":
                    return unaryFunction(name, arguments, value -> Math.sin(Math.toRadians(value)));
                case "math.sqrt":
                    return unaryFunction(name, arguments, Math::sqrt);
                case "math.trunc":
                    return unaryFunction(name, arguments, value -> value < 0.0D ? Math.ceil(value) : Math.floor(value));
                case "math.atan2":
                    return binaryFunction(name, arguments,
                            (y, x) -> Math.toDegrees(Math.atan2(y, x)));
                case "math.max":
                    return binaryFunction(name, arguments, Math::max);
                case "math.min":
                    return binaryFunction(name, arguments, Math::min);
                case "math.mod":
                    return binaryFunction(name, arguments, (left, right) -> left % right);
                case "math.pow":
                    return binaryFunction(name, arguments, Math::pow);
                case "math.clamp":
                    return ternaryFunction(name, arguments,
                            (value, minimum, maximum) -> Math.max(minimum, Math.min(maximum, value)));
                case "math.lerp":
                    return ternaryFunction(name, arguments,
                            (start, end, progress) -> start + (end - start) * progress);
                case "math.lerprotate":
                    return ternaryFunction(name, arguments, (start, end, progress) -> {
                        double difference = (end - start) % 360.0D;
                        if (difference >= 180.0D) {
                            difference -= 360.0D;
                        }
                        if (difference < -180.0D) {
                            difference += 360.0D;
                        }
                        return start + difference * progress;
                    });
                default:
                    throw error("Unknown Molang function '" + name + "'");
            }
        }

        /**
         * 创建一元运算节点。
         *
         * @param expression 输入节点
         * @param operation 运算函数
         * @return 一元运算节点
         */
        private Expression unary(Expression expression, DoubleUnaryOperator operation) {
            return () -> operation.applyAsDouble(expression.evaluate());
        }

        /**
         * 创建二元运算节点。
         *
         * @param left 左节点
         * @param right 右节点
         * @param operation 运算函数
         * @return 二元运算节点
         */
        private Expression binary(Expression left, Expression right, DoubleBinaryOperator operation) {
            return () -> operation.applyAsDouble(left.evaluate(), right.evaluate());
        }

        /**
         * 创建参数数量固定的一元函数节点。
         *
         * @param name 函数名称
         * @param arguments 函数参数
         * @param operation 运算函数
         * @return 函数节点
         * @throws MolangException 参数数量错误
         */
        private Expression unaryFunction(String name, List<Expression> arguments, DoubleUnaryOperator operation)
                throws MolangException {
            requireArgumentCount(name, arguments, 1);
            return unary(arguments.get(0), operation);
        }

        /**
         * 创建参数数量固定的二元函数节点。
         *
         * @param name 函数名称
         * @param arguments 函数参数
         * @param operation 运算函数
         * @return 函数节点
         * @throws MolangException 参数数量错误
         */
        private Expression binaryFunction(String name, List<Expression> arguments, DoubleBinaryOperator operation)
                throws MolangException {
            requireArgumentCount(name, arguments, 2);
            return binary(arguments.get(0), arguments.get(1), operation);
        }

        /**
         * 创建参数数量固定的三元函数节点。
         *
         * @param name 函数名称
         * @param arguments 函数参数
         * @param operation 运算函数
         * @return 函数节点
         * @throws MolangException 参数数量错误
         */
        private Expression ternaryFunction(String name, List<Expression> arguments, TernaryOperation operation)
                throws MolangException {
            requireArgumentCount(name, arguments, 3);
            Expression first = arguments.get(0);
            Expression second = arguments.get(1);
            Expression third = arguments.get(2);
            return () -> operation.apply(first.evaluate(), second.evaluate(), third.evaluate());
        }

        /**
         * 严格校验函数参数数量。
         *
         * @param name 函数名称
         * @param arguments 实际参数
         * @param expected 期望数量
         * @throws MolangException 参数数量错误
         */
        private void requireArgumentCount(String name, List<Expression> arguments, int expected) throws MolangException {
            if (arguments.size() != expected) {
                throw error("Function '" + name + "' requires " + expected + " arguments");
            }
        }

        /**
         * 读取标识符。
         *
         * @return 标识符文本
         */
        private String parseIdentifier() {
            int start = index;
            index++;
            while (isIdentifierPart(peek())) {
                index++;
            }
            return source.substring(start, index);
        }

        /**
         * 消费数字字符。
         */
        private void consumeDigits() {
            while (Character.isDigit(peek())) {
                index++;
            }
        }

        /**
         * 匹配指定文本，并跳过其前导空白。
         *
         * @param token 要匹配的文本
         * @return 是否已匹配
         */
        private boolean match(String token) {
            skipWhitespace();
            if (!source.startsWith(token, index)) {
                return false;
            }
            index += token.length();
            return true;
        }

        /**
         * 要求出现指定文本。
         *
         * @param token 期望文本
         * @param message 失败消息
         * @throws MolangException 指定文本不存在
         */
        private void require(String token, String message) throws MolangException {
            if (!match(token)) {
                throw error(message);
            }
        }

        /**
         * 跳过表达式内允许的空白字符。
         */
        private void skipWhitespace() {
            while (!isAtEnd() && Character.isWhitespace(source.charAt(index))) {
                index++;
            }
        }

        /**
         * 读取当前位置字符，结束时返回零字符。
         *
         * @return 当前字符或零字符
         */
        private char peek() {
            return isAtEnd() ? '\0' : source.charAt(index);
        }

        /**
         * 判断当前是否已读取到结尾。
         *
         * @return 是否到达结尾
         */
        private boolean isAtEnd() {
            return index >= source.length();
        }

        /**
         * 判断当前位置是否可能开始数字。
         *
         * @return 是否为数字起点
         */
        private boolean isNumberStart() {
            return Character.isDigit(peek()) || peek() == '.' && Character.isDigit(peekNext());
        }

        /**
         * 读取下一个字符，结束时返回零字符。
         *
         * @return 下一个字符或零字符
         */
        private char peekNext() {
            return index + 1 >= source.length() ? '\0' : source.charAt(index + 1);
        }

        /**
         * 判断字符是否可以开始标识符。
         *
         * @param character 待判断字符
         * @return 是否为标识符起始字符
         */
        private boolean isIdentifierStart(char character) {
            return character == '_' || Character.isLetter(character);
        }

        /**
         * 判断字符是否可以构成标识符后续部分。
         *
         * @param character 待判断字符
         * @return 是否为标识符后续字符
         */
        private boolean isIdentifierPart(char character) {
            return isIdentifierStart(character) || Character.isDigit(character) || character == '.';
        }

        /**
         * 把当前位置转换为可诊断的受检解析错误。
         *
         * @param message 错误原因
         * @return 包含位置的异常
         */
        private MolangException error(String message) {
            return new MolangException(message + " at character " + index + " in '" + source + "'");
        }
    }

    /**
     * 表示三元数学函数的纯数值运算。
     */
    @FunctionalInterface
    private interface TernaryOperation {

        /**
         * 计算三个输入值的结果。
         *
         * @param first 第一个输入值
         * @param second 第二个输入值
         * @param third 第三个输入值
         * @return 运算结果
         */
        double apply(double first, double second, double third);
    }

    /**
     * 判断数值在 Molang 布尔上下文中是否为真。
     *
     * @param value 待判断值
     * @return 非零值为真
     */
    private static boolean truthy(double value) {
        return value != 0.0D;
    }
}
