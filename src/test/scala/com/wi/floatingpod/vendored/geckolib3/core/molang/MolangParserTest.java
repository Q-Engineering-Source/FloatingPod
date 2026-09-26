/*
 * FloatingPod 对 Molang 求值表面的独立实现测试。
 * 因上游 mclib 的许可证无法取得，本文件未复制任何第三方代码。
 */
package com.wi.floatingpod.vendored.geckolib3.core.molang;

import com.google.gson.JsonPrimitive;
import com.wi.floatingpod.vendored.geckolib3.core.ConstantValue;
import com.wi.floatingpod.vendored.mclib.math.IValue;
import com.wi.floatingpod.vendored.mclib.math.Variable;
import com.wi.floatingpod.vendored.mclib.utils.Interpolations;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 直接验证 FloatingPod 独立 Molang 求值器的公开行为。
 */
class MolangParserTest {

    /**
     * 验证 GeckoLib 消费端的 ConstantValue factory 与减法结果均通过 IValue 暴露。
     */
    @Test
    void createsAndSubtractsConstantValues() {
        ConstantValue fromDouble = ConstantValue.fromDouble(4.5D);
        ConstantValue fromFloat = ConstantValue.fromFloat(2.25F);
        ConstantValue parsedDouble = ConstantValue.parseDouble("3.75");
        ConstantValue parsedFloat = ConstantValue.parseFloat("1.5");

        assertEquals(4.5D, fromDouble.get());
        assertEquals(2.25D, fromFloat.get());
        assertEquals(3.75D, parsedDouble.get());
        assertEquals(1.5D, parsedFloat.get());
        assertEquals(2.25D, ConstantValue.subtract(fromDouble, fromFloat).get());
    }

    /**
     * 验证 IValue 和 Variable 始终返回当前值。
     */
    @Test
    void exposesCurrentValuesThroughIValue() {
        IValue fixedValue = () -> 2.5D;
        Variable variable = new Variable("query.anim_time", 1.0D);

        assertEquals(2.5D, fixedValue.get());
        assertEquals(1.0D, variable.get());

        variable.set(4.0D);

        assertEquals(4.0D, variable.get());
    }

    /**
     * 验证登记变量后更新数值会被已解析表达式读取。
     *
     * @throws MolangException 表达式不应解析失败
     */
    @Test
    void resolvesRegisteredVariablesAfterSetValue() throws MolangException {
        MolangParser parser = new MolangParser();
        parser.register(new Variable("query.anim_time", 0.0D));
        IValue expression = parser.parseJson(new JsonPrimitive("query.anim_time"));

        parser.setValue("query.anim_time", 7.25D);

        assertEquals(7.25D, expression.get());
    }

    /**
     * 验证 GeckoLib 登记的全部 query 变量无冲突，并可读取动画时间和生命周期。
     *
     * @throws MolangException 表达式不应解析失败
     */
    @Test
    void registersAllGeckoLibQueryVariables() throws MolangException {
        MolangParser parser = new MolangParser();
        MolangRegistrar.registerVars(parser);
        String[] names = {
                "query.anim_time",
                "query.actor_count",
                "query.health",
                "query.max_health",
                "query.distance_from_camera",
                "query.yaw_speed",
                "query.is_in_water_or_rain",
                "query.is_in_water",
                "query.is_on_ground",
                "query.time_of_day",
                "query.is_on_fire",
                "query.ground_speed"
        };

        for (int index = 0; index < names.length; index++) {
            parser.setValue(names[index], index + 0.5D);
            assertEquals(index + 0.5D, parser.parseJson(new JsonPrimitive(names[index])).get());
        }

        IValue lifeTime = parser.parseJson(new JsonPrimitive("query.life_time"));
        parser.setValue("query.anim_time", 2.0D);
        parser.setValue("query.life_time", 3.0D);

        assertEquals(2.0D, parser.parseJson(new JsonPrimitive("query.anim_time")).get());
        assertEquals(3.0D, lifeTime.get());
    }

    /**
     * 验证算术、变量和函数调用会组成可延迟计算的表达式。
     *
     * @throws MolangException 表达式不应解析失败
     */
    @Test
    void evaluatesArithmeticVariableAndFunctionExpression() throws MolangException {
        MolangParser parser = new MolangParser();
        parser.register(new Variable("query.anim_time", 0.0D));
        parser.setValue("query.anim_time", 4.0D);

        IValue expression = parser.parseJson(new JsonPrimitive("math.max(query.anim_time * 2, 3) + 1"));

        assertEquals(9.0D, expression.get());
    }

    /**
     * 验证 Molang 三角函数以角度作为输入和输出，而不是直接暴露 Java 的弧度 API。
     *
     * @throws MolangException 表达式不应解析失败
     */
    @Test
    void evaluatesTrigonometricFunctionsInDegrees() throws MolangException {
        MolangParser parser = new MolangParser();

        assertEquals(0.5D, parser.parseJson(new JsonPrimitive("math.cos(60)")).get(), 1.0E-9D);
        assertEquals(0.5D, parser.parseJson(new JsonPrimitive("math.sin(30)")).get(), 1.0E-9D);
        assertEquals(60.0D, parser.parseJson(new JsonPrimitive("math.acos(0.5)")).get(), 1.0E-9D);
        assertEquals(30.0D, parser.parseJson(new JsonPrimitive("math.asin(0.5)")).get(), 1.0E-9D);
        assertEquals(45.0D, parser.parseJson(new JsonPrimitive("math.atan(1)")).get(), 1.0E-9D);
        assertEquals(45.0D, parser.parseJson(new JsonPrimitive("math.atan2(1, 1)")).get(), 1.0E-9D);
    }

    /**
     * 验证语法错误以受检 MolangException 失败，而非产生静默默认值。
     */
    @Test
    void rejectsMalformedExpressions() {
        MolangParser parser = new MolangParser();

        MolangException exception = assertThrows(
                MolangException.class,
                () -> parser.parseJson(new JsonPrimitive("1 + )"))
        );

        assertTrue(exception.getMessage().contains("Expected"));
    }

    /**
     * 验证普通插值保留起点、终点和中点。
     */
    @Test
    void linearlyInterpolatesValues() {
        assertEquals(10.0F, Interpolations.lerp(10.0F, 30.0F, 0.0F));
        assertEquals(20.0F, Interpolations.lerp(10.0F, 30.0F, 0.5F));
        assertEquals(30.0F, Interpolations.lerp(10.0F, 30.0F, 1.0F));
    }

    /**
     * 验证偏航插值在跨越零度时走最短路径。
     */
    @Test
    void interpolatesYawAcrossZeroByShortestPath() {
        float midpoint = Interpolations.lerpYaw(350.0F, 10.0F, 0.5F);

        assertEquals(0.0F, Math.floorMod(Math.round(midpoint), 360));
    }
}
