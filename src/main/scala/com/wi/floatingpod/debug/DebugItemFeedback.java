package com.wi.floatingpod.debug;

import java.util.List;
import java.util.function.Function;

/** 与 vanilla TextComponentTranslation 共用的真实参数顺序，可直接验证三语言资源。 */
public final class DebugItemFeedback {
    public record Translation(String key) { }
    public record Message(String key, List<Object> parameters) {
        public Message { parameters = List.copyOf(parameters); }
        public String render(Function<String, String> translations) {
            Object[] localized = parameters.stream().map(value -> value instanceof Translation translation
                    ? translations.apply(translation.key()) : value).toArray();
            return String.format(java.util.Locale.ROOT, translations.apply(key), localized);
        }
    }
    private DebugItemFeedback() { }
    public static Message state(DebugItemEndpoint.View view) {
        Object owner = view.owner() == null ? new Translation("floatingpod.debug_tool.unclaimed")
                : view.owner().toString();
        return new Message("floatingpod.debug_tool.state", List.of(view.pod().toString(), owner,
                view.mode(), view.active(), view.phase(), view.reason().name()));
    }
}
