package com.wi.floatingpod.debug;

import org.junit.jupiter.api.Test;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

class DebugItemFeedbackTest {
    @Test void realThreeLanguageResourcesRenderAllSixPublicStateParameters() throws Exception {
        UUID pod = UUID.fromString("00000000-0000-0000-0000-000000000007");
        UUID owner = UUID.fromString("00000000-0000-0000-0000-000000000008");
        String[] languages = {"en_us", "zh_cn", "zh_tw"};
        String[] prefixes = {"Pod: ", "Pod：", "Pod："};
        String[] owners = {" | Owner: ", " | 所有者：", " | 擁有者："};
        String[] tails = {" | Mode: WorldIdle | Active: true | Initialization/recovery: PendingCleanup | Reason: CLEANUP_PENDING",
                " | 模式：WorldIdle | 活动：true | 初始化/恢复：PendingCleanup | 原因码：CLEANUP_PENDING",
                " | 模式：WorldIdle | 活動：true | 初始化/恢復：PendingCleanup | 原因碼：CLEANUP_PENDING"};
        String[] unclaimed = {"Unclaimed", "待认领", "待認領"};
        for (int i = 0; i < languages.length; i++) {
            Properties translations = new Properties();
            try (InputStream input = getClass().getClassLoader().getResourceAsStream(
                    "assets/floatingpod/lang/" + languages[i] + ".lang")) {
                assertNotNull(input);
                translations.load(new InputStreamReader(input, StandardCharsets.UTF_8));
            }
            DebugItemFeedback.Message owned = DebugItemFeedback.state(new DebugItemEndpoint.View(pod, owner,
                    "WorldIdle", true, "PendingCleanup", DebugItemEndpoint.Reason.CLEANUP_PENDING));
            assertEquals(List.of(pod.toString(), owner.toString(), "WorldIdle", true, "PendingCleanup", "CLEANUP_PENDING"), owned.parameters());
            assertEquals(prefixes[i] + pod + owners[i] + owner + tails[i], owned.render(translations::getProperty));
            DebugItemFeedback.Message vacant = DebugItemFeedback.state(new DebugItemEndpoint.View(pod, null,
                    "WorldIdle", true, "PendingCleanup", DebugItemEndpoint.Reason.CLEANUP_PENDING));
            assertEquals(prefixes[i] + pod + owners[i] + unclaimed[i] + tails[i], vacant.render(translations::getProperty));
        }
    }
}
