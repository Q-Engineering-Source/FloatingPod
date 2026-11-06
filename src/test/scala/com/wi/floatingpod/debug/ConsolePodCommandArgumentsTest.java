package com.wi.floatingpod.debug;

import org.junit.jupiter.api.Test;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

class ConsolePodCommandArgumentsTest {
    @Test void acceptsExplicitPlacementAndActualWorldSpawnSelection() {
        var spawn=(ConsolePodCommandArguments.Create)ConsolePodCommandArguments.parse(new String[]{"create","-1"});
        assertEquals(-1,spawn.dimension()); assertNull(spawn.position());
        var pose=(ConsolePodCommandArguments.Create)ConsolePodCommandArguments.parse(new String[]{"create","7","1.5","64","-2.5","90"});
        assertEquals(new DebugItemEndpoint.Position(1.5,64,-2.5,90),pose.position());
        UUID pod=UUID.randomUUID();
        assertEquals(pod,((ConsolePodCommandArguments.Status)ConsolePodCommandArguments.parse(new String[]{"status",pod.toString()})).pod());
        assertEquals(pod,((ConsolePodCommandArguments.Resume)ConsolePodCommandArguments.parse(new String[]{"resume",pod.toString()})).originalCreate());
    }
    @Test void refusesIncompleteNonFiniteAndMalformedInput() {
        for (String[] args:new String[][]{{},{"create"},{"create","0","1"},{"create","0","NaN","64","2"},
                {"create","0","1","64","2","Infinity"},{"create","2147483648"},{"status","1-1-1-1-1"},
                {"resume"},{"resume","1-1-1-1-1"},{"resume",UUID.randomUUID().toString(),"extra"},{"unknown"}})
            assertThrows(IllegalArgumentException.class,()->ConsolePodCommandArguments.parse(args));
    }
    @Test void consoleOriginCannotMasqueradeAsPlayerSession() {
        UUID boot=UUID.randomUUID();
        var request=new MainPodOperationRequest.ConsoleCreate(boot,UUID.randomUUID(),"actual-world",0,new DebugItemEndpoint.Position(1,64,2,0));
        assertEquals(MainPodOperationRequest.OriginKind.LOCAL_CONSOLE_BOOT,request.origin().kind());
        assertEquals(boot,request.origin().identity());
        assertEquals(DebugItemEndpoint.Action.PLACE,request.action());
        assertThrows(IllegalStateException.class,request::playerRequest);
        assertNotEquals(request.origin(),new MainPodOperationRequest.Origin(MainPodOperationRequest.OriginKind.PLAYER_SESSION,boot));
    }
}
