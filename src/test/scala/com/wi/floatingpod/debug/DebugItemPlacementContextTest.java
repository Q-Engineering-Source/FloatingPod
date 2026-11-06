package com.wi.floatingpod.debug;

import java.util.UUID;
import org.junit.jupiter.api.Test;
import static com.wi.floatingpod.debug.DebugItemEndpoint.*;
import static org.junit.jupiter.api.Assertions.*;

/** 直接调用production Request/matching，不包含Minecraft、IO或假Endpoint。 */
class DebugItemPlacementContextTest {
    private Identity identity(boolean placement) {
        return new Identity(UUID.randomUUID(),UUID.randomUUID(),UUID.randomUUID(),UUID.randomUUID(),"test-world",0,
                placement?-1:7,placement?null:UUID.randomUUID(),placement?null:UUID.randomUUID());
    }
    private Request place(PlacementContext context,Position position) {
        return place(context, position, DebugItemDispatch.Hand.MAIN_HAND);
    }
    private Request place(PlacementContext context,Position position,DebugItemDispatch.Hand hand) {
        return new Request(identity(true),1,UUID.randomUUID(),Action.PLACE,position,context,hand);
    }
    @Test void sixActualFacesMatchOnlyTheirExactAdjacentBlockCenters() {
        PlacementFace[] faces={PlacementFace.DOWN,PlacementFace.UP,PlacementFace.NORTH,PlacementFace.SOUTH,PlacementFace.WEST,PlacementFace.EAST};
        Position[] expected={new Position(10.5,19,30.5,0),new Position(10.5,21,30.5,0),new Position(10.5,20,29.5,0),
                new Position(10.5,20,31.5,0),new Position(9.5,20,30.5,0),new Position(11.5,20,30.5,0)};
        for(int i=0;i<faces.length;i++) {
            var context=new PlacementContext(10,20,30,faces[i]);
            assertTrue(DebugItemEndpoint.placementMatches(context,expected[i]));
            assertEquals(context,place(context,expected[i]).placementContext());
        }
    }
    @Test void actualRequestRetainsHandAndLegacyConstructorsRemainMainHand() {
        Identity original=identity(true);UUID operation=UUID.randomUUID();
        Position position=new Position(1.5,65,1.5,0);
        PlacementContext context=new PlacementContext(1,64,1,PlacementFace.UP);
        Request legacy=new Request(original,1,operation,Action.PLACE,position,context);
        Request offhand=new Request(original,1,UUID.randomUUID(),Action.PLACE,position,context,
                DebugItemDispatch.Hand.OFF_HAND);
        assertEquals(DebugItemDispatch.Hand.MAIN_HAND,legacy.hand());
        assertEquals(DebugItemDispatch.Hand.OFF_HAND,offhand.hand());
        assertEquals(DebugItemDispatch.Hand.OFF_HAND,
                place(context,position,DebugItemDispatch.Hand.OFF_HAND).hand());
    }

    @Test void placeCannotUseMissingContextOrOldFiveArgumentConstructor() {
        Position position=new Position(1.5,65,1.5,0);
        assertThrows(IllegalArgumentException.class,()->place(null,position));
        assertThrows(IllegalArgumentException.class,()->new Request(identity(true),1,UUID.randomUUID(),Action.PLACE,position));
        assertThrows(IllegalArgumentException.class,()->place(new PlacementContext(1,64,1,PlacementFace.UP),null));
    }
    @Test void nonPlaceCannotCarryClickedFaceContext() {
        for(Action action:new Action[]{Action.INSPECT,Action.TOGGLE})
            assertThrows(IllegalArgumentException.class,()->new Request(identity(false),1,UUID.randomUUID(),action,null,
                new PlacementContext(1,64,1,PlacementFace.UP)));
    }
    @Test void nullFaceIsRejectedBeforeTargetDerivation() {
        assertThrows(IllegalArgumentException.class,()->new PlacementContext(1,64,1,null));
    }
    @Test void arbitraryPoseAndAnyCoordinateMismatchAreRejected() {
        var context=new PlacementContext(1,64,1,PlacementFace.UP);
        for(Position bad:new Position[]{new Position(1,65,1,0),new Position(2.5,65,1.5,0),
                new Position(1.5,64,1.5,0),new Position(1.5,65,2.5,0),new Position(1.5,65.01,1.5,0)}) {
            assertFalse(DebugItemEndpoint.placementMatches(context,bad));
            assertThrows(IllegalArgumentException.class,()->place(context,bad));
        }
        assertEquals(90F,place(context,new Position(1.5,65,1.5,90)).placement().yaw());
    }
    @Test void sixOverflowDirectionsFailWithoutIntegerWraparound() {
        PlacementContext[] inputs={new PlacementContext(Integer.MAX_VALUE,1,1,PlacementFace.EAST),new PlacementContext(Integer.MIN_VALUE,1,1,PlacementFace.WEST),
            new PlacementContext(1,Integer.MAX_VALUE,1,PlacementFace.UP),new PlacementContext(1,Integer.MIN_VALUE,1,PlacementFace.DOWN),
            new PlacementContext(1,1,Integer.MAX_VALUE,PlacementFace.SOUTH),new PlacementContext(1,1,Integer.MIN_VALUE,PlacementFace.NORTH)};
        for(var context:inputs) {
            Position position=new Position(context.clickedX()+0.5,context.clickedY(),context.clickedZ()+0.5,0);
            assertThrows(ArithmeticException.class,()->place(context,position));
        }
        assertTrue(DebugItemEndpoint.placementMatches(new PlacementContext(Integer.MAX_VALUE,0,Integer.MIN_VALUE,PlacementFace.UP),
            new Position(Integer.MAX_VALUE+0.5,1,Integer.MIN_VALUE+0.5,0)));
    }
    @Test void existingInspectAndToggleRequestsKeepNullPlacementAndContext() {
        for(Action action:new Action[]{Action.INSPECT,Action.TOGGLE}) {
            Identity original=identity(false);UUID operation=UUID.randomUUID();var request=new Request(original,17,operation,action,null);
            assertSame(original,request.identity());assertEquals(operation,request.operation());assertEquals(17,request.tick());
            assertEquals(action,request.action());assertNull(request.placement());assertNull(request.placementContext());
        }
    }
}
