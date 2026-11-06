package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.config.SourceLimitProfile
import com.wi.floatingpod.domain.FloatingPodPose
import com.wi.floatingpod.storage.{PhysicalRecoveryBinding, PhysicalRecoveryCutoverState}
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import java.util.UUID

/** 真实request encoder DATA断言；不持有plan或session资格。 */
class MySqlControlledDebugRequestIdentityTest:
  private def id(n: Long)=new UUID(0L,n)
  private val binding=PhysicalRecoveryBinding(id(1),PhysicalRecoveryCutoverState.Certified,2)
  private val pose=FloatingPodPose("world",1.25,2.5,3.75,1.0f,0.0f,0)
  private val profile=SourceLimitProfile.of(1_000_000L,1_000_000L,2_000_000L,64L,256L,16_000_000L,4_000_000L,2_000_000L)
  private def request(footprint: UUID)=MySqlControlledDebugPrepareRequest(binding,id(2),id(3),footprint,pose,id(5))
  private def identity(operation: MySqlRepositoryOperation)=MySqlRepositoryRequestIdentity.single(operation,profile)
  @Test def changingOnlyFootprintChangesOriginalRequestIdentity(): Unit =
    assertNotEquals(identity(request(id(4)).repositoryOperation),identity(request(id(6)).repositoryOperation))
    assertEquals(identity(request(id(4)).repositoryOperation),identity(request(id(4)).repositoryOperation))
  @Test def oldMissingFootprintEncodingCannotEqualF3Request(): Unit =
    val old=MySqlRepositoryOperation.PrepareUnboundCreate(binding,id(2),id(3),pose,id(5))
    assertNotEquals(identity(old),identity(request(id(4)).repositoryOperation))
  @Test def activateIdentityContainsOriginalFootprintAndVersion(): Unit =
    val operation=MySqlRepositoryOperation.CommitControlledDebugCreate(binding,id(2),id(3),id(4),0L,pose,id(5),256)
    assertNotEquals(identity(operation),identity(MySqlRepositoryOperation.CommitControlledDebugCreate(binding,id(2),id(3),id(6),0L,pose,id(5),256)))
    assertNotEquals(identity(operation),identity(MySqlRepositoryOperation.CommitControlledDebugCreate(binding,id(2),id(3),id(4),1L,pose,id(5),256)))
  @Test def zeroOrMissingProposalIsRejectedBeforePlanConstruction(): Unit =
    assertThrows(classOf[IllegalArgumentException],()=>request(new UUID(0L,0L)))
    assertThrows(classOf[IllegalArgumentException],()=>request(null))

  @Test def deploymentOriginalIdentitySeparatesDomainActorAndOriginalOperation(): Unit =
    def find(actor: UUID, original: UUID) = MySqlRepositoryOperation.FindOriginalDeploymentOperation(actor, original)
    assertEquals(identity(find(id(10),id(11))),identity(find(id(10),id(11))))
    assertNotEquals(identity(find(id(10),id(11))),identity(find(id(12),id(11))))
    assertNotEquals(identity(find(id(10),id(11))),identity(find(id(10),id(12))))
    assertNotEquals(identity(find(id(10),id(11))),identity(MySqlRepositoryOperation.FindByUuid(id(11))))
    assertThrows(classOf[java.sql.SQLDataException],()=>identity(find(null,id(11))))
    assertThrows(classOf[java.sql.SQLDataException],()=>identity(find(id(10),null)))
