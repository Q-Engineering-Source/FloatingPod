package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.config.SourceLimitProfile
import java.sql.SQLDataException
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class MySqlManagedPrivilegePreflightTest:
  private val Installer = "installer@localhost"
  private val Schema = "fp_managed_test"

  private def managedProfile = MySqlManagedReleasePolicy.profile("floatingpodrbssv1",
    java.util.UUID.fromString("982937bc-bc46-11f1-b957-04421a0aa74d"), "127.0.0.1")

  @Test def managedPolicyExplicitlyAllowsOnlyTheStandardLocalRootAdministrativeProxy(): Unit =
    import MySqlDeploymentReceiptSchemaProfile.TrustedProxyRow
    val profile = managedProfile
    assertEquals(Vector(TrustedProxyRow("root", "localhost", "", "", true)), profile.trustedAdministrativeProxyRows)
    MySqlManagedPrivilegePreflight.requireApprovedAdministrativeProxy(profile, "root", "localhost", "", "", 1)

  @Test def administrativeProxyApprovalRequiresAllFiveExactFieldsAndNeverApprovesGeneratedAccounts(): Unit =
    val profile = managedProfile
    Vector(
      ("other", "localhost", "", "", Integer.valueOf(1)),
      ("root", "%", "", "", Integer.valueOf(1)),
      ("root", "localhost", "other", "", Integer.valueOf(1)),
      ("root", "localhost", "", "%", Integer.valueOf(1)),
      ("root", "localhost", "", "", Integer.valueOf(0)),
      ("root", "localhost", "", "", Integer.valueOf(2)),
      (null, "localhost", "", "", Integer.valueOf(1)),
      ("root", "localhost", "", "", null),
      (profile.accounts.runtime.user, "127.0.0.1", "", "", Integer.valueOf(1)),
      ("external", "%", profile.accounts.runtime.user, "127.0.0.1", Integer.valueOf(1))
    ).foreach { (user, host, proxiedUser, proxiedHost, grant) =>
      assertThrows(classOf[SQLDataException], () => MySqlManagedPrivilegePreflight.requireApprovedAdministrativeProxy(
        profile, user, host, proxiedUser, proxiedHost, grant))
    }
    assertThrows(classOf[SQLDataException], () => MySqlManagedPrivilegePreflight.requireApprovedAdministrativeProxy(
      rendered.inputs, "root", "localhost", "", "", 1))

  private def rendered: MySqlDbFirstSchemaProfile.Rendered =
    import MySqlDeploymentReceiptSchemaProfile.*
    def account(name: String) = Account(name, "127.0.0.1")
    val inputs = MySqlDeploymentReceiptSchemaProfile.inputs(Schema,
      Accounts(account("fp_s5_guard"), account("fp_s5_audit"), account("fp_s5_runtime"),
        account("fp_s5_provisioner"), account("fp_s5_importer"), account("fp_s5_quota")),
      SourceLimitProfile.of(1000000, 65536, 2000000, 64, 100000, 134217728, 1000000, 2000000),
      MySqlScriptSchemaProfile.Budget(16384, 16384, 16384, 4096, 65536, 142, 20000, 2000000, 1000000),
      MySqlDeploymentReceiptSchemaDefinition.Budget(8192, 8192), 4096, Vector.empty)
    MySqlDbFirstSchemaProfile.renderExistingDatabase(inputs)

  private def allowed(schema: String = Schema, grantOption: Boolean = true): Vector[String] =
    val grant = if grantOption then " WITH GRANT OPTION" else ""
    Vector(
      s"GRANT CREATE USER ON *.* TO 'installer'@'localhost'",
      s"GRANT SET_ANY_DEFINER ON *.* TO 'installer'@'localhost'",
      s"GRANT ALL PRIVILEGES ON `$schema`.* TO 'installer'@'localhost'$grant",
      "GRANT SELECT ON `mysql`.* TO 'installer'@'localhost' WITH GRANT OPTION",
      "GRANT SHOW_ROUTINE ON *.* TO 'installer'@'localhost' WITH GRANT OPTION"
    )

  @Test def acceptsOnlyTheClosedExistingDatabasePlanWithAllRequiredAndGrantablePrivileges(): Unit =
    val result = MySqlManagedPrivilegePreflight.verifyObserved(rendered, Installer, "NONE", allowed())
    assertEquals(Installer, result.effectivePrincipal)
    assertEquals(5, result.observedGrantRows)
    assertTrue(result.requiredGrantChecks > 10)
    assertFalse(result.toString.contains(Installer))
    assertFalse(result.toString.contains("GRANT"))

  @Test def requiresCreateUserForRuntimeAccountLifecycleAndSetAnyDefinerExplicitly(): Unit =
    val noCreateUser = allowed().filterNot(_.contains("CREATE USER"))
    assertRejected("required", noCreateUser)
    val noDefiner = allowed().filterNot(_.contains("SET_ANY_DEFINER"))
    assertRejected("required", noDefiner)
    assertRejected("required", noDefiner :+ "GRANT FUTURE_ADMIN_PRIV ON *.* TO 'installer'@'localhost'")
    val withUnrelatedDynamic = allowed() :+ "GRANT FUTURE_ADMIN_PRIV ON *.* TO 'installer'@'localhost'"
    assertEquals(6, MySqlManagedPrivilegePreflight.verifyObserved(rendered, Installer, "NONE", withUnrelatedDynamic).observedGrantRows)

  @Test def schemaAndRendererGrantsMustBeGrantableAtAnApplicableScope(): Unit =
    assertRejected("required", allowed(grantOption = false))
    assertRejected("required", allowed().filterNot(_.contains("`mysql`.*")))
    assertRejected("required", allowed().filterNot(_.contains("SHOW_ROUTINE")))
    val globalSelect = allowed().updated(3, "GRANT SELECT ON *.* TO 'installer'@'localhost' WITH GRANT OPTION")
    assertEquals(5, MySqlManagedPrivilegePreflight.verifyObserved(rendered, Installer, "NONE", globalSelect).observedGrantRows)

  @Test def foreignKeyCreationRequiresReferencesEvenWhenAllOtherRecipePrivilegesAreGrantable(): Unit =
    val withoutReferences = allowed().updated(2,
      s"GRANT CREATE, ALTER, INDEX, TRIGGER, CREATE ROUTINE, ALTER ROUTINE, EXECUTE, SELECT, INSERT, UPDATE, DELETE, LOCK TABLES ON `$Schema`.* TO 'installer'@'localhost' WITH GRANT OPTION")
    assertRejected("required", withoutReferences)
    assertEquals(6, MySqlManagedPrivilegePreflight.verifyObserved(rendered, Installer, "NONE",
      withoutReferences :+ s"GRANT REFERENCES ON `$Schema`.* TO 'installer'@'localhost' WITH GRANT OPTION").observedGrantRows)

  @Test def activeRolesRoleGrantsAndWrongEffectiveAccountFailClosed(): Unit =
    assertThrows(classOf[SQLDataException], () =>
      MySqlManagedPrivilegePreflight.verifyObserved(rendered, Installer, "fp_admin_role@%", allowed()))
    val roleGrant = allowed() :+ "GRANT 'fp_installer_role'@'%' TO 'installer'@'localhost'"
    assertRejected("role", roleGrant)
    assertRejected("different effective account", allowed().updated(0, "GRANT CREATE USER ON *.* TO 'other'@'localhost'"))

  @Test def unsupportedOrOverlongGrantSyntaxIsRejectedWithoutEchoingGrantText(): Unit =
    val hostile = "GRANT CREATE SOMETHING_NEW ON *.* TO 'installer'@'localhost' SECRET_SENTINEL"
    val failure = assertThrows(classOf[SQLDataException], () =>
      MySqlManagedPrivilegePreflight.verifyObserved(rendered, Installer, "NONE", allowed() :+ hostile))
    assertFalse(failure.getMessage.contains("SECRET_SENTINEL"))
    assertFalse(failure.getMessage.contains(hostile))
    assertThrows(classOf[SQLDataException], () =>
      MySqlManagedPrivilegePreflight.verifyObserved(rendered, Installer, "NONE", allowed() :+ ("x" * 8193)))
    assertThrows(classOf[SQLDataException], () =>
      MySqlManagedPrivilegePreflight.verifyObserved(rendered, Installer, "NONE", Vector.fill(257)(allowed().head)))
    assertThrows(classOf[SQLDataException], () =>
      MySqlManagedPrivilegePreflight.verifyObserved(rendered, Installer, "NONE", Vector.fill(33)("x" * 8192)))

  @Test def parsesGlobalSchemaTableProcedureAndColumnGrantFormsByTopLevelComma(): Unit =
    val global = MySqlManagedPrivilegePreflight.parseGrantForTest(
      "GRANT CREATE USER ON *.* TO 'installer'@'localhost';")
    val schema = MySqlManagedPrivilegePreflight.parseGrantForTest(
      "GRANT SELECT ON `db`.* TO 'installer'@'localhost' WITH GRANT OPTION")
    val table = MySqlManagedPrivilegePreflight.parseGrantForTest(
      "GRANT UPDATE (`state_version`, owner_uuid), SELECT ON `db`.`floating_pod_state` TO 'installer'@'localhost'")
    val procedure = MySqlManagedPrivilegePreflight.parseGrantForTest(
      "GRANT EXECUTE ON PROCEDURE `db`.`audit_routine` TO `installer`@`localhost` WITH GRANT OPTION")
    assertEquals("GLOBAL", global._1)
    assertFalse(global._3)
    assertEquals("SCHEMA:db", schema._1)
    assertTrue(schema._3)
    assertEquals(Vector("UPDATE", "SELECT"), table._2.map(_._1))
    assertEquals(Vector("state_version", "owner_uuid"), table._2.head._2)
    assertEquals("PROCEDURE:db.audit_routine", procedure._1)
    assertTrue(procedure._3)
  
  private def assertRejected(fragment: String, grants: Vector[String]): Unit =
    val failure = assertThrows(classOf[SQLDataException], () =>
      MySqlManagedPrivilegePreflight.verifyObserved(rendered, Installer, "NONE", grants))
    assertTrue(failure.getMessage.contains(fragment), failure.getMessage)

  @Test def exactSchemaWildcardEscapesInRealShowGrantsRemainScoped(): Unit =
    val escaped = Schema.replace("_", "\\_")
    val observed = allowed().updated(2,
      s"GRANT ALL PRIVILEGES ON `$escaped`.* TO `installer`@`localhost` WITH GRANT OPTION")
    assertEquals(5, MySqlManagedPrivilegePreflight.verifyObserved(rendered, Installer, "NONE", observed).observedGrantRows)
    assertRejected("required", observed.updated(2,
      s"GRANT ALL PRIVILEGES ON `${escaped}other`.* TO `installer`@`localhost` WITH GRANT OPTION"))
    val literalPercent = MySqlManagedPrivilegePreflight.parseGrantForTest(
      "GRANT SELECT ON `literal\\%name`.* TO `installer`@`localhost`")
    assertEquals("SCHEMA:literal%name", literalPercent._1)

  @Test def wildcardEscapesAreRejectedOutsideSchemaGrantTargets(): Unit =
    Vector(
      "GRANT SELECT ON `some\\_schema`.`some_table` TO `installer`@`localhost`",
      "GRANT SELECT ON `some_schema`.`some\\_table` TO `installer`@`localhost`",
      "GRANT SELECT (`some\\_column`) ON `some_schema`.`some_table` TO `installer`@`localhost`",
      "GRANT EXECUTE ON PROCEDURE `some\\_schema`.`some_proc` TO `installer`@`localhost`",
      "GRANT SELECT ON `some_schema`.* TO `install\\_er`@`localhost`",
      "GRANT SELECT ON `some_schema`.* TO `installer`@`local\\_host`",
      "GRANT SELECT ON `some\\nschema`.* TO `installer`@`localhost`",
      "GRANT SELECT ON `some\\`.* TO `installer`@`localhost`"
    ).foreach(line => assertThrows(classOf[SQLDataException], () =>
      MySqlManagedPrivilegePreflight.parseGrantForTest(line)))
