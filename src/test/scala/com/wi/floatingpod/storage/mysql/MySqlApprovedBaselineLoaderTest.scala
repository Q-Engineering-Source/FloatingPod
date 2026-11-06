package com.wi.floatingpod.storage.mysql

import com.google.gson.JsonParser
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotEquals, assertNotNull, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.security.MessageDigest
import java.sql.SQLNonTransientConnectionException
import java.sql.Types
import java.util.HexFormat
import java.util.UUID

class MySqlApprovedBaselineLoaderTest:
  private val ApprovedPackSha256 = "8658374ee70881f6ebf140019df98e790b189fb640ca43d647dd164e4acd0a89"
  private val FixtureResource = "/com/wi/floatingpod/storage/mysql/approved-static-baseline.json"
  private val ExpectedIdentity = MySqlApprovedBaselineIdentity(
    "FP_MYSQL_APPROVED_STATIC_V1",
    1,
    MySqlAuditCoverage.V4,
    UUID.fromString("982937bc-bc46-11f1-b957-04421a0aa74d"),
    "9.7.2",
    "fp_aud04_base",
    UUID.fromString("eea917d4-e945-4c54-b666-5ba68d666f6f"),
    UUID.fromString("9ff22189-4730-41f6-80d8-3a3bdf971604"),
    "ba81369e7fa7d5adf3faca48a789d502d26f50dc709e3e2aa9bd5e60558c907c",
    "fpa04_base_audit@127.0.0.1",
    "AE5F92AAABF8FB881ADCE2AEA281B4E24024B282C5B0ABEC3EFBC4FC5AD3BB42"
  )

  private val Format2PackSha256 = "5d5626a4e3ed7b48ce4036788c9885b73f4fa92a9ec01424bbf7ce282029948e"
  private val Format2FixtureResource = "/com/wi/floatingpod/storage/mysql/approved-static-baseline-format2.json"
  private val ExpectedFormat2Identity = MySqlApprovedBaselineIdentity(
    "FP_MYSQL_APPROVED_STATIC_V1",
    2,
    MySqlAuditCoverage.Format2,
    UUID.fromString("982937bc-bc46-11f1-b957-04421a0aa74d"),
    "9.7.2",
    "fp_test_20261006_sec06",
    UUID.fromString("16fb58b8-a867-43b7-8e8e-03554578bf14"),
    UUID.fromString("e83a7eb6-c7fd-4094-9751-029bdef5c0cf"),
    "d638b7d4f8a235b59787ff628f408d5ba2110371736c60c0db755c759e0a1e1f",
    "fp_sec06_audit@127.0.0.1",
    "13E6930BABA77B47C71A68AE053CF79EB595AD5707D857926F18765DBF5BA83B"
  )

  @Test
  def loadsThePinnedApprovedStaticBaselineWithoutCreatingRuntimeQualification(): Unit =
    val bytes = fixtureBytes()
    assertEquals(338346, bytes.length)
    assertEquals(ApprovedPackSha256, sha256(bytes))

    withTemporaryFile(bytes) { path =>
      val baseline = MySqlApprovedBaselineLoader.load(path, ApprovedPackSha256, ExpectedIdentity)
      assertEquals(ExpectedIdentity, baseline.identity)
      assertEquals(ApprovedPackSha256, baseline.fileSha256)
      assertEquals(MySqlAuditCoverage.StaticResultSetIds, baseline.objects.resultSets.map(_.id))
      assertEquals(285, baseline.objects.resultSets.map(_.rows.size).sum)

      val firstColumn = baseline.objects.resultSets.head.columns.head
      assertEquals(MySqlAuditColumn("TABLE_SCHEMA", Types.VARCHAR, "VARCHAR", 64, 0, 1, false), firstColumn)

      val cells = baseline.objects.resultSets.flatMap(_.rows).flatMap(_.cells)
      assertTrue(cells.contains(MySqlAuditNull))
      assertTrue(cells.contains(MySqlAuditText("")))
      assertTrue(cells.exists(_.isInstanceOf[MySqlAuditBinary]))
      assertNotEquals(MySqlAuditText("00"), MySqlAuditBinary(Array[Byte](0)))
    }

  @Test
  def loadsThePinnedFormat2FixtureAndItsB1GuardsWithoutCreatingRuntimeQualification(): Unit =
    val bytes = format2FixtureBytes()
    assertEquals(382792, bytes.length)
    assertEquals(Format2PackSha256, sha256(bytes))

    withTemporaryFile(bytes) { path =>
      val baseline = MySqlApprovedBaselineLoader.load(path, Format2PackSha256, ExpectedFormat2Identity)
      assertEquals(ExpectedFormat2Identity, baseline.identity)
      assertEquals(Format2PackSha256, baseline.fileSha256)
      assertEquals(MySqlSchemaAuditProfile.Format2, baseline.objects.profile)
      assertEquals(MySqlAuditCoverage.StaticResultSetIds, baseline.objects.resultSets.map(_.id))

      val tableSet = baseline.objects.resultSets.find(_.id == "RS02_TABLES").get
      val triggerSet = baseline.objects.resultSets.find(_.id == "RS09_TRIGGERS").get
      assertEquals(9, tableSet.rows.size)
      assertEquals(18, triggerSet.rows.size)
      assertTrue(textColumnValues(tableSet, "TABLE_NAME").contains("binding_bootstrap_intent"))
      val triggerNames = textColumnValues(triggerSet, "TRIGGER_NAME")
      assertTrue(triggerNames.contains("trg_binding_bootstrap_intent_insert"))
      assertTrue(triggerNames.contains("trg_binding_bootstrap_intent_update"))
    }

  @Test
  def rejectsUnknownOrMixedFormat2EnvelopeProfilesWithTheActualDigest(): Unit =
    val original = new String(format2FixtureBytes(), StandardCharsets.UTF_8)
    val format2Coverage = "\"coverage\": \"" + MySqlAuditCoverage.Format2 + "\""
    val format1Coverage = "\"coverage\": \"" + MySqlAuditCoverage.V4 + "\""
    val mixedSchema = replaceFirst(original, "\"schemaFormat\": 2", "\"schemaFormat\": 1")
    assertFormat2RejectedTestOnlyVariant(mixedSchema.getBytes(StandardCharsets.UTF_8))

    val mixedCoverage = replaceFirst(original, format2Coverage, format1Coverage)
    assertFormat2RejectedTestOnlyVariant(mixedCoverage.getBytes(StandardCharsets.UTF_8))

    val unknownSchema = replaceFirst(original, "\"schemaFormat\": 2", "\"schemaFormat\": 3")
    assertFormat2RejectedTestOnlyVariant(unknownSchema.getBytes(StandardCharsets.UTF_8))

    val unknownEnvelope = replaceFirst(original, "FP_MYSQL_APPROVED_STATIC_V1", "FP_MYSQL_APPROVED_STATIC_V2")
    assertFormat2RejectedTestOnlyVariant(unknownEnvelope.getBytes(StandardCharsets.UTF_8))

  @Test
  def rejectsFormat2IdentityWithWrongBindingUuid(): Unit =
    withFormat2FixtureFile { path =>
      val wrongIdentity = ExpectedFormat2Identity.copy(bindingUuid = UUID.fromString("7b9c3a17-38d7-41ef-811f-527002ba0001"))
      assertThrows(
        classOf[SQLNonTransientConnectionException],
        () => MySqlApprovedBaselineLoader.load(path, Format2PackSha256, wrongIdentity)
      )
    }

  @Test
  def rejectsAFormat2FixtureMissingTheB1UpdateGuard(): Unit =
    val root = JsonParser.parseString(new String(format2FixtureBytes(), StandardCharsets.UTF_8)).getAsJsonObject
    val resultSets = root.getAsJsonArray("resultSets")
    val triggerSet = resultSets.get(7).getAsJsonObject
    assertEquals("RS09_TRIGGERS", triggerSet.get("id").getAsString)
    val triggerRows = triggerSet.getAsJsonArray("rows")
    var missingRow = -1
    var rowIndex = 0
    while rowIndex < triggerRows.size() && missingRow < 0 do
      val cells = triggerRows.get(rowIndex).getAsJsonArray
      val cell = cells.get(1).getAsJsonObject
      if cell.get("tag").getAsString == "TEXT" && cell.get("value").getAsString == "trg_binding_bootstrap_intent_update" then
        missingRow = rowIndex
      rowIndex += 1
    assertTrue(missingRow >= 0, "approved format 2 fixture contains the B1 update guard row")
    triggerRows.remove(missingRow)
    assertFormat2RejectedTestOnlyVariant(root.toString.getBytes(StandardCharsets.UTF_8))

  @Test
  def refusesMissingPathsAndNonRegularFiles(): Unit =
    val missing = Files.createTempFile("fp-approved-baseline-missing-", ".json")
    Files.deleteIfExists(missing)
    try
      assertThrows(
        classOf[SQLNonTransientConnectionException],
        () => MySqlApprovedBaselineLoader.load(missing, ApprovedPackSha256, ExpectedIdentity)
      )
    finally Files.deleteIfExists(missing)

    val directory = Files.createTempDirectory("fp-approved-baseline-directory-")
    try
      assertThrows(
        classOf[SQLNonTransientConnectionException],
        () => MySqlApprovedBaselineLoader.load(directory, ApprovedPackSha256, ExpectedIdentity)
      )
    finally Files.deleteIfExists(directory)

  @Test
  def rejectsAFileWhoseDigestDiffersFromTheIndependentApproval(): Unit =
    withFixtureFile { path =>
      val wrongDigest = "0" * 64
      assertNotEquals(ApprovedPackSha256, wrongDigest)
      assertThrows(
        classOf[SQLNonTransientConnectionException],
        () => MySqlApprovedBaselineLoader.load(path, wrongDigest, ExpectedIdentity)
      )
    }

  @Test
  def rejectsAnIdentityThatDiffersFromTheIndependentTypedContract(): Unit =
    withFixtureFile { path =>
      val wrongIdentity = ExpectedIdentity.copy(schema = "fp_aud04_other")
      assertThrows(
        classOf[SQLNonTransientConnectionException],
        () => MySqlApprovedBaselineLoader.load(path, ApprovedPackSha256, wrongIdentity)
      )
    }

  @Test
  def rejectsDuplicateObjectKeysEvenWhenTheTestDigestMatchesTheMalformedBytes(): Unit =
    val original = new String(fixtureBytes(), StandardCharsets.UTF_8)
    val field = "\"format\": \"FP_MYSQL_APPROVED_STATIC_V1\","
    val duplicate = replaceFirst(
      original,
      field,
      field + "\n  \"format\": \"FP_MYSQL_APPROVED_STATIC_V1\","
    )

    assertRejectedTestOnlyVariant(duplicate.getBytes(StandardCharsets.UTF_8), "duplicate")

  @Test
  def rejectsUnknownAndMissingRequiredProperties(): Unit =
    val original = new String(fixtureBytes(), StandardCharsets.UTF_8)
    val afterFormat = "\"format\": \"FP_MYSQL_APPROVED_STATIC_V1\","
    val withUnknown = replaceFirst(original, afterFormat, afterFormat + "\n  \"unexpected\": false,")
    assertRejectedTestOnlyVariant(withUnknown.getBytes(StandardCharsets.UTF_8), "unknown")

    val withMissingSourceHash = replaceFirst(
      original,
      "  \"sourceCaptureSha256\": \"AE5F92AAABF8FB881ADCE2AEA281B4E24024B282C5B0ABEC3EFBC4FC5AD3BB42\",\n",
      ""
    )
    assertRejectedTestOnlyVariant(withMissingSourceHash.getBytes(StandardCharsets.UTF_8), "missing")

  @Test
  def rejectsWrongJsonTypesAndIntegerOverflow(): Unit =
    val original = new String(fixtureBytes(), StandardCharsets.UTF_8)
    val wrongType = replaceFirst(original, "\"schemaFormat\": 1", "\"schemaFormat\": \"1\"")
    assertRejectedTestOnlyVariant(wrongType.getBytes(StandardCharsets.UTF_8), "JSON number")

    val overflow = replaceFirst(original, "\"schemaFormat\": 1", "\"schemaFormat\": 2147483648")
    assertRejectedTestOnlyVariant(overflow.getBytes(StandardCharsets.UTF_8), "32-bit integer range")

  @Test
  def rejectsMalformedUtf8AndUtf8BomWithTheOriginalDecodeCause(): Unit =
    val malformed = fixtureBytes()
    malformed(0) = 0xff.toByte
    withTemporaryFile(malformed) { path =>
      val failure = assertThrows(
        classOf[SQLNonTransientConnectionException],
        () => MySqlApprovedBaselineLoader.load(path, sha256(malformed), ExpectedIdentity)
      )
      assertNotNull(failure.getCause)
    }

    val original = fixtureBytes()
    val bom = Array(0xef.toByte, 0xbb.toByte, 0xbf.toByte) ++ original
    withTemporaryFile(bom) { path =>
      val failure = assertThrows(
        classOf[SQLNonTransientConnectionException],
        () => MySqlApprovedBaselineLoader.load(path, sha256(bom), ExpectedIdentity)
      )
      assertTrue(failure.getMessage.contains("BOM"))
    }

  @Test
  def rejectsTrailingJsonContent(): Unit =
    val bytes = (new String(fixtureBytes(), StandardCharsets.UTF_8) + "{}").getBytes(StandardCharsets.UTF_8)
    withTestOnlyVariant(bytes) { (path, digest) =>
      assertThrows(
        classOf[SQLNonTransientConnectionException],
        () => MySqlApprovedBaselineLoader.load(path, digest, ExpectedIdentity)
      )
    }

  @Test
  def rejectsLegacyJsonSyntaxAndUnescapedControlCharacters(): Unit =
    val original = new String(fixtureBytes(), StandardCharsets.UTF_8)
    val unquotedName = replaceFirst(original, "\"format\":", "format:")
    assertRejectedTestOnlyVariant(unquotedName.getBytes(StandardCharsets.UTF_8), "JSON")

    val controlCharacter = replaceFirst(original, "fpa04_base_audit", "fpa04_base" + 1.toChar + "_audit")
    assertRejectedTestOnlyVariant(controlCharacter.getBytes(StandardCharsets.UTF_8), "JSON")

  @Test
  def rejectsColumnShapeMismatchWithTypedStorageFailure(): Unit =
    val original = new String(fixtureBytes(), StandardCharsets.UTF_8)
    val changedLabel = replaceFirst(original, "\"label\": \"TABLE_SCHEMA\"", "\"label\": \"BROKEN_LABEL\"")
    withTestOnlyVariant(changedLabel.getBytes(StandardCharsets.UTF_8)) { (path, digest) =>
      assertThrows(
        classOf[SQLNonTransientConnectionException],
        () => MySqlApprovedBaselineLoader.load(path, digest, ExpectedIdentity)
      )
    }

  @Test
  def requiresUppercaseBinaryHex(): Unit =
    val original = new String(fixtureBytes(), StandardCharsets.UTF_8)
    val prefix = "\"hex\": \""
    val valueStart = original.indexOf(prefix) + prefix.length
    assertTrue(valueStart >= prefix.length)
    val valueEnd = original.indexOf('"', valueStart)
    val encoded = original.substring(valueStart, valueEnd)
    val index = encoded.indexWhere(character => character >= 'A' && character <= 'F')
    assertTrue(index >= 0)
    val lowered = encoded.updated(index, encoded.charAt(index).toLower)
    val changed = original.substring(0, valueStart) + lowered + original.substring(valueEnd)
    assertRejectedTestOnlyVariant(changed.getBytes(StandardCharsets.UTF_8), "uppercase")

  @Test
  def preservesWhitespaceTextAndDoesNotHardcodeApprovedColumnMetadata(): Unit =
    val original = new String(fixtureBytes(), StandardCharsets.UTF_8)
    val whitespaceVariant = replaceFirst(original, "\"value\": \"\"", "\"value\": \"  \"")
    withTestOnlyVariant(whitespaceVariant.getBytes(StandardCharsets.UTF_8)) { (path, digest) =>
      val baseline = MySqlApprovedBaselineLoader.load(path, digest, ExpectedIdentity)
      val cells = baseline.objects.resultSets.flatMap(_.rows).flatMap(_.cells)
      assertTrue(cells.contains(MySqlAuditText("  ")))
    }

    val metadataVariant = replaceFirst(original, "\"nullable\": 1", "\"nullable\": 0")
    withTestOnlyVariant(metadataVariant.getBytes(StandardCharsets.UTF_8)) { (path, digest) =>
      val baseline = MySqlApprovedBaselineLoader.load(path, digest, ExpectedIdentity)
      assertEquals(0, baseline.objects.resultSets.head.columns.head.nullable)
    }

    withFixtureFile { path =>
      val changedMetadata = replaceFirst(original, "\"nullable\": 1", "\"nullable\": 0").getBytes(StandardCharsets.UTF_8)
      Files.write(path, changedMetadata)
      assertThrows(
        classOf[SQLNonTransientConnectionException],
        () => MySqlApprovedBaselineLoader.load(path, ApprovedPackSha256, ExpectedIdentity)
      )
    }

  @Test
  def enforcesTheApprovedFileAndDecodedCellByteBounds(): Unit =
    val tooLargeFile = new Array[Byte](MySqlApprovedBaselineLoader.MaxApprovedFileBytes + 1)
    withTemporaryFile(tooLargeFile) { path =>
      val failure = assertThrows(
        classOf[SQLNonTransientConnectionException],
        () => MySqlApprovedBaselineLoader.load(path, sha256(tooLargeFile), ExpectedIdentity)
      )
      assertTrue(failure.getMessage.contains("file exceeds the byte bound"))
    }

    val original = new String(fixtureBytes(), StandardCharsets.UTF_8)
    val oversizedValue = "\"value\": \"" + "x".repeat(MySqlSchemaAuditor.MaxCellBytes + 1) + "\""
    val overlongCell = replaceFirst(original, "\"value\": \"fp_aud04_base\"", oversizedValue)
    withTestOnlyVariant(overlongCell.getBytes(StandardCharsets.UTF_8)) { (path, digest) =>
      val failure = assertThrows(
        classOf[SQLNonTransientConnectionException],
        () => MySqlApprovedBaselineLoader.load(path, digest, ExpectedIdentity)
      )
      assertTrue(failure.getMessage.contains("UTF-8 byte bound"))
    }

  private val OwnedFormat3Sha = "96F8E58D60A8B373FF9D4AC750B7FECD3E1A0FDD2E11E06B38EDA700413B9702"
  private val OwnedFormat3Identity = MySqlApprovedBaselineIdentity(
    "FP_MYSQL_APPROVED_STATIC_V1", 3, MySqlAuditCoverage.Format3,
    UUID.fromString("982937bc-bc46-11f1-b957-04421a0aa74d"), "9.7.2", "fp_ordinary_synthetic",
    UUID.fromString("16fb58b8-a867-43b7-8e8e-03554578bf14"), UUID.fromString("e83a7eb6-c7fd-4094-9751-029bdef5c0cf"),
    "74FFE4EC6176D184EA1C840EC71954DE3E150261F26CC6EF8C9511910AEBA920", "fp_audit@localhost",
    "34D10D2AEB0C73241C67E5A0D7EFD86752F7F7C43DA16F27765DA4A748BADF74")
  private val OwnedProfile = com.wi.floatingpod.config.SourceLimitProfile.of(1024,2048,4096,32,100,8L*1024*1024,8192,16384)
  private def ownedRoot(ledger: MySqlFormat3AllocationLedger): MySqlFormat3ReadBudget =
    MySqlFormat3ReadBudget.rootOnLedger(OwnedProfile,OwnedProfile.maxSqlPacketBytes(),ledger)
  private def withOwnedFormat3[A](body: Path => A): A =
    val directory=Files.createTempDirectory("fp-owned-B-test-")
    try
      // 只生成独立定义fixture的synthetic DATA文件；不以旧load返回对象构造新API golden。
      MySqlOrdinaryCertifiedHandoffFixture.loadIndependentStaticBaseline(directory)
      val path=directory.resolve("approved-static-baseline-format3-ordinary-synthetic.json")
      assertEquals(386887L,Files.size(path));assertEquals(OwnedFormat3Sha,sha256(Files.readAllBytes(path)).toUpperCase(java.util.Locale.ROOT))
      body(path)
    finally
      val paths=Files.walk(directory)
      try
        val all=paths.sorted(java.util.Comparator.reverseOrder()).toList()
        all.forEach(path => Files.delete(path))
      finally paths.close()

  @Test def ownedFormat3KeepsExactIndependentApprovalAndLiveGraphUntilClose(): Unit = withOwnedFormat3 { path =>
    val ledger=new MySqlFormat3AllocationLedger(OwnedProfile.maxRetainedBytes());val root=ownedRoot(ledger)
    val other=ownedRoot(ledger)
    val owner=MySqlApprovedBaselineLoader.loadOwned(path,OwnedFormat3Sha,OwnedFormat3Identity,root)
    try
      val baseline=owner.baselineFor(root)
      assertEquals(OwnedFormat3Identity,baseline.identity);assertEquals(OwnedFormat3Sha.toLowerCase(java.util.Locale.ROOT),baseline.fileSha256)
      assertEquals(MySqlSchemaAuditProfile.Format3,baseline.objects.profile)
      assertEquals(MySqlAuditCoverage.StaticResultSetIds,baseline.objects.resultSets.map(_.id))
      assertEquals(20,baseline.objects.resultSets.head.rows.size);assertEquals(368,baseline.objects.resultSets(1).rows.size)
      assertTrue(ledger.accountedBytes > 0L)
      assertThrows(classOf[SQLNonTransientConnectionException],() => owner.baselineFor(other))
      owner.close();assertEquals(0L,ledger.accountedBytes);assertTrue(!root.isClosed)
      assertThrows(classOf[SQLNonTransientConnectionException],() => owner.baselineFor(root))
      owner.close();assertEquals(0L,ledger.accountedBytes)
    finally
      owner.close();other.closeRootReadBudget();root.closeRootReadBudget()
  }
  @Test def ownedFailuresPreserveIndependentPinIdentityAndManifestChecksWithoutDebt(): Unit = withOwnedFormat3 { path =>
    val ledger=new MySqlFormat3AllocationLedger(OwnedProfile.maxRetainedBytes());val root=ownedRoot(ledger)
    try
      assertThrows(classOf[SQLNonTransientConnectionException],() => MySqlApprovedBaselineLoader.loadOwned(path,"0".repeat(64),OwnedFormat3Identity,root))
      assertEquals(0L,ledger.accountedBytes)
      assertThrows(classOf[SQLNonTransientConnectionException],() => MySqlApprovedBaselineLoader.loadOwned(path,OwnedFormat3Sha,OwnedFormat3Identity.copy(schema="other_schema"),root))
      assertEquals(0L,ledger.accountedBytes)
      val json=JsonParser.parseString(Files.readString(path)).getAsJsonObject
      val rows=json.getAsJsonArray("resultSets").get(0).getAsJsonObject.getAsJsonArray("rows")
      rows.add(rows.get(0).deepCopy())
      withTemporaryFile(json.toString.getBytes(StandardCharsets.UTF_8)) { changed =>
        assertThrows(classOf[SQLNonTransientConnectionException],() => MySqlApprovedBaselineLoader.loadOwned(changed,sha256(Files.readAllBytes(changed)),OwnedFormat3Identity,root))
      }
      assertEquals(0L,ledger.accountedBytes)
    finally root.closeRootReadBudget()
  }
  @Test def ownedStrictUtf8AndMissingFileRetainActualCausesAndNoDebt(): Unit = withOwnedFormat3 { path =>
    val ledger=new MySqlFormat3AllocationLedger(OwnedProfile.maxRetainedBytes());val root=ownedRoot(ledger)
    try
      val bytes=Files.readAllBytes(path);bytes(0)=0xC0.toByte
      withTemporaryFile(bytes) { malformed =>
        val failure=assertThrows(classOf[SQLNonTransientConnectionException],() => MySqlApprovedBaselineLoader.loadOwned(malformed,sha256(bytes),OwnedFormat3Identity,root))
        assertTrue(failure.getCause.isInstanceOf[java.nio.charset.CharacterCodingException])
      }
      val missing=path.resolveSibling("missing.json")
      val failure=assertThrows(classOf[SQLNonTransientConnectionException],() => MySqlApprovedBaselineLoader.loadOwned(missing,OwnedFormat3Sha,OwnedFormat3Identity,root))
      assertTrue(failure.getCause.isInstanceOf[java.nio.file.NoSuchFileException]);assertEquals(0L,ledger.accountedBytes)
      assertThrows(classOf[SQLNonTransientConnectionException],() => MySqlApprovedBaselineLoader.loadOwned(path.getParent,OwnedFormat3Sha,OwnedFormat3Identity,root))
      assertEquals(0L,ledger.accountedBytes)
    finally root.closeRootReadBudget()
  }
  @Test def ownedQuotaIsCheckedBeforeIOAndRetainedGraphSharesTheActualLedger(): Unit = withOwnedFormat3 { path =>
    val tiny=new MySqlFormat3AllocationLedger(1024L);val tinyRoot=ownedRoot(tiny)
    try
      val failure=assertThrows(classOf[java.sql.SQLDataException],() => MySqlApprovedBaselineLoader.loadOwned(path.resolveSibling("missing"),OwnedFormat3Sha,OwnedFormat3Identity,tinyRoot))
      assertTrue(failure.getMessage.contains("budget"));assertEquals(0L,tiny.accountedBytes)
    finally tinyRoot.closeRootReadBudget()
    val ledger=new MySqlFormat3AllocationLedger(OwnedProfile.maxRetainedBytes());val root=ownedRoot(ledger);val sibling=ownedRoot(ledger)
    val owner=MySqlApprovedBaselineLoader.loadOwned(path,OwnedFormat3Sha,OwnedFormat3Identity,root)
    try
      val held=ledger.accountedBytes;val rest=ledger.availableBytes
      sibling.reserveTemporaryMemory(rest,"test occupies actual shared remaining space")
      try assertThrows(classOf[java.sql.SQLDataException],() => MySqlApprovedBaselineLoader.loadOwned(path,OwnedFormat3Sha,OwnedFormat3Identity,sibling))
      finally sibling.releaseTemporaryMemory(rest,"test shared space dropped")
      assertEquals(held,ledger.accountedBytes);owner.close();assertEquals(0L,ledger.accountedBytes)
    finally
      owner.close();sibling.closeRootReadBudget();root.closeRootReadBudget()
  }
  @Test def ownedCensusBoundsUnknownEscapedNamesAndHugeBareNumbersBeforeTheRealParser(): Unit = withOwnedFormat3 { path =>
    val original=Files.readString(path)
    val badName="{\""+"\\u0061".repeat(10000)+"\":0, "+original.substring(1)
    val badNumber=original.replace("\"schemaFormat\":3","\"schemaFormat\":"+"9".repeat(10000))
    assertTrue(badNumber != original)
    for text <- Vector(badName,badNumber) do
      val ledger=new MySqlFormat3AllocationLedger(OwnedProfile.maxRetainedBytes());val root=ownedRoot(ledger)
      try withTemporaryFile(text.getBytes(StandardCharsets.UTF_8)) { malformed =>
        assertThrows(classOf[SQLNonTransientConnectionException],() => MySqlApprovedBaselineLoader.loadOwned(malformed,sha256(Files.readAllBytes(malformed)),OwnedFormat3Identity,root))
        assertEquals(0L,ledger.accountedBytes)
      }
      finally root.closeRootReadBudget()
  }

  @Test def ownedStageCapsFailBeforeTheirAllocationAndCallerMustCloseRootLast(): Unit = withOwnedFormat3 { path =>
    val n=386887L
    val rawCap=65536L+n+64L-1L
    val textCap=65536L+n+64L+2L*n+64L-1L
    val decoderCap=65536L+n+64L+2L*n+64L+2L*n+4096L-1L
    // 明确synthetic DATA测试cap；不定义生产profile，不使用fake allocation callback。
    for (cap,context) <- Vector((rawCap,"baseline raw bytes"),(textCap,"baseline decoded text"),
        (decoderCap,"baseline UTF8 decoder"),(4L*1024*1024,"baseline typed graph"),(5500000L,"baseline parser/manifest workspace")) do
      val ledger=new MySqlFormat3AllocationLedger(cap);val root=ownedRoot(ledger)
      try
        val failure=assertThrows(classOf[java.sql.SQLDataException],() => MySqlApprovedBaselineLoader.loadOwned(path,OwnedFormat3Sha,OwnedFormat3Identity,root))
        assertTrue(failure.getMessage.contains(context),failure.getMessage);assertEquals(0L,ledger.accountedBytes)
      finally root.closeRootReadBudget()
    val ledger=new MySqlFormat3AllocationLedger(OwnedProfile.maxRetainedBytes());val root=ownedRoot(ledger)
    val owner=MySqlApprovedBaselineLoader.loadOwned(path,OwnedFormat3Sha,OwnedFormat3Identity,root)
    root.closeRootReadBudget() // caller违反顺序，不得由owner.close假报结算成功。
    assertThrows(classOf[SQLNonTransientConnectionException],() => owner.baselineFor(root))
    assertThrows(classOf[IllegalStateException],() => owner.close())
    assertThrows(classOf[IllegalStateException],() => owner.close())
  }

  private def assertRejectedTestOnlyVariant(bytes: Array[Byte], expectedMessage: String): Unit =
    withTestOnlyVariant(bytes) { (path, digest) =>
      val failure = assertThrows(
        classOf[SQLNonTransientConnectionException],
        () => MySqlApprovedBaselineLoader.load(path, digest, ExpectedIdentity)
      )
      assertTrue(failure.getMessage.contains(expectedMessage))
    }

  private def withTestOnlyVariant[A](bytes: Array[Byte])(body: (Path, String) => A): A =
    withTemporaryFile(bytes) { path =>
      body(path, sha256(bytes))
    }

  private def withFixtureFile[A](body: Path => A): A =
    withTemporaryFile(fixtureBytes())(body)

  private def withTemporaryFile[A](bytes: Array[Byte])(body: Path => A): A =
    val path = Files.createTempFile("fp-mysql-approved-static-baseline-", ".json")
    try
      Files.write(path, bytes)
      body(path)
    finally Files.deleteIfExists(path)

  private def assertFormat2RejectedTestOnlyVariant(bytes: Array[Byte]): Unit =
    withTemporaryFile(bytes) { path =>
      assertThrows(
        classOf[SQLNonTransientConnectionException],
        () => MySqlApprovedBaselineLoader.load(path, sha256(bytes), ExpectedFormat2Identity)
      )
    }

  private def withFormat2FixtureFile[A](body: Path => A): A =
    withTemporaryFile(format2FixtureBytes())(body)

  private def format2FixtureBytes(): Array[Byte] =
    val input = getClass.getResourceAsStream(Format2FixtureResource)
    if input == null then throw new IllegalStateException("Approved format 2 baseline test resource is unavailable")
    try input.readAllBytes()
    finally input.close()

  private def textColumnValues(resultSet: MySqlAuditResultSet, label: String): Vector[String] =
    val column = resultSet.columns.indexWhere(_.label == label)
    assertTrue(column >= 0, s"$label column exists in ${resultSet.id}")
    resultSet.rows.map { row =>
      row.cells(column) match
        case MySqlAuditText(value) => value
        case _ => throw new AssertionError(s"$label cell in ${resultSet.id} is text")
    }

  private def fixtureBytes(): Array[Byte] =
    val input = getClass.getResourceAsStream(FixtureResource)
    if input == null then throw new IllegalStateException("Approved static baseline test resource is unavailable")
    try input.readAllBytes()
    finally input.close()

  private def replaceFirst(source: String, needle: String, replacement: String): String =
    val index = source.indexOf(needle)
    assertTrue(index >= 0, "test fixture mutation anchor is present")
    source.substring(0, index) + replacement + source.substring(index + needle.length)

  private def sha256(bytes: Array[Byte]): String =
    HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes))
