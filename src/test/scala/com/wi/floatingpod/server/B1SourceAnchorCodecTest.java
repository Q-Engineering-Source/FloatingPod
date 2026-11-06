package com.wi.floatingpod.server;

import com.wi.floatingpod.config.MySqlRuntimeMetadata;
import com.wi.floatingpod.storage.mysql.MySqlSessionBarrierNamespace;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Properties;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import static org.junit.jupiter.api.Assertions.*;

@Tag("b1-anchor-pure")
class B1SourceAnchorCodecTest {
    @TempDir Path temporary;
    @Test void fullCanonicalRoundTripPreservesReceiptFileTimePlanAndConfiguration() throws Exception {
        var data = B1SourceAnchorFixture.data(temporary); byte[] bytes = B1SourceAnchorCodec.encode(data);
        assertEquals(0x46504231414e4334L,ByteBuffer.wrap(bytes).getLong());
        assertEquals(4,ByteBuffer.wrap(bytes).getInt(8));
        var decoded = B1SourceAnchorCodec.decode(bytes);
        assertArrayEquals(bytes, B1SourceAnchorCodec.encode(decoded));
        assertArrayEquals(data.receipt(), decoded.receipt());
        assertArrayEquals(data.configuration(), decoded.configuration());
        var receipt = WindowsWorldProvisioner.decodeReceipt(decoded.receipt());
        assertEquals(B1SourceAnchorFixture.plan(), receipt.plan());
        assertEquals(B1SourceAnchorFixture.FILETIME, receipt.creatorCreationTime());
        assertEquals(B1SourceAnchorFixture.receiptData().creationBoot(), receipt.creationBoot());
        assertEquals(data.receiptFileIdentity(), decoded.receiptFileIdentity());
        assertEquals(data.configParentIdentity(), decoded.configParentIdentity());
        assertEquals(data.metadataFileIdentity(), decoded.metadataFileIdentity());
        assertEquals(data.worldNativePath(), decoded.worldNativePath());
        assertEquals(data.configParentNativePath(), decoded.configParentNativePath());
    }
    @Test void sharedNamespaceFactoryIsTheOnlyAuthorityForLockName() throws Exception {
        var m = B1SourceAnchorFixture.metadata(temporary);
        assertEquals(MySqlSessionBarrierNamespace.forIdentity(m.expectedServerUuid(), m.schema(),
            m.baseline().provisioningUuid(), m.expectedBindingUuid()).lockName(), B1SourceAnchorCodec.namespace(m));
        assertEquals(48, B1SourceAnchorCodec.namespace(m).length());
    }
    @Test void fullConfigurationDigestIncludesEndpointAndIndependentBaselinePins() throws Exception {
        var m = B1SourceAnchorFixture.metadata(temporary);
        byte[] original = B1SourceAnchorCodec.configurationBytes(m);
        var hostChanged = MySqlRuntimeMetadata.fromBytes(B1SourceAnchorFixture.properties(temporary)
            .replace("database.example.invalid", "other.example.invalid").getBytes(StandardCharsets.UTF_8));
        assertFalse(Arrays.equals(original, B1SourceAnchorCodec.configurationBytes(hostChanged)));
        var baselineChanged = MySqlRuntimeMetadata.fromBytes(B1SourceAnchorFixture.properties(temporary)
            .replace("approvedBaselineSha256=" + "a".repeat(64), "approvedBaselineSha256=" + "c".repeat(64)).getBytes(StandardCharsets.UTF_8));
        assertFalse(Arrays.equals(original, B1SourceAnchorCodec.configurationBytes(baselineChanged)));
        assertEquals(m.host(), MySqlRuntimeMetadata.fromBytes(original).host());
    }
    @Test void receiptPlanConfigurationMismatchFailsBeforeEncoding() throws Exception {
        var data = B1SourceAnchorFixture.data(temporary);
        var changed = MySqlRuntimeMetadata.fromBytes(B1SourceAnchorFixture.properties(temporary)
            .replace("expectedBindingUuid=" + B1SourceAnchorFixture.plan().bindingUuid(),
                "expectedBindingUuid=4e777434-ec21-45c6-8899-c0e871887bf9").getBytes(StandardCharsets.UTF_8));
        var mismatch = new B1SourceAnchorCodec.Data(B1SourceAnchorCodec.configurationBytes(changed), data.receipt(),
            data.receiptFileIdentity(), data.configParentIdentity(), data.metadataFileIdentity(),
            data.configParentNativePath(), data.worldNativePath(), data.metadataFilename());
        assertThrows(IOException.class, () -> B1SourceAnchorCodec.encode(mismatch));
    }
    @Test void tamperTruncationTailUnknownVersionAndOversizeFail() throws Exception {
        byte[] good = B1SourceAnchorCodec.encode(B1SourceAnchorFixture.data(temporary));
        byte[] tamper = good.clone(); tamper[40] ^= 1;
        assertThrows(IOException.class, () -> B1SourceAnchorCodec.decode(tamper));
        assertThrows(IOException.class, () -> B1SourceAnchorCodec.decode(Arrays.copyOf(good, good.length - 1)));
        assertThrows(IOException.class, () -> B1SourceAnchorCodec.decode(Arrays.copyOf(good, good.length + 1)));
        byte[] version = good.clone(); version[11] = 3;
        assertThrows(IOException.class, () -> B1SourceAnchorCodec.decode(version));
        assertThrows(IOException.class, () -> B1SourceAnchorCodec.decode(new byte[65537]));
    }
    @Test void v3AnchorMagicUnderV4NameIsRejected() throws Exception {
        byte[] old=B1SourceAnchorCodec.encode(B1SourceAnchorFixture.data(temporary));
        ByteBuffer.wrap(old).putLong(0x46504231414e4333L);
        assertThrows(IOException.class,()->B1SourceAnchorCodec.decode(old));
    }
    @Test void malformedCanonicalUtf8KeepsDecoderCause() throws Exception {
        byte[] good = B1SourceAnchorCodec.encode(B1SourceAnchorFixture.data(temporary));
        byte[] bad = good.clone(); bad[20] = (byte) 0xc0;
        int size = ByteBuffer.wrap(bad).getInt(12);
        byte[] body = Arrays.copyOfRange(bad, 16, 16 + size);
        System.arraycopy(WindowsWorldProvisioner.digest(body), 0, bad, bad.length - 32, 32);
        IOException failure = assertThrows(IOException.class, () -> B1SourceAnchorCodec.decode(bad));
        assertInstanceOf(CharacterCodingException.class, failure.getCause());
    }
    @Test void namespaceAndSlotsCannotBeChangedEvenWithRecomputedEnvelopeHash() throws Exception {
        byte[] good = B1SourceAnchorCodec.encode(B1SourceAnchorFixture.data(temporary));
        byte[] bad = good.clone();
        byte[] slot = B1SourceAnchorCodec.SLOT0.getBytes(StandardCharsets.UTF_8);
        int offset = indexOf(bad, slot); assertTrue(offset >= 0); bad[offset + slot.length - 1] = '9';
        int size = ByteBuffer.wrap(bad).getInt(12);
        System.arraycopy(WindowsWorldProvisioner.digest(Arrays.copyOfRange(bad, 16, 16 + size)), 0, bad, bad.length - 32, 32);
        assertThrows(IOException.class, () -> B1SourceAnchorCodec.decode(bad));
    }
    @Test void filenameAliasesAndNativeWorldContainmentAreRejected() throws Exception {
        for (String bad : new String[]{"..", "x.", "x ", "x:stream", "RUNTIM~1.properties", "CON", "nul.txt", "LPT1.txt"})
            assertThrows(IllegalArgumentException.class, () -> WindowsB1SourceAnchorStore.requireFilename(bad));
        assertThrows(IOException.class, () -> WindowsB1SourceAnchorStore.requireNativeOutside(B1SourceAnchorFixture.WORLD, B1SourceAnchorFixture.WORLD));
        assertThrows(IOException.class, () -> WindowsB1SourceAnchorStore.requireNativeOutside(B1SourceAnchorFixture.WORLD + "\\config", B1SourceAnchorFixture.WORLD));
        WindowsB1SourceAnchorStore.requireNativeOutside(B1SourceAnchorFixture.WORLD + "-other", B1SourceAnchorFixture.WORLD);
        assertThrows(IOException.class, () -> WindowsB1SourceAnchorStore.requireNativeOutside("C:\\config", "C:\\world"));
        assertThrows(IOException.class, () -> WindowsB1SourceAnchorStore.requireExactNativeChild(B1SourceAnchorFixture.PARENT,
            B1SourceAnchorFixture.PARENT + "\\other.properties", "runtime.properties"));
    }
    @Test void oversizedTextAndUnpairedSurrogatesFailRatherThanTruncate() throws Exception {
        var d = B1SourceAnchorFixture.data(temporary);
        var longPath = new B1SourceAnchorCodec.Data(d.configuration(), d.receipt(), d.receiptFileIdentity(), d.configParentIdentity(),
            d.metadataFileIdentity(), d.configParentNativePath() + "x".repeat(5000), d.worldNativePath(), d.metadataFilename());
        assertThrows(IOException.class, () -> B1SourceAnchorCodec.encode(longPath));
        var invalid = new B1SourceAnchorCodec.Data(d.configuration(), d.receipt(), d.receiptFileIdentity(), d.configParentIdentity(),
            d.metadataFileIdentity(), d.configParentNativePath() + "\ud800", d.worldNativePath(), d.metadataFilename());
        IOException failure = assertThrows(IOException.class, () -> B1SourceAnchorCodec.encode(invalid));
        assertInstanceOf(CharacterCodingException.class, failure.getCause());
    }
    private static int indexOf(byte[] bytes, byte[] wanted) {
        for (int i = 0; i <= bytes.length - wanted.length; i++)
            if (Arrays.equals(Arrays.copyOfRange(bytes, i, i + wanted.length), wanted)) return i;
        return -1;
    }
    private String fullV2Properties() throws Exception {
        var values=new Properties();values.load(new StringReader(B1SourceAnchorFixture.properties(temporary)));
        values.setProperty("approvedSchemaFormat","3");
        values.setProperty("approvedCoverage","format3:all-twenty-fixed-schema-tables-and-static-objects;all-sixty-fixed-schema-triggers;required-auditself-only;other-routine-absence-unclaimed");
        values.setProperty("format3ManifestConfigurationEncoding",MySqlRuntimeMetadata.MANIFEST_V2);
        values.setProperty("format3RuntimeContractPath",temporary.resolve("contract.json").toAbsolutePath().toString());
        values.setProperty("format3RuntimeContractSha256","C".repeat(64));
        for (String key:new String[]{"pkcs12TrustStorePath","approvedBaselinePath"}) values.setProperty(key,Path.of(values.getProperty(key)).toString());
        for (String key:new String[]{"credentialSourceRef","trustSecretSourceRef"}) values.setProperty(key,"file:"+Path.of(values.getProperty(key).substring(5)));
        var writer=new StringWriter();values.store(writer,null);return writer.toString();
    }
    private MySqlRuntimeMetadata.FullV2 fullV2(String properties) throws Exception {
        return MySqlRuntimeMetadata.fromBytesV2(properties.getBytes(StandardCharsets.UTF_8));
    }
    private B1SourceAnchorCodec.Data v2Data(byte[] configuration) throws Exception {
        var p=B1SourceAnchorFixture.plan();
        var plan=new WindowsWorldProvisioner.Plan(p.creationOperationUuid(),p.bootstrapOperationUuid(),p.prepareWriteUuid(),
            p.certifyWriteUuid(),p.bindingUuid(),p.physicalFormat(),p.mysqlServerUuid(),p.schema(),3,p.ddlSha256(),p.provisioningUuid());
        var old=B1SourceAnchorFixture.receiptData();
        byte[] receipt=WindowsWorldProvisioner.encodeReceipt(new WindowsWorldProvisioner.ReceiptData(plan,old.sourceIdentity(),
            old.guardIdentity(),old.creatorPid(),old.creatorCreationTime(),old.creationBoot()));
        return new B1SourceAnchorCodec.Data(configuration,receipt,B1SourceAnchorFixture.identity(3),B1SourceAnchorFixture.identity(4),
            B1SourceAnchorFixture.identity(5),B1SourceAnchorFixture.PARENT,B1SourceAnchorFixture.WORLD,"runtime.properties");
    }
    @Test void explicitV2RoundTripKeepsOuterV4FramingCompleteAAndLegacySeparation() throws Exception {
        var metadata=fullV2(fullV2Properties());byte[] config=B1SourceAnchorCodec.configurationBytesV2(metadata);
        var data=v2Data(config);byte[] bytes=B1SourceAnchorCodec.encodeV2(data);
        assertEquals(B1SourceAnchorCodec.MAGIC,ByteBuffer.wrap(bytes).getLong());assertEquals(4,ByteBuffer.wrap(bytes).getInt(8));
        assertArrayEquals(bytes,B1SourceAnchorCodec.encodeV2(B1SourceAnchorCodec.decodeV2(bytes)));
        assertArrayEquals(config,B1SourceAnchorCodec.decodeV2(bytes).configuration());
        assertEquals(B1SourceAnchorCodec.namespace(B1SourceAnchorFixture.metadata(temporary)),B1SourceAnchorCodec.canonicalConfigurationNamespaceV2(config));
        assertThrows(IOException.class,()->B1SourceAnchorCodec.encode(data));
        assertThrows(IOException.class,()->B1SourceAnchorCodec.decode(bytes));
        byte[] legacy=B1SourceAnchorCodec.encode(B1SourceAnchorFixture.data(temporary));
        assertThrows(IOException.class,()->B1SourceAnchorCodec.decodeV2(legacy));
        assertArrayEquals(legacy,B1SourceAnchorCodec.encode(B1SourceAnchorCodec.decode(legacy)));
        assertEquals(WindowsWorldProvisioner.decodeReceipt(data.receipt()).plan(),WindowsWorldProvisioner.decodeReceipt(B1SourceAnchorCodec.decodeV2(bytes).receipt()).plan());
    }
    @Test void v2AStillCommitsEachFullFilePinAndExactReencodeRejectsAliases() throws Exception {
        String properties=fullV2Properties();byte[] original=B1SourceAnchorCodec.configurationBytesV2(fullV2(properties));
        for (String[] edit:new String[][]{{"a".repeat(64),"E".repeat(64)},{"b".repeat(64),"F".repeat(64)},{"C".repeat(64),"D".repeat(64)}}) {
            byte[] changed=B1SourceAnchorCodec.configurationBytesV2(fullV2(properties.replace(edit[0],edit[1])));
            assertFalse(Arrays.equals(original,changed));
            assertFalse(Arrays.equals(WindowsWorldProvisioner.digest(original),WindowsWorldProvisioner.digest(changed)));
            assertThrows(IOException.class,()->B1SourceAnchorCodec.requireExact(original,changed,"old complete A approval differs"));
        }
        String text=new String(original,StandardCharsets.UTF_8);
        for (String bad:new String[]{
            text.replace("FP_B1_CONFIGURATION_FORMAT3_V2","FP_B1_CONFIGURATION_FORMAT3_V1"),
            text.replace("FP_B1_CONFIGURATION_FORMAT3_CONTRACT_PIN_V2","FP_B1_CONFIGURATION_FORMAT3_CONTRACT_PIN_V1"),
            text.replace("format3ManifestConfigurationEncoding="+MySqlRuntimeMetadata.MANIFEST_V2+"\n",""),
            text.replace(MySqlRuntimeMetadata.MANIFEST_V2,"OLD"),
            text+"format3ConfigurationEncoding=FP_B1_CONFIGURATION_FORMAT3_V2\n",
            text.replace("format3ConfigurationEncoding=FP_B1_CONFIGURATION_FORMAT3_V2\n",""),
            text.replace("approvedSchemaFormat=3","approvedSchemaFormat=2"),text+"\n",text+"unknown=value\n"}) {
            byte[] malformed=bad.getBytes(StandardCharsets.UTF_8);
            assertThrows(IOException.class,()->B1SourceAnchorCodec.canonicalConfigurationNamespaceV2(malformed));
            assertThrows(IOException.class,()->B1SourceAnchorCodec.encodeV2(v2Data(malformed)));
        }
    }
    @Test void v2EnvelopeTamperTailAndMalformedUtf8FailWithActualCause() throws Exception {
        byte[] good=B1SourceAnchorCodec.encodeV2(v2Data(B1SourceAnchorCodec.configurationBytesV2(fullV2(fullV2Properties()))));
        assertThrows(IOException.class,()->B1SourceAnchorCodec.decodeV2(Arrays.copyOf(good,good.length+1)));
        byte[] invalid=good.clone();invalid[20]=(byte)0xC0;
        int size=ByteBuffer.wrap(invalid).getInt(12);
        System.arraycopy(WindowsWorldProvisioner.digest(Arrays.copyOfRange(invalid,16,16+size)),0,invalid,invalid.length-32,32);
        var failure=assertThrows(IOException.class,()->B1SourceAnchorCodec.decodeV2(invalid));
        assertInstanceOf(CharacterCodingException.class,failure.getCause());
    }

    @Test void v2DecodedUnicodeCannotLoseAccountMeaningInFullA() throws Exception {
        String good=fullV2Properties();
        for (String escaped:new String[]{"\\uD800","\\uD801","\\uDC00"}) {
            String malformed=good.replaceAll("(?m)^username=.*$","username="+java.util.regex.Matcher.quoteReplacement(escaped));
            assertThrows(IllegalArgumentException.class,()-> {
                var metadata=fullV2(malformed);
                B1SourceAnchorCodec.encodeV2(v2Data(B1SourceAnchorCodec.configurationBytesV2(metadata)));
            });
        }
        String paired=good.replaceAll("(?m)^username=.*$","username="+java.util.regex.Matcher.quoteReplacement("\\uD83D\\uDE80"));
        var metadata=fullV2(paired);assertEquals("\uD83D\uDE80",metadata.username());
        byte[] configuration=B1SourceAnchorCodec.configurationBytesV2(metadata);
        assertTrue(new String(configuration,StandardCharsets.UTF_8).contains("username=\uD83D\uDE80\n"));
        byte[] fullA=B1SourceAnchorCodec.encodeV2(v2Data(configuration));
        assertArrayEquals(configuration,B1SourceAnchorCodec.decodeV2(fullA).configuration());
        assertArrayEquals(fullA,B1SourceAnchorCodec.encodeV2(B1SourceAnchorCodec.decodeV2(fullA)));
    }
    @Test void storeV2TypedConfigurationUsesCompleteAAndActualCodecDispatch() throws Exception {
        String properties=fullV2Properties();var metadata=fullV2(properties);
        var expected=WindowsB1SourceAnchorStore.AnchorConfiguration.ofMetadataV2(metadata);
        byte[] configuration=B1SourceAnchorCodec.configurationBytesV2(metadata);
        assertEquals(WindowsWorldProvisioner.sha256(configuration),expected.configurationSha256());
        expected.requireMetadata(properties.getBytes(StandardCharsets.UTF_8),"same handle metadata differs");
        var data=v2Data(configuration);byte[] anchor=expected.encode(data);
        assertArrayEquals(B1SourceAnchorCodec.encodeV2(data),anchor);
        assertArrayEquals(configuration,expected.decode(anchor).configuration());
        assertArrayEquals(anchor,expected.encode(expected.decode(anchor)));
        assertEquals(B1SourceAnchorCodec.canonicalConfigurationNamespaceV2(configuration),expected.namespace(configuration));
        byte[] malformed=properties.getBytes(StandardCharsets.UTF_8);malformed[0]=(byte)0xC0;
        assertThrows(CharacterCodingException.class,()->expected.requireMetadata(malformed,"malformed metadata"));
    }
    @Test void storeV2MetadataReplayAndAnchorRejectEachChangedIndependentPin() throws Exception {
        String properties=fullV2Properties();var expected=WindowsB1SourceAnchorStore.AnchorConfiguration.ofMetadataV2(fullV2(properties));
        for (String[] edit:new String[][]{{"a".repeat(64),"E".repeat(64)},{"b".repeat(64),"F".repeat(64)},{"C".repeat(64),"D".repeat(64)}}) {
            String changed=properties.replace(edit[0],edit[1]);
            byte[] configuration=B1SourceAnchorCodec.configurationBytesV2(fullV2(changed));
            assertThrows(IOException.class,()->expected.requireMetadata(changed.getBytes(StandardCharsets.UTF_8),"metadata approval differs"));
            assertThrows(IOException.class,()->expected.encode(v2Data(configuration)));
            byte[] changedAnchor=B1SourceAnchorCodec.encodeV2(v2Data(configuration));
            assertThrows(IOException.class,()->expected.decode(changedAnchor));
            assertThrows(IOException.class,()->expected.namespace(configuration));
        }
    }
    @Test void storeExplicitV2AndLegacySelectorsCannotCrossFallback() throws Exception {
        String properties=fullV2Properties();var v2=WindowsB1SourceAnchorStore.AnchorConfiguration.ofMetadataV2(fullV2(properties));
        var legacy=WindowsB1SourceAnchorStore.AnchorConfiguration.ofMetadata(B1SourceAnchorFixture.metadata(temporary));
        var data=B1SourceAnchorFixture.data(temporary);byte[] old=legacy.encode(data);
        assertArrayEquals(B1SourceAnchorCodec.encode(data),old);
        assertArrayEquals(old,legacy.encode(legacy.decode(old)));
        assertEquals(B1SourceAnchorCodec.canonicalConfigurationNamespace(data.configuration()),legacy.namespace(data.configuration()));
        legacy.requireMetadata(B1SourceAnchorFixture.properties(temporary).getBytes(StandardCharsets.UTF_8),"legacy metadata differs");
        byte[] current=v2.encode(v2Data(B1SourceAnchorCodec.configurationBytesV2(fullV2(properties))));
        assertThrows(IOException.class,()->legacy.decode(current));assertThrows(IOException.class,()->v2.decode(old));
        assertThrows(IOException.class,()->legacy.encode(v2Data(B1SourceAnchorCodec.configurationBytesV2(fullV2(properties)))));
        assertThrows(IOException.class,()->v2.encode(data));
        assertThrows(IllegalArgumentException.class,()->legacy.requireMetadata(properties.getBytes(StandardCharsets.UTF_8),"no legacy fallback"));
        assertThrows(IllegalArgumentException.class,()->v2.requireMetadata(B1SourceAnchorFixture.properties(temporary).getBytes(StandardCharsets.UTF_8),"no V2 fallback"));
        var oldValues=new Properties();oldValues.load(new StringReader(properties));
        oldValues.remove("format3ManifestConfigurationEncoding");var writer=new StringWriter();oldValues.store(writer,null);
        String old3Properties=writer.toString();var old3Metadata=MySqlRuntimeMetadata.fromBytes(old3Properties.getBytes(StandardCharsets.UTF_8));
        var old3=WindowsB1SourceAnchorStore.AnchorConfiguration.ofMetadata(old3Metadata);
        byte[] old3Configuration=B1SourceAnchorCodec.configurationBytes(old3Metadata);
        byte[] old3Anchor=old3.encode(v2Data(old3Configuration));
        old3.requireMetadata(old3Properties.getBytes(StandardCharsets.UTF_8),"legacy3 metadata differs");
        assertArrayEquals(old3Anchor,old3.encode(old3.decode(old3Anchor)));
        assertEquals(B1SourceAnchorCodec.canonicalConfigurationNamespace(old3Configuration),old3.namespace(old3Configuration));
        assertThrows(IOException.class,()->v2.decode(old3Anchor));assertThrows(IOException.class,()->old3.decode(current));
        assertThrows(IOException.class,()->v2.encode(v2Data(old3Configuration)));
        assertThrows(IOException.class,()->old3.encode(v2Data(B1SourceAnchorCodec.configurationBytesV2(fullV2(properties)))));

    }

}
