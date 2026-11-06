package com.wi.floatingpod.server;

import com.wi.floatingpod.config.MySqlRuntimeMetadata;
import com.wi.floatingpod.storage.mysql.MySqlSessionBarrierNamespace;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.TreeMap;
import static org.junit.jupiter.api.Assertions.*;

/** 仅 DATA/codec；不调用 NativeIO，不构造 private native proof。 */
class B1DeploymentReceiptSourceAnchorCodecTest {
    @TempDir Path temporary;
    private String properties() {
        return B1SourceAnchorFixture.properties(temporary).replace("approvedSchemaFormat=2", "approvedSchemaFormat=5")
            .replace("format2:all-nine-fixed-schema-tables-and-static-objects;all-eighteen-fixed-schema-triggers;required-auditself-only;other-routine-absence-unclaimed",
                "script-publication1-deployment-receipt1:exact26-tables-exact78-guards-auditself-and-privileges")
            + "deploymentReceiptRuntimeContractPath=" + temporary.resolve("contract.json").toAbsolutePath().toString().replace('\\','/') + "\n"
            + "deploymentReceiptRuntimeContractSha256=" + "d".repeat(64) + "\n"
            + "deploymentReceiptManifestConfigurationEncoding=FP_MYSQL_DEPLOYMENT_RECEIPT1_MANIFEST_CONFIGURATION_BE_V1\n";
    }
    private MySqlRuntimeMetadata.DeploymentReceiptStatic metadata(String text) throws IOException {
        return MySqlRuntimeMetadata.fromDeploymentReceiptBytes(text.getBytes(StandardCharsets.UTF_8));
    }
    private WindowsWorldProvisionReceiptDataCodec.PlanData plan() {
        var p=B1SourceAnchorFixture.plan();
        return new WindowsWorldProvisionReceiptDataCodec.PlanData(p.creationOperationUuid(),p.bootstrapOperationUuid(),
            p.prepareWriteUuid(),p.certifyWriteUuid(),p.bindingUuid(),2,p.mysqlServerUuid(),p.schema(),5,p.ddlSha256(),p.provisioningUuid());
    }
    private B1SourceAnchorCodec.Data data() throws IOException {
        var receipt=new WindowsWorldProvisionReceiptDataCodec.ReceiptData(plan(),B1SourceAnchorFixture.identity(1),
            B1SourceAnchorFixture.identity(2),1234,B1SourceAnchorFixture.FILETIME,B1SourceAnchorFixture.receiptData().creationBoot());
        return new B1SourceAnchorCodec.Data(B1DeploymentReceiptSourceAnchorCodec.configurationBytes(metadata(properties())),
            WindowsWorldProvisionReceiptDataCodec.encode(receipt),B1SourceAnchorFixture.identity(3),B1SourceAnchorFixture.identity(4),
            B1SourceAnchorFixture.identity(5),B1SourceAnchorFixture.PARENT,B1SourceAnchorFixture.WORLD,"runtime.properties");
    }
    private static byte[] hash(byte[] bytes) throws Exception { return MessageDigest.getInstance("SHA-256").digest(bytes); }
    private static String escape(String value) {
        return value.replace("\\","\\\\").replace("\n","\\n").replace("\r","\\r").replace("\t","\\t")
            .replace("\f","\\f").replace(" ","\\ ");
    }
    @Test void completeAUsesSortedExplicitFamilyAndCommitsAllRawPins() throws Exception {
        var m=metadata(properties());
        var expected=new TreeMap<String,String>(m.completeCanonicalFields());
        expected.put("deploymentReceiptConfigurationEncoding","FP_B1_CONFIGURATION_DEPLOYMENT_RECEIPT_V1");
        var text=new StringBuilder();expected.forEach((k,v)->text.append(k).append('=').append(escape(v)).append('\n'));
        byte[] actual=B1DeploymentReceiptSourceAnchorCodec.configurationBytes(m);
        assertArrayEquals(text.toString().getBytes(StandardCharsets.UTF_8),actual);
        assertEquals(expected.size(),new String(actual,StandardCharsets.UTF_8).lines().count());
        assertEquals(m.completeCanonicalFields(),B1DeploymentReceiptSourceAnchorCodec.configurationMetadata(actual).completeCanonicalFields());
        for (String key:new String[]{"approvedBaselineSha256","approvedSourceCaptureSha256","deploymentReceiptRuntimeContractSha256"}) {
            String original=m.completeCanonicalFields().get(key);
            var changed=metadata(properties().replace(key+"="+original.toLowerCase(java.util.Locale.ROOT),key+"="+"e".repeat(64)));
            assertFalse(Arrays.equals(actual,B1DeploymentReceiptSourceAnchorCodec.configurationBytes(changed)),key);
        }
        assertFalse(Files.exists(temporary.resolve("credential.txt")));
        assertFalse(Files.exists(temporary.resolve("contract.json")));
    }
    private static void blob(DataOutputStream out,byte[] bytes) throws IOException { out.writeInt(bytes.length);out.write(bytes); }
    private static void text(DataOutputStream out,String text) throws IOException { blob(out,text.getBytes(StandardCharsets.UTF_8)); }
    @Test void independentV6EnvelopeMatchesEveryFieldDigestAndFixedSlot() throws Exception {
        var d=data();var body=new ByteArrayOutputStream();
        try(var out=new DataOutputStream(body)) {
            text(out,"FP_B1_SOURCE_ANCHOR_BINARY_BE_V6");blob(out,d.configuration());out.write(hash(d.configuration()));
            blob(out,d.receipt());out.write(hash(d.receipt()));text(out,d.receiptFileIdentity());text(out,d.configParentIdentity());
            text(out,d.metadataFileIdentity());text(out,d.configParentNativePath());text(out,d.worldNativePath());text(out,d.metadataFilename());
            var p=plan();text(out,MySqlSessionBarrierNamespace.forIdentity(p.mysqlServerUuid(),p.schema(),p.provisioningUuid(),p.bindingUuid()).lockName());
            text(out,"FP_B1_RESUME_BINARY_BE_V6");text(out,".floatingpod-b1-resume-v6.0");
            text(out,".floatingpod-b1-resume-v6.1");text(out,".floatingpod-b1-resume-v6.tmp");
        }
        var expected=new ByteArrayOutputStream();
        try(var out=new DataOutputStream(expected)) {
            out.writeLong(0x46504231414e4336L);out.writeInt(6);out.writeInt(body.size());out.write(body.toByteArray());out.write(hash(body.toByteArray()));
        }
        byte[] bytes=B1DeploymentReceiptSourceAnchorCodec.encode(d);
        assertArrayEquals(expected.toByteArray(),bytes);
        var decoded=B1DeploymentReceiptSourceAnchorCodec.decode(bytes);
        assertArrayEquals(bytes,B1DeploymentReceiptSourceAnchorCodec.encode(decoded));
        assertArrayEquals(d.receipt(),decoded.receipt());assertArrayEquals(d.configuration(),decoded.configuration());
        assertEquals(plan(),WindowsWorldProvisionReceiptDataCodec.decode(decoded.receipt()).plan());
    }
    private static void rehash(byte[] bytes) throws Exception {
        int size=ByteBuffer.wrap(bytes).getInt(12);
        System.arraycopy(hash(Arrays.copyOfRange(bytes,16,16+size)),0,bytes,bytes.length-32,32);
    }
    private static int index(byte[] bytes,String text) {
        byte[] target=text.getBytes(StandardCharsets.UTF_8);
        outer:for(int i=0;i<=bytes.length-target.length;i++) {
            for(int j=0;j<target.length;j++) if(bytes[i+j]!=target[j])continue outer;
            return i;
        }
        throw new AssertionError("Missing independent fixture field");
    }
    @Test void strictBoundsUtf8EofDigestsAndFixedNamespaceCannotBeBypassed() throws Exception {
        byte[] good=B1DeploymentReceiptSourceAnchorCodec.encode(data());
        for(byte[] bad:new byte[][]{new byte[65537],Arrays.copyOf(good,good.length-1),Arrays.copyOf(good,good.length+1)})
            assertThrows(IOException.class,()->B1DeploymentReceiptSourceAnchorCodec.decode(bad));
        byte[] digest=good.clone();digest[50]^=1;
        assertThrows(IOException.class,()->B1DeploymentReceiptSourceAnchorCodec.decode(digest));
        byte[] utf=good.clone();utf[20]=(byte)0xc0;rehash(utf);
        assertInstanceOf(CharacterCodingException.class,assertThrows(IOException.class,()->B1DeploymentReceiptSourceAnchorCodec.decode(utf)));
        for(String field:new String[]{"FP_B1_RESUME_BINARY_BE_V6",".floatingpod-b1-resume-v6.0",".floatingpod-b1-resume-v6.1",".floatingpod-b1-resume-v6.tmp"}) {
            byte[] bad=good.clone();bad[index(bad,field)+field.length()-1]='9';rehash(bad);
            assertThrows(IOException.class,()->B1DeploymentReceiptSourceAnchorCodec.decode(bad));
        }
        byte[] unknown=good.clone();ByteBuffer.wrap(unknown).putInt(8,5);
        assertThrows(IOException.class,()->B1DeploymentReceiptSourceAnchorCodec.decode(unknown));
    }
    @Test void explicitSelectorsRejectOldAnchorFamiliesAndWrongMetadataOrReceipt() throws Exception {
        byte[] old=B1SourceAnchorCodec.encode(B1SourceAnchorFixture.data(temporary));
        byte[] current=B1DeploymentReceiptSourceAnchorCodec.encode(data());
        assertThrows(IOException.class,()->B1DeploymentReceiptSourceAnchorCodec.decode(old));
        assertThrows(IOException.class,()->B1SourceAnchorCodec.decode(current));
        assertThrows(IOException.class,()->B1SourceAnchorCodec.decodeV2(current));
        var selected=WindowsB1SourceAnchorStore.AnchorConfiguration.ofDeploymentReceiptMetadata(metadata(properties()));
        assertArrayEquals(current,selected.encode(data()));
        assertArrayEquals(current,selected.encode(selected.decode(current)));
        assertThrows(IOException.class,()->selected.decode(old));
        assertThrows(IllegalArgumentException.class,()->selected.requireMetadata(B1SourceAnchorFixture.properties(temporary).getBytes(StandardCharsets.UTF_8),"wrong family"));
        var d=data();var changed=metadata(properties().replace("approvedDdlSha256="+plan().ddlSha256(),"approvedDdlSha256="+"EF".repeat(32)));
        var mismatch=new B1SourceAnchorCodec.Data(B1DeploymentReceiptSourceAnchorCodec.configurationBytes(changed),d.receipt(),
            d.receiptFileIdentity(),d.configParentIdentity(),d.metadataFileIdentity(),d.configParentNativePath(),d.worldNativePath(),d.metadataFilename());
        assertThrows(IOException.class,()->B1DeploymentReceiptSourceAnchorCodec.encode(mismatch));
        var legacyReceipt=new B1SourceAnchorCodec.Data(d.configuration(),B1SourceAnchorFixture.data(temporary).receipt(),
            d.receiptFileIdentity(),d.configParentIdentity(),d.metadataFileIdentity(),d.configParentNativePath(),d.worldNativePath(),d.metadataFilename());
        assertThrows(IOException.class,()->B1DeploymentReceiptSourceAnchorCodec.encode(legacyReceipt));
    }
    @Test void completeARejectsDuplicateNoncanonicalUnknownAndWrongFamilyMarkers() throws Exception {
        byte[] good=B1DeploymentReceiptSourceAnchorCodec.configurationBytes(metadata(properties()));
        String text=new String(good,StandardCharsets.UTF_8);
        for(String bad:new String[]{text+"backend=mysql\n",text+"unknown=1\n",text.replace("FP_B1_CONFIGURATION_DEPLOYMENT_RECEIPT_V1","FP_B1_CONFIGURATION_FORMAT3_V1"),text.replace("\n","\r\n")})
            assertThrows(IOException.class,()->B1DeploymentReceiptSourceAnchorCodec.configurationMetadata(bad.getBytes(StandardCharsets.UTF_8)));
        assertThrows(IOException.class,()->B1DeploymentReceiptSourceAnchorCodec.configurationMetadata(new byte[65537]));
        byte[] utf=good.clone();utf[0]=(byte)0xc0;
        assertThrows(CharacterCodingException.class,()->B1DeploymentReceiptSourceAnchorCodec.configurationMetadata(utf));
    }
}
