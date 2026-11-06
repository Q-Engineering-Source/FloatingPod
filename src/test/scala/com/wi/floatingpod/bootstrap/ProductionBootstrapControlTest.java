package com.wi.floatingpod.bootstrap;

import com.wi.floatingpod.config.MySqlRuntimeMetadata;
import com.wi.floatingpod.config.SourceLimitProfile;
import com.wi.floatingpod.server.WindowsB1SourceAnchorStore;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.charset.CharacterCodingException;
import java.nio.file.NoSuchFileException;
import java.util.Properties;
import java.util.HexFormat;
import java.util.Arrays;


import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

/** 真实有界读取/parser/原始hash/身份消费行为；不制造 SQL/native qualification。 */
class ProductionBootstrapControlTest {
    @TempDir Path temporary;
    private record Fixture(Path control, Path metadata, Path baseline, Path world, LinkedHashMap<String,String> values) {
        ProductionBootstrapControl.LaunchContract contract() throws Exception {
            return new ProductionBootstrapControl.LaunchContract(control,ProductionBootstrapControl.digest(Files.readAllBytes(control)));
        }
    }
    private static String text(LinkedHashMap<String,String> values) {
        var out=new StringBuilder();values.forEach((key,value)->out.append(key).append('=').append(value).append('\n'));return out.toString();
    }
    private Fixture fixture() throws Exception {
        String suffix=UUID.randomUUID().toString();Path dir=Files.createDirectory(temporary.resolve(suffix));
        Path baseline=dir.resolve("approved.bin"),metadata=dir.resolve("metadata.properties"),control=dir.resolve("control.properties"),world=dir.resolve("absent-world");
        // The carrier checks approved bytes/hash, not semantic SQL baseline acceptance; product loader remains mandatory.
        Files.writeString(baseline,"static carrier byte fixture only",StandardCharsets.UTF_8);
        String bsha=ProductionBootstrapControl.digest(Files.readAllBytes(baseline));
        UUID server=UUID.randomUUID(),binding=UUID.randomUUID(),prov=UUID.randomUUID();String path=dir.toAbsolutePath().toString().replace("\\","/");
        String coverage="format2:all-nine-fixed-schema-tables-and-static-objects;all-eighteen-fixed-schema-triggers;required-auditself-only;other-routine-absence-unclaimed";
        String md="backend=mysql\nhost=fixture.example.invalid\nport=3307\nschema=fp_carrier\nexpectedServerUuid="+server+"\nexpectedBindingUuid="+binding+
            "\nexpectedEffectiveUser=runner@localhost\nusername=runner\npkcs12TrustStorePath="+path+"/ABSENT-trust.p12\ncredentialSourceRef=file:"+path+
            "/ABSENT-credential.secret\ntrustSecretSourceRef=file:"+path+"/ABSENT-trust.secret\nconnectTimeoutMillis=3000\nsocketTimeoutMillis=10000\nactiveLimit=256\ninnodbFlushLogAtTrxCommit=1\nsyncBinlog=1\napprovedBaselinePath="+
            baseline.toString().replace("\\","/")+"\napprovedBaselineSha256="+bsha+"\napprovedBaselineFormat=FP_MYSQL_APPROVED_STATIC_V1\napprovedSchemaFormat=2\napprovedCoverage="+coverage+
            "\nexpectedServerVersion=9.7.2\nexpectedProvisioningUuid="+prov+"\napprovedDdlSha256="+"B".repeat(64)+"\nexpectedAuditDefiner=audit@localhost\napprovedSourceCaptureSha256="+"C".repeat(64)+"\n";
        Files.writeString(metadata,md,StandardCharsets.UTF_8);
        var v=new LinkedHashMap<String,String>();
        v.put("controlFormat","FP_PRODUCTION_BOOTSTRAP_CONTROL_V1");v.put("mode","FRESH");v.put("metadataPath",metadata.toString().replace("\\","/"));v.put("metadataSha256",ProductionBootstrapControl.digest(Files.readAllBytes(metadata)));
        v.put("configurationSha256","E".repeat(64));v.put("worldPath",world.toString().replace("\\","/"));
        v.put("host","fixture.example.invalid");v.put("port","3307");v.put("schema","fp_carrier");v.put("effectiveUser","runner@localhost");v.put("username","runner");
        v.put("serverUuid",server.toString());v.put("bindingUuid",binding.toString());v.put("activeLimit","256");v.put("connectTimeoutMillis","3000");v.put("socketTimeoutMillis","10000");
        v.put("baselinePath",baseline.toString().replace("\\","/"));v.put("baselineSha256",bsha);v.put("baselineFormat","FP_MYSQL_APPROVED_STATIC_V1");v.put("schemaFormat","2");
        v.put("coverage",coverage);v.put("serverVersion","9.7.2");v.put("provisioningUuid",prov.toString());v.put("ddlSha256","B".repeat(64));v.put("auditDefiner","audit@localhost");v.put("sourceCaptureSha256","C".repeat(64));
        for(String key:new String[]{"bootUuid","submitOperationUuid","creationUuid","bootstrapUuid","prepareUuid","certifyUuid"})v.put(key,UUID.randomUUID().toString());
        v.put("bootGeneration","1");v.put("connectionGeneration","1");v.put("callerRoles","NONE");v.put("auditDefinerRoles","NONE");v.put("importState","NONE");v.put("physicalFormat","2");
        v.put("receiptFreshnessMillis","30000");v.put("leaseMillis","120000");v.put("preparationMillis","45000");v.put("claimMillis","10000");v.put("finalizationMillis","10000");
        v.put("sqlOuterSha256",EarlyDependencyResolver.SQL_OUTER);v.put("scalarOuterSha256","1".repeat(64));v.put("scalarRuntimeVersion","3.8.4");v.put("scalaLibrarySha256","2".repeat(64));v.put("scalaParserSha256","3".repeat(64));v.put("fugueOuterSha256","4".repeat(64));v.put("fugueRuntimeVersion","0.99.1");
        Files.writeString(control,text(v),StandardCharsets.UTF_8);return new Fixture(control,metadata,baseline,world,v);
    }
    private ProductionBootstrapControl replace(Fixture f,String key,String value)throws Exception {
        f.values.put(key,value);Files.writeString(f.control,text(f.values),StandardCharsets.UTF_8);return ProductionBootstrapControl.load(f.contract());
    }
    @Test void actualPinnedCarrierPreservesIndependentIdentityAndDoesNotReadSecretReferences()throws Exception {
        Fixture f=fixture();var c=ProductionBootstrapControl.load(f.contract());
        assertEquals(f.world.toAbsolutePath(),c.worldPath());assertEquals(f.metadata.toAbsolutePath(),c.metadataPath());
        assertEquals(c.metadata().baseline().fileSha256(),f.values.get("baselineSha256"));assertEquals("NONE",c.policy().callerRoles());
        assertEquals(120000,c.policy().writerLease().toMillis());assertEquals(EarlyDependencyResolver.SQL_OUTER,c.dependencyPins().sqlOuterSha256());
        c.requireFreshWorld(f.world);assertFalse(Files.exists(c.metadata().credentialSourceRef().absolutePath()));assertFalse(Files.exists(c.metadata().trustSecretSourceRef().absolutePath()));
        assertFalse(c.toString().contains("fixture.example.invalid"));
    }
    @Test void callerMustSupplyAbsoluteUppercaseIndependentPinAndWrongPinFails()throws Exception {
        Fixture f=fixture();assertThrows(IllegalArgumentException.class,()->new ProductionBootstrapControl.LaunchContract(Path.of("relative"),"A".repeat(64)));
        assertThrows(IllegalArgumentException.class,()->new ProductionBootstrapControl.LaunchContract(f.control,"a".repeat(64)));
        assertThrows(IllegalArgumentException.class,()->ProductionBootstrapControl.load(new ProductionBootstrapControl.LaunchContract(f.control,"0".repeat(64))));
    }
    @Test void missingDuplicateUnknownAndUnsetAreRealParserRejections()throws Exception {
        for(String added:new String[]{"host=other.invalid\n","password=NON_SECRET_SENTINEL\n"}){
            Fixture f=fixture();Files.writeString(f.control,text(f.values)+added);assertThrows(IllegalArgumentException.class,()->ProductionBootstrapControl.load(f.contract()));
        }
        Fixture missing=fixture();missing.values.remove("submitOperationUuid");Files.writeString(missing.control,text(missing.values));assertThrows(IllegalArgumentException.class,()->ProductionBootstrapControl.load(missing.contract()));
        Fixture unset=fixture();assertThrows(IllegalArgumentException.class,()->replace(unset,"configurationSha256","UNSET"));
    }
    @Test void actualMetadataAndPackMutationFailBeforeAnySourceAcquisition()throws Exception {
        Fixture a=fixture();var contract=a.contract();Files.writeString(a.metadata,"changed");assertThrows(IllegalArgumentException.class,()->ProductionBootstrapControl.load(contract));
        Fixture b=fixture();var pinned=b.contract();Files.writeString(b.baseline,"changed");assertThrows(IllegalArgumentException.class,()->ProductionBootstrapControl.load(pinned));
        Fixture c=fixture();assertThrows(IllegalArgumentException.class,()->replace(c,"bindingUuid",UUID.randomUUID().toString()));
    }
    @Test void ordinaryModeIsTypedUnavailableWithoutMetadataOrFreshMint()throws Exception {
        Path control=temporary.resolve("ordinary.properties");Files.writeString(control,"controlFormat=FP_PRODUCTION_BOOTSTRAP_CONTROL_V1\nmode=ORDINARY_CERTIFIED\n");
        var contract=new ProductionBootstrapControl.LaunchContract(control,ProductionBootstrapControl.digest(Files.readAllBytes(control)));
        assertThrows(ProductionBootstrapControl.OrdinaryRecoveryUnavailable.class,()->ProductionBootstrapControl.load(contract));
    }
    @Test void exactWorldAndAbsentDirectoryAreRequiredAndIdentityCannotBeRewrappedForSecondBoot()throws Exception {
        Fixture f=fixture();var c=ProductionBootstrapControl.load(f.contract());
        assertThrows(IllegalArgumentException.class,()->c.requireFreshWorld(f.world.resolveSibling("another-world")));
        Files.createDirectory(f.world);assertThrows(IllegalArgumentException.class,()->c.requireFreshWorld(f.world));
        // Static control consumption has no physical proof/SQL success; same identity replay is still prohibited.
        c.consumeFreshIdentityOnce();assertThrows(IllegalArgumentException.class,c::consumeFreshIdentityOnce);
        var again=ProductionBootstrapControl.load(f.contract());assertThrows(IllegalArgumentException.class,again::consumeFreshIdentityOnce);
    }
    @Test void malformedUtf8BomOversizeAndMissingFileKeepOriginalFailureBoundary()throws Exception {
        Path utf=temporary.resolve("invalid-utf8.properties");byte[] malformed={(byte)0xC3,0x28};Files.write(utf,malformed);
        assertThrows(CharacterCodingException.class,()->ProductionBootstrapControl.load(new ProductionBootstrapControl.LaunchContract(utf,ProductionBootstrapControl.digest(malformed))));
        Path bom=temporary.resolve("bom.properties");byte[] bomBytes="\uFEFFmode=FRESH\n".getBytes(StandardCharsets.UTF_8);Files.write(bom,bomBytes);
        assertThrows(IllegalArgumentException.class,()->ProductionBootstrapControl.load(new ProductionBootstrapControl.LaunchContract(bom,ProductionBootstrapControl.digest(bomBytes))));
        Path big=temporary.resolve("oversize.properties");byte[] oversized=new byte[65537];Files.write(big,oversized);
        assertThrows(IOException.class,()->ProductionBootstrapControl.load(new ProductionBootstrapControl.LaunchContract(big,ProductionBootstrapControl.digest(oversized))));
        assertThrows(IOException.class,()->ProductionBootstrapControl.load(new ProductionBootstrapControl.LaunchContract(temporary.resolve("missing"),"A".repeat(64))));
    }
    @Test void crossProfileImportTimeoutAndNonpositiveGenerationNeverFallback()throws Exception {
        for(String[] pair:new String[][]{{"schemaFormat","1"},{"physicalFormat","1"},{"importState","IMPORTED"},{"bootGeneration","0"},{"leaseMillis","1800001"},{"scalarRuntimeVersion","3.8.3"}}){
            Fixture f=fixture();assertThrows(IllegalArgumentException.class,()->replace(f,pair[0],pair[1]));
        }
    }
    private static final SourceLimitProfile V3_PROFILE=
        SourceLimitProfile.of(1024,2048,4096,32,100,8L*1024*1024,8192,16384);
    private Fixture deploymentFixture(boolean ordinary)throws Exception {
        Fixture f=fixture();var v=f.values;
        Path k=f.metadata.resolveSibling("schema5-contract.json");Files.writeString(k,"static K bytes; semantic admission remains late");
        String md=Files.readString(f.metadata).replace("approvedSchemaFormat=2","approvedSchemaFormat=5")
            .replace(v.get("coverage"),"script-publication1-deployment-receipt1:exact26-tables-exact78-guards-auditself-and-privileges")
            +"deploymentReceiptRuntimeContractPath="+k.toString().replace("\\","/")+"\n"
            +"deploymentReceiptRuntimeContractSha256="+ProductionBootstrapControl.digest(Files.readAllBytes(k))+"\n"
            +"deploymentReceiptManifestConfigurationEncoding="+MySqlRuntimeMetadata.DEPLOYMENT_RECEIPT_MANIFEST+"\n";
        Files.writeString(f.metadata,md);
        var metadata=MySqlRuntimeMetadata.fromDeploymentReceiptBytes(Files.readAllBytes(f.metadata));
        v.put("controlFormat",ProductionBootstrapControl.DEPLOYMENT_RECEIPT_FORMAT);v.put("schemaFormat","5");v.put("coverage",metadata.baseline().coverage());
        v.put("metadataSha256",ProductionBootstrapControl.digest(Files.readAllBytes(f.metadata)));
        v.put("configurationSha256",WindowsB1SourceAnchorStore.AnchorConfiguration.ofDeploymentReceiptMetadata(metadata).configurationSha256());
        v.put("runtimeContractSha256",metadata.runtimeContract().fileSha256());
        v.put("sourceLimitProfileCanonicalHex",HexFormat.of().formatHex(V3_PROFILE.canonicalBytes()));
        v.put("sourceLimitProfileSha256",HexFormat.of().withUpperCase().formatHex(V3_PROFILE.sha256()));
        if(ordinary){
            var a=new LinkedHashMap<String,String>();a.put("approvalFormat","FP_DEPLOYMENT_RECEIPT1_ORIGINAL_SOURCE_PINS_V1");
            for(String key:new String[]{"creationUuid","bootstrapUuid","prepareUuid","certifyUuid","bindingUuid","physicalFormat","serverUuid","schema","schemaFormat","ddlSha256","provisioningUuid","worldPath","configurationSha256","metadataSha256","baselineSha256","runtimeContractSha256"})a.put(key,v.get(key));
            a.put("creationBootUuid",v.get("bootUuid"));a.put("creationBootGeneration",v.get("bootGeneration"));
            a.put("canonicalReceiptLength","500");a.put("canonicalReceiptSha256","6".repeat(64));
            for(String key:new String[]{"sourceWorldIdentity","guardFileIdentity","receiptFileIdentity","anchorFileIdentity"})a.put(key,"FP_WIN32_FILE_ID_V1;volume=0000000000000001;id="+String.format("%032x",a.size()));
            a.put("anchorPath",f.metadata.resolveSibling("metadata.properties.b1-source-v6").toString().replace("\\","/"));a.put("anchorSha256","7".repeat(64));
            Path approval=f.metadata.resolveSibling("original-pins.properties");Files.writeString(approval,text(a));
            for(String key:new String[]{"creationUuid","bootstrapUuid","prepareUuid","certifyUuid"})v.remove(key);
            for(String key:new String[]{"bootUuid","submitOperationUuid","handoffAttemptUuid","handoffOperationUuid"})v.put(key,UUID.randomUUID().toString());
            v.put("mode","ORDINARY_CERTIFIED");v.put("ordinarySourceApprovalPath",approval.toString().replace("\\","/"));v.put("ordinarySourceApprovalSha256",ProductionBootstrapControl.digest(Files.readAllBytes(approval)));
        }
        Files.writeString(f.control,text(v));return f;
    }
    private Fixture dbFirstFixture()throws Exception {
        Fixture f=deploymentFixture(false);var v=f.values;
        String md=Files.readString(f.metadata).replace("approvedSchemaFormat=5","approvedSchemaFormat=6")
            .replace(v.get("coverage"),MySqlRuntimeMetadata.DB_FIRST_COVERAGE)
            .replace("deploymentReceiptRuntimeContract","dbFirstRuntimeContract")
            .replace("deploymentReceiptManifestConfigurationEncoding","dbFirstManifestConfigurationEncoding")
            .replace(MySqlRuntimeMetadata.DEPLOYMENT_RECEIPT_MANIFEST,MySqlRuntimeMetadata.DB_FIRST_MANIFEST);
        Files.writeString(f.metadata,md);
        v.put("controlFormat",ProductionBootstrapControl.DB_FIRST_FORMAT);v.put("schemaFormat","6");
        v.put("coverage",MySqlRuntimeMetadata.DB_FIRST_COVERAGE);
        v.put("metadataSha256",ProductionBootstrapControl.digest(Files.readAllBytes(f.metadata)));
        for(String key:new String[]{"mode","submitOperationUuid","creationUuid","bootstrapUuid","prepareUuid","certifyUuid"})v.remove(key);
        v.put("observeOperationUuid",UUID.randomUUID().toString());v.put("acquireOperationUuid",UUID.randomUUID().toString());
        v.put("barrierWaitSeconds","0");Files.writeString(f.control,text(v));return f;
    }
    @Test void dbFirstStaticControlPreservesExplicitPolicyPinsAndSeparateOperations()throws Exception {
        Fixture f=dbFirstFixture();var c=ProductionBootstrapControl.loadDbFirst(f.contract());
        assertEquals(6,c.metadata().baseline().schemaFormat());assertEquals(V3_PROFILE,c.sourceLimitProfile());
        assertEquals(java.time.Duration.ZERO,c.barrierWait());assertEquals(f.values.get("configurationSha256"),c.expectedConfigurationSha256());
        assertNotEquals(c.identity().observeOperation(),c.identity().acquireOperation());
        assertEquals(Long.parseLong(f.values.get("leaseMillis")),c.policy().writerLease().toMillis());
        assertEquals(f.world.toAbsolutePath(),c.worldPath());
        assertFalse(Files.exists(Path.of(c.metadata().completeCanonicalFields().get("credentialSourceRef").substring(5))));
        c.consumeIdentityOnce();assertThrows(IllegalArgumentException.class,c::consumeIdentityOnce);
        var reload=ProductionBootstrapControl.loadDbFirst(f.contract());assertThrows(IllegalArgumentException.class,reload::consumeIdentityOnce);
        // discovery仅选择实际schema6入口；完整load、依赖核验和生命周期仍各自承担验证。
        assertEquals("DB_FIRST",ProductionBootstrapControl.discoverProductionFormat(f.contract()).name());
        Fixture max=dbFirstFixture();max.values.put("barrierWaitSeconds","30");Files.writeString(max.control,text(max.values));
        assertEquals(java.time.Duration.ofSeconds(30),ProductionBootstrapControl.loadDbFirst(max.contract()).barrierWait());
    }
    @Test void dbFirstDiscoveryRequiresExactPinnedSelectorButCannotApproveIncompleteControl()throws Exception {
        Fixture missing=dbFirstFixture();missing.values.remove("barrierWaitSeconds");Files.writeString(missing.control,text(missing.values));
        assertEquals("DB_FIRST",ProductionBootstrapControl.discoverProductionFormat(missing.contract()).name());
        assertThrows(IllegalArgumentException.class,()->ProductionBootstrapControl.loadDbFirst(missing.contract()));
        Fixture alias=dbFirstFixture();alias.values.put("controlFormat",ProductionBootstrapControl.DB_FIRST_FORMAT.toLowerCase(java.util.Locale.ROOT));
        Files.writeString(alias.control,text(alias.values));
        assertThrows(IllegalArgumentException.class,()->ProductionBootstrapControl.discoverProductionFormat(alias.contract()));
        Fixture duplicate=dbFirstFixture();Files.writeString(duplicate.control,text(duplicate.values)+"controlFormat="+ProductionBootstrapControl.DB_FIRST_FORMAT+"\n");
        assertThrows(IllegalArgumentException.class,()->ProductionBootstrapControl.discoverProductionFormat(duplicate.contract()));
        Fixture changed=dbFirstFixture();var original=changed.contract();Files.writeString(changed.control,text(changed.values)+"\n");
        assertThrows(IllegalArgumentException.class,()->ProductionBootstrapControl.discoverProductionFormat(original));
    }
    @Test void dbFirstRejectsMissingWaitForeignKeysIdentityReuseAndChangedPinnedFiles()throws Exception {
        for(String value:new String[]{"-1","31","0.5","","00","+1"}){
            Fixture f=dbFirstFixture();f.values.put("barrierWaitSeconds",value);Files.writeString(f.control,text(f.values));
            assertThrows(IllegalArgumentException.class,()->ProductionBootstrapControl.loadDbFirst(f.contract()),value);
        }
        Fixture missing=dbFirstFixture();missing.values.remove("barrierWaitSeconds");Files.writeString(missing.control,text(missing.values));
        assertThrows(IllegalArgumentException.class,()->ProductionBootstrapControl.loadDbFirst(missing.contract()));
        Fixture extra=dbFirstFixture();extra.values.put("mode","FRESH");Files.writeString(extra.control,text(extra.values));
        assertThrows(IllegalArgumentException.class,()->ProductionBootstrapControl.loadDbFirst(extra.contract()));
        Fixture same=dbFirstFixture();same.values.put("acquireOperationUuid",same.values.get("observeOperationUuid"));Files.writeString(same.control,text(same.values));
        assertThrows(IllegalArgumentException.class,()->ProductionBootstrapControl.loadDbFirst(same.contract()));
        Fixture zero=dbFirstFixture();zero.values.put("bootUuid",new UUID(0,0).toString());Files.writeString(zero.control,text(zero.values));
        assertThrows(IllegalArgumentException.class,()->ProductionBootstrapControl.loadDbFirst(zero.contract()));
        Fixture b=dbFirstFixture();Files.writeString(b.baseline,"changed");assertThrows(IllegalArgumentException.class,()->ProductionBootstrapControl.loadDbFirst(b.contract()));
        Fixture k=dbFirstFixture();Files.writeString(k.metadata.resolveSibling("schema5-contract.json"),"changed");assertThrows(IllegalArgumentException.class,()->ProductionBootstrapControl.loadDbFirst(k.contract()));
        Fixture raw=dbFirstFixture();var original=raw.contract();Files.writeString(raw.control,text(raw.values)+"barrierWaitSeconds=1\n");
        assertThrows(IllegalArgumentException.class,()->ProductionBootstrapControl.loadDbFirst(original));
        assertThrows(IllegalArgumentException.class,()->ProductionBootstrapControl.loadDbFirst(raw.contract()));
    }
    @Test void deploymentReceiptFreshUsesRealStaticParserAndPreservesPinsWithoutSecrets()throws Exception {
        Fixture f=deploymentFixture(false);var c=ProductionBootstrapControl.loadDeploymentReceipt(f.contract());
        assertEquals(ProductionBootstrapControl.ProductionFormat.DEPLOYMENT_RECEIPT,ProductionBootstrapControl.discoverProductionFormat(f.contract()));
        assertEquals(5,c.metadata().baseline().schemaFormat());assertEquals(V3_PROFILE,c.sourceLimitProfile());
        assertEquals(f.values.get("runtimeContractSha256"),c.runtimeContractSha256());assertNull(c.originalSourcePins());
        assertEquals(EarlyDependencyResolver.SQL_OUTER,c.dependencyPins().sqlOuterSha256());c.requireFreshWorld(f.world);
        assertFalse(Files.exists(Path.of(c.metadata().completeCanonicalFields().get("credentialSourceRef").substring(5))));
        c.consumeFreshIdentityOnce();assertThrows(IllegalArgumentException.class,c::consumeFreshIdentityOnce);
        assertThrows(IllegalArgumentException.class,()->ProductionBootstrapControl.loadV3(f.contract()));
    }
    @Test void deploymentReceiptOrdinaryPreservesOriginalDataAndBurnsOnlyCurrentIdentity()throws Exception {
        Fixture f=deploymentFixture(true);var c=ProductionBootstrapControl.loadDeploymentReceipt(f.contract());
        assertEquals(ProductionBootstrapControl.Mode.ORDINARY_CERTIFIED,c.mode());assertNull(c.fresh());
        assertEquals(5,c.originalSourcePins().plan().schemaFormat());assertEquals(f.world,c.originalSourcePins().worldPath());
        assertNotEquals(c.ordinary().boot(),c.originalSourcePins().creationBootUuid());
        Files.createDirectory(f.world);c.requireOrdinaryWorld(f.world);c.consumeOrdinaryIdentityOnce();
        assertThrows(IllegalArgumentException.class,c::consumeOrdinaryIdentityOnce);
    }
    @Test void deploymentReceiptRejectsCrossVersionFieldsAndActualFilePinMutation()throws Exception {
        Fixture old=fixture();assertThrows(IllegalArgumentException.class,()->ProductionBootstrapControl.loadDeploymentReceipt(old.contract()));
        Fixture extra=deploymentFixture(false);extra.values.put("ordinarySourceApprovalPath",extra.metadata.toString().replace("\\","/"));Files.writeString(extra.control,text(extra.values));
        assertThrows(IllegalArgumentException.class,()->ProductionBootstrapControl.loadDeploymentReceipt(extra.contract()));
        Fixture b=deploymentFixture(false);Files.writeString(b.baseline,"mutated");assertThrows(IllegalArgumentException.class,()->ProductionBootstrapControl.loadDeploymentReceipt(b.contract()));
        Fixture k=deploymentFixture(false);Files.writeString(k.metadata.resolveSibling("schema5-contract.json"),"mutated");assertThrows(IllegalArgumentException.class,()->ProductionBootstrapControl.loadDeploymentReceipt(k.contract()));
        Fixture original=deploymentFixture(true);Path approval=Path.of(original.values.get("ordinarySourceApprovalPath"));
        Files.writeString(approval,Files.readString(approval).replace("FP_DEPLOYMENT_RECEIPT1_ORIGINAL_SOURCE_PINS_V1","FP_ORDINARY_SOURCE_APPROVAL_V1"));
        original.values.put("ordinarySourceApprovalSha256",ProductionBootstrapControl.digest(Files.readAllBytes(approval)));Files.writeString(original.control,text(original.values));
        assertThrows(IllegalArgumentException.class,()->ProductionBootstrapControl.loadDeploymentReceipt(original.contract()));
    }
    @Test void deploymentReceiptOrdinaryRejectsOriginalIdentityReuseAndWrongWorld()throws Exception {
        Fixture f=deploymentFixture(true);var control=ProductionBootstrapControl.loadDeploymentReceipt(f.contract());
        assertThrows(IllegalArgumentException.class,()->control.requireOrdinaryWorld(f.world.resolveSibling("other-world")));
        f.values.put("bootUuid",control.originalSourcePins().creationBootUuid().toString());Files.writeString(f.control,text(f.values));
        assertThrows(IllegalArgumentException.class,()->ProductionBootstrapControl.loadDeploymentReceipt(f.contract()));
        Fixture zero=deploymentFixture(false);zero.values.put("bootUuid",new UUID(0,0).toString());Files.writeString(zero.control,text(zero.values));
        assertThrows(IllegalArgumentException.class,()->ProductionBootstrapControl.loadDeploymentReceipt(zero.contract()));
    }
    private static String propertiesText(Properties p)throws Exception {
        var out=new StringWriter();p.store(out,null);return out.toString();
    }
    private Fixture v3Fixture(boolean ordinary)throws Exception {
        Fixture f=fixture();var p=new Properties();p.load(new StringReader(Files.readString(f.metadata)));
        String coverage="format3:all-twenty-fixed-schema-tables-and-static-objects;all-sixty-fixed-schema-triggers;required-auditself-only;other-routine-absence-unclaimed";
        p.setProperty("approvedSchemaFormat","3");p.setProperty("approvedCoverage",coverage);
        p.setProperty("format3ManifestConfigurationEncoding",MySqlRuntimeMetadata.MANIFEST_V2);
        p.setProperty("format3RuntimeContractPath",f.control.resolveSibling("ABSENT-contract.json").toString());
        p.setProperty("format3RuntimeContractSha256","D".repeat(64));
        for(String key:new String[]{"approvedBaselinePath","pkcs12TrustStorePath"})p.setProperty(key,Path.of(p.getProperty(key)).toString());
        for(String key:new String[]{"credentialSourceRef","trustSecretSourceRef"})p.setProperty(key,"file:"+Path.of(p.getProperty(key).substring(5)));
        Files.writeString(f.metadata,propertiesText(p));
        var m=MySqlRuntimeMetadata.fromBytesV2(Files.readAllBytes(f.metadata));
        f.values.put("controlFormat","FP_PRODUCTION_BOOTSTRAP_CONTROL_V3");f.values.put("schemaFormat","3");f.values.put("coverage",coverage);
        f.values.put("metadataSha256",ProductionBootstrapControl.digest(Files.readAllBytes(f.metadata)));
        f.values.put("runtimeContractSha256","D".repeat(64));
        f.values.put("configurationSha256",WindowsB1SourceAnchorStore.AnchorConfiguration.ofMetadataV2(m).configurationSha256());
        f.values.put("sourceLimitProfileCanonicalHex",HexFormat.of().withUpperCase().formatHex(V3_PROFILE.canonicalBytes()));
        f.values.put("sourceLimitProfileSha256",HexFormat.of().withUpperCase().formatHex(V3_PROFILE.sha256()));
        Files.delete(f.baseline); // V3 must not read B/K here; root-owned IO validation remains pending.
        if(ordinary) {
            var approval=new LinkedHashMap<String,String>();approval.put("approvalFormat","FP_ORDINARY_SOURCE_APPROVAL_V1");
            for(String key:new String[]{"creationUuid","bootstrapUuid","prepareUuid","certifyUuid"}) {
                approval.put(key,f.values.remove(key));
            }
            approval.put("creationBootUuid",UUID.randomUUID().toString());approval.put("creationBootGeneration","1");
            for(String key:new String[]{"bindingUuid","physicalFormat","serverUuid","schema","schemaFormat","ddlSha256","provisioningUuid"})approval.put(key,f.values.get(key));
            approval.put("canonicalReceiptLength","123");approval.put("canonicalReceiptSha256","5".repeat(64));
            int id=1;for(String key:new String[]{"sourceWorldIdentity","guardFileIdentity","receiptFileIdentity","anchorFileIdentity"})
                approval.put(key,"FP_WIN32_FILE_ID_V1;volume="+"0".repeat(15)+"1;id="+String.format("%032x",id++));
            approval.put("worldPath",f.world.toString().replace("\\","/"));approval.put("anchorPath",f.control.resolveSibling("source.anchor").toString().replace("\\","/"));
            approval.put("anchorSha256","6".repeat(64));
            for(String key:new String[]{"configurationSha256","metadataSha256","baselineSha256","runtimeContractSha256"})approval.put(key,f.values.get(key));
            Path path=f.control.resolveSibling("original.properties");Files.writeString(path,text(approval));
            f.values.put("mode","ORDINARY_CERTIFIED");f.values.put("ordinarySourceApprovalPath",path.toString().replace("\\","/"));
            f.values.put("ordinarySourceApprovalSha256",ProductionBootstrapControl.digest(Files.readAllBytes(path)));
            f.values.put("handoffAttemptUuid",UUID.randomUUID().toString());f.values.put("handoffOperationUuid",UUID.randomUUID().toString());
            Files.createDirectory(f.world);
        }
        Files.writeString(f.control,text(f.values));return f;
    }
    private ProductionBootstrapControl.StaticV3 reloadV3(Fixture f)throws Exception {
        Files.writeString(f.control,text(f.values));return ProductionBootstrapControl.loadV3(f.contract());
    }
    @Test void v3FreshAndOrdinaryPreserveTypedCompletePinsWithoutReadingBOrK()throws Exception {
        for(boolean ordinary:new boolean[]{false,true}) {
            Fixture f=v3Fixture(ordinary);var c=ProductionBootstrapControl.loadV3(f.contract());
            assertEquals(3,c.controlVersion());assertEquals(f.values.get("configurationSha256"),c.configurationSha256());
            assertEquals(c.configurationSha256(),WindowsB1SourceAnchorStore.AnchorConfiguration.ofMetadataV2(c.metadata()).configurationSha256());
            assertArrayEquals(V3_PROFILE.canonicalBytes(),c.sourceLimitProfile().canonicalBytes());assertEquals(97,c.sourceLimitProfile().canonicalBytes().length);
            assertFalse(Files.exists(c.metadata().baseline().absolutePath()));assertFalse(Files.exists(c.metadata().format3RuntimeContract().absolutePath()));
            assertFalse(c.toString().contains("fixture.example.invalid"));
            if(ordinary) { c.requireOrdinaryWorld(f.world);assertNotNull(c.originalCreation());assertEquals(c.metadata().expectedBindingUuid(),c.originalCreation().plan().bindingUuid()); }
            else { c.requireFreshWorld(f.world);assertNull(c.originalCreation()); }
        }
    }
    @Test void v3ExplicitSelectorAndModeClosedKeysNeverFallbackToOldLoad()throws Exception {
        Fixture f=v3Fixture(false);assertThrows(IllegalArgumentException.class,()->ProductionBootstrapControl.load(f.contract()));
        for(String version:new String[]{"FP_PRODUCTION_BOOTSTRAP_CONTROL_V1","FP_PRODUCTION_BOOTSTRAP_CONTROL_V2"}) {
            Fixture c=v3Fixture(false);c.values.put("controlFormat",version);assertThrows(IllegalArgumentException.class,()->reloadV3(c));
        }
        Fixture old=fixture();assertThrows(IllegalArgumentException.class,()->ProductionBootstrapControl.loadV3(old.contract()));
        for(String key:new String[]{"sourceLimitProfileCanonicalHex","sourceLimitProfileSha256","creationUuid","runtimeContractSha256"}) {
            Fixture c=v3Fixture(false);c.values.remove(key);assertThrows(IllegalArgumentException.class,()->reloadV3(c));
        }
        Fixture mixed=v3Fixture(false);mixed.values.put("handoffAttemptUuid",UUID.randomUUID().toString());assertThrows(IllegalArgumentException.class,()->reloadV3(mixed));
        Fixture duplicate=v3Fixture(false);Files.writeString(duplicate.control,text(duplicate.values)+"host=another.invalid\n");
        assertThrows(IllegalArgumentException.class,()->ProductionBootstrapControl.loadV3(duplicate.contract()));
    }
    @Test void v3IndependentControlMetadataFullAAndAllMetadataPinsAreRequired()throws Exception {
        Fixture f=v3Fixture(false);assertThrows(IllegalArgumentException.class,()->ProductionBootstrapControl.loadV3(new ProductionBootstrapControl.LaunchContract(f.control,"0".repeat(64))));
        for(String key:new String[]{"metadataSha256","configurationSha256","baselineSha256","runtimeContractSha256","sourceCaptureSha256","ddlSha256"}) {
            Fixture c=v3Fixture(false);c.values.put(key,"0".repeat(64));assertThrows(IllegalArgumentException.class,()->reloadV3(c));
        }
        for(String key:new String[]{"host","schema","serverUuid","bindingUuid","provisioningUuid"}) {
            Fixture c=v3Fixture(false);c.values.put(key,key.endsWith("Uuid")?UUID.randomUUID().toString():"different");assertThrows(IllegalArgumentException.class,()->reloadV3(c));
        }
    }
    @Test void v3ProfileIsBoundBeforeHexParsingAndUsesTheActualCanonicalCodec()throws Exception {
        for(String bad:new String[]{"A".repeat(1026),"A","ＧＧ","GG","00".repeat(512)}) {
            Fixture c=v3Fixture(false);c.values.put("sourceLimitProfileCanonicalHex",bad);assertThrows(IllegalArgumentException.class,()->reloadV3(c));
        }
        Fixture pin=v3Fixture(false);pin.values.put("sourceLimitProfileSha256","0".repeat(64));assertThrows(IllegalArgumentException.class,()->reloadV3(pin));
        for(int mode=0;mode<3;mode++) {
            Fixture c=v3Fixture(false);byte[] bytes=V3_PROFILE.canonicalBytes();
            if(mode==0)bytes[4]^=1;else if(mode==1)bytes=Arrays.copyOf(bytes,bytes.length+1);
            else Arrays.fill(bytes,bytes.length-8,bytes.length,(byte)0);
            c.values.put("sourceLimitProfileCanonicalHex",HexFormat.of().formatHex(bytes));c.values.put("sourceLimitProfileSha256",ProductionBootstrapControl.digest(bytes));
            assertThrows(IllegalArgumentException.class,()->reloadV3(c));
        }
    }
    @Test void v3PropertiesEscapesRejectUnpairedSurrogatesAndKeepSupplementaryPairs()throws Exception {
        for(String escaped:new String[]{"\\uD800","\\uD801","\\uDC00"}) {
            Fixture c=v3Fixture(false);c.values.put("username",escaped);assertThrows(IllegalArgumentException.class,()->reloadV3(c));
            Fixture m=v3Fixture(false);String raw=Files.readString(m.metadata).replace("username=runner","username="+escaped);
            Files.writeString(m.metadata,raw);m.values.put("metadataSha256",ProductionBootstrapControl.digest(Files.readAllBytes(m.metadata)));
            assertThrows(IllegalArgumentException.class,()->reloadV3(m));
        }
        Fixture paired=v3Fixture(false);String username="runner\uD83D\uDE80";
        var p=new Properties();p.load(new StringReader(Files.readString(paired.metadata)));p.setProperty("username",username);
        Files.writeString(paired.metadata,propertiesText(p));var m=MySqlRuntimeMetadata.fromBytesV2(Files.readAllBytes(paired.metadata));
        paired.values.put("metadataSha256",ProductionBootstrapControl.digest(Files.readAllBytes(paired.metadata)));
        paired.values.put("configurationSha256",WindowsB1SourceAnchorStore.AnchorConfiguration.ofMetadataV2(m).configurationSha256());paired.values.put("username",username);
        assertEquals(username,reloadV3(paired).metadata().username());
    }
    @Test void v3SharedProcessOnceIdentityCannotBeReplayedAcrossVersionsOrModes()throws Exception {
        Fixture f=v3Fixture(false);var c=ProductionBootstrapControl.loadV3(f.contract());c.consumeFreshIdentityOnce();
        assertThrows(IllegalArgumentException.class,c::consumeFreshIdentityOnce);
        assertThrows(IllegalArgumentException.class,()->ProductionBootstrapControl.loadV3(f.contract()).consumeFreshIdentityOnce());
        Fixture old=fixture();for(String key:new String[]{"bootUuid","submitOperationUuid","creationUuid","bootstrapUuid","prepareUuid","certifyUuid"})old.values.put(key,f.values.get(key));
        Files.writeString(old.control,text(old.values));assertThrows(IllegalArgumentException.class,()->ProductionBootstrapControl.load(old.contract()).consumeFreshIdentityOnce());
        Fixture ordinary=v3Fixture(true);var o=ProductionBootstrapControl.loadV3(ordinary.contract());o.consumeOrdinaryIdentityOnce();
        assertThrows(IllegalArgumentException.class,o::consumeOrdinaryIdentityOnce);
        assertThrows(IllegalArgumentException.class,()->ProductionBootstrapControl.loadV3(ordinary.contract()).consumeOrdinaryIdentityOnce());
        Fixture collision=v3Fixture(true);collision.values.put("bootUuid",f.values.get("bootUuid"));
        var cross=reloadV3(collision);assertThrows(IllegalArgumentException.class,cross::consumeOrdinaryIdentityOnce);
    }
    @Test void v3OrdinaryApprovalUsesActualTypedParserAndPreservesLegacyErrorOrder()throws Exception {
        for(String key:new String[]{"metadataSha256","configurationSha256","baselineSha256","runtimeContractSha256","bindingUuid","serverUuid","provisioningUuid","schema","ddlSha256"}) {
            Fixture c=v3Fixture(true);Path a=Path.of(c.values.get("ordinarySourceApprovalPath"));var p=new Properties();p.load(new StringReader(Files.readString(a)));
            p.setProperty(key,key.endsWith("Uuid")?UUID.randomUUID().toString():key.equals("schema")?"other_schema":"0".repeat(64));
            Files.writeString(a,propertiesText(p));c.values.put("ordinarySourceApprovalSha256",ProductionBootstrapControl.digest(Files.readAllBytes(a)));
            assertThrows(IllegalArgumentException.class,()->reloadV3(c));
        }
        Fixture wrongPin=v3Fixture(true);wrongPin.values.put("ordinarySourceApprovalSha256","0".repeat(64));
        assertThrows(IllegalArgumentException.class,()->reloadV3(wrongPin));
        for(String bad:new String[]{"\\uD800","\\uDC00"}) {
            Fixture invalid=v3Fixture(true);Path path=Path.of(invalid.values.get("ordinarySourceApprovalPath"));
            Files.writeString(path,Files.readString(path).replace("schema=fp_carrier","schema="+bad));
            invalid.values.put("ordinarySourceApprovalSha256",ProductionBootstrapControl.digest(Files.readAllBytes(path)));
            assertThrows(IllegalArgumentException.class,()->reloadV3(invalid));
        }
        Fixture c=v3Fixture(true);Path a=Path.of(c.values.get("ordinarySourceApprovalPath"));
        assertThrows(NoSuchFileException.class,()->ProductionOrdinarySourceApproval.loadPinned(a.resolveSibling("missing"),"A".repeat(64),c.control,c.world,null,"A".repeat(64),"B".repeat(64),"C".repeat(64)));
        String validPin=ProductionBootstrapControl.digest(Files.readAllBytes(a));
        assertThrows(NullPointerException.class,()->ProductionOrdinarySourceApproval.loadPinned(a,validPin,c.control,c.world,null,"A".repeat(64),"B".repeat(64),"C".repeat(64)));
        Files.writeString(a,Files.readString(a)+"schema=duplicate\n");String pin=ProductionBootstrapControl.digest(Files.readAllBytes(a));
        assertThrows(IllegalArgumentException.class,()->ProductionOrdinarySourceApproval.loadPinned(a,pin,c.control,c.world,null,"A".repeat(64),"B".repeat(64),"C".repeat(64)));
    }
    @Test void legacyV2StillLoadsAndBurnsTheSameProcessIdentityBeforeV3()throws Exception {
        Fixture f=v3Fixture(false);var v3=ProductionBootstrapControl.loadV3(f.contract());
        var p=new Properties();p.load(new StringReader(Files.readString(f.metadata)));p.remove("format3ManifestConfigurationEncoding");
        Files.writeString(f.metadata,propertiesText(p));Files.writeString(f.baseline,"static carrier byte fixture only");
        f.values.remove("sourceLimitProfileCanonicalHex");f.values.remove("sourceLimitProfileSha256");
        f.values.put("controlFormat","FP_PRODUCTION_BOOTSTRAP_CONTROL_V2");f.values.put("metadataSha256",ProductionBootstrapControl.digest(Files.readAllBytes(f.metadata)));
        var legacyMetadata=MySqlRuntimeMetadata.fromBytes(Files.readAllBytes(f.metadata));
        f.values.put("configurationSha256",WindowsB1SourceAnchorStore.AnchorConfiguration.ofMetadata(legacyMetadata).configurationSha256());
        Files.writeString(f.control,text(f.values));var old=ProductionBootstrapControl.load(f.contract());
        assertTrue(old.isProductionFormatV2());assertEquals(2,old.controlVersion());
        old.consumeFreshIdentityOnce();assertThrows(IllegalArgumentException.class,v3::consumeFreshIdentityOnce);
    }
    @Test void v3BoundedUtf8BomAndMetadataReadFailuresRemainExplicit()throws Exception {
        Fixture c=v3Fixture(false);byte[] invalid={(byte)0xC0};Files.write(c.control,invalid);
        assertThrows(CharacterCodingException.class,()->ProductionBootstrapControl.loadV3(c.contract()));
        Fixture bom=v3Fixture(false);Files.writeString(bom.control,"\uFEFF"+Files.readString(bom.control));assertThrows(IllegalArgumentException.class,()->ProductionBootstrapControl.loadV3(bom.contract()));
        Fixture big=v3Fixture(false);Files.write(big.control,new byte[65537]);assertThrows(IOException.class,()->ProductionBootstrapControl.loadV3(big.contract()));
        Fixture metadata=v3Fixture(false);Files.write(metadata.metadata,new byte[65537]);metadata.values.put("metadataSha256",ProductionBootstrapControl.digest(Files.readAllBytes(metadata.metadata)));
        assertThrows(IOException.class,()->reloadV3(metadata));
        Fixture missing=v3Fixture(false);Files.delete(missing.metadata);assertThrows(NoSuchFileException.class,()->ProductionBootstrapControl.loadV3(missing.contract()));
        for(int mode=0;mode<4;mode++) {
            Fixture approval=v3Fixture(true);Path path=Path.of(approval.values.get("ordinarySourceApprovalPath"));
            if(mode==0)Files.write(path,new byte[]{(byte)0xC0});
            else if(mode==1)Files.writeString(path,"\uFEFF"+Files.readString(path));
            else if(mode==2)Files.write(path,new byte[65537]);
            else Files.delete(path);
            if(mode!=3)approval.values.put("ordinarySourceApprovalSha256",ProductionBootstrapControl.digest(Files.readAllBytes(path)));
            if(mode==0)assertThrows(CharacterCodingException.class,()->reloadV3(approval));
            else if(mode==1)assertThrows(IllegalArgumentException.class,()->reloadV3(approval));
            else if(mode==2)assertThrows(IOException.class,()->reloadV3(approval));
            else assertThrows(NoSuchFileException.class,()->reloadV3(approval));
        }
    }

    @Test void callerDiscoveryUsesExactPinnedFormatAndNeverLoadsMetadata() throws Exception {
        for (boolean ordinary : new boolean[]{false,true}) {
            Fixture f=v3Fixture(ordinary);
            assertEquals(ProductionBootstrapControl.ProductionFormat.V3,ProductionBootstrapControl.discoverProductionFormat(f.contract()));
            Files.delete(f.metadata);
            assertEquals(ProductionBootstrapControl.ProductionFormat.V3,ProductionBootstrapControl.discoverProductionFormat(f.contract()));
            assertThrows(NoSuchFileException.class,()->ProductionBootstrapControl.loadV3(f.contract()));
        }
        Fixture legacy=fixture();
        legacy.values.put("controlFormat","FP_PRODUCTION_BOOTSTRAP_CONTROL_V2");Files.writeString(legacy.control,text(legacy.values));
        assertEquals(ProductionBootstrapControl.ProductionFormat.V2,ProductionBootstrapControl.discoverProductionFormat(legacy.contract()));
        for(String format:new String[]{"FP_PRODUCTION_BOOTSTRAP_CONTROL_V1","FP_PRODUCTION_BOOTSTRAP_CONTROL_V4","V3"}) {
            legacy.values.put("controlFormat",format);Files.writeString(legacy.control,text(legacy.values));
            assertThrows(IllegalArgumentException.class,()->ProductionBootstrapControl.discoverProductionFormat(legacy.contract()));
        }
    }
    @Test void callerDiscoveryRejectsRawPinBoundsUtf8BomAndDuplicateBeforeRouting() throws Exception {
        Fixture f=v3Fixture(false);
        assertThrows(IllegalArgumentException.class,()->ProductionBootstrapControl.discoverProductionFormat(new ProductionBootstrapControl.LaunchContract(f.control,"0".repeat(64))));
        Files.write(f.control,new byte[65537]);assertThrows(IOException.class,()->ProductionBootstrapControl.discoverProductionFormat(f.contract()));
        Files.write(f.control,new byte[]{(byte)0xc0});assertThrows(CharacterCodingException.class,()->ProductionBootstrapControl.discoverProductionFormat(f.contract()));
        Files.writeString(f.control,"\uFEFF"+text(f.values));assertThrows(IllegalArgumentException.class,()->ProductionBootstrapControl.discoverProductionFormat(f.contract()));
        Files.writeString(f.control,text(f.values)+"controlFormat=FP_PRODUCTION_BOOTSTRAP_CONTROL_V2\n");
        assertThrows(IllegalArgumentException.class,()->ProductionBootstrapControl.discoverProductionFormat(f.contract()));
        f.values.put("unrecognizedField","value");Files.writeString(f.control,text(f.values));
        assertEquals(ProductionBootstrapControl.ProductionFormat.V3,ProductionBootstrapControl.discoverProductionFormat(f.contract()));
        assertThrows(IllegalArgumentException.class,()->ProductionBootstrapControl.loadV3(f.contract()));
    }
    @Test void callerV3CommandsKeepTypedOriginFullAAndLeaveOnceToWorker() throws Exception {
        for(boolean ordinary:new boolean[]{false,true}) {
            Fixture f=v3Fixture(ordinary);var control=ProductionBootstrapControl.loadV3(f.contract());
            var command=ProductionDedicatedBootstrapCallbacks$.MODULE$.commandV3(control,f.world);
            if(ordinary) {
                var actual=assertInstanceOf(com.wi.floatingpod.storage.mysql.MySqlOwnedIoCommand.BootOrdinaryV3.class,command);
                assertSame(control,actual.control());assertSame(control.originalCreation(),actual.request().approvedOriginalCreation());
                assertEquals(control.configurationSha256(),actual.request().controlledMetadata().approvedConfigurationSha256());
                control.consumeOrdinaryIdentityOnce();assertThrows(IllegalArgumentException.class,control::consumeOrdinaryIdentityOnce);
            } else {
                var actual=assertInstanceOf(com.wi.floatingpod.storage.mysql.MySqlOwnedIoCommand.BootFreshV3.class,command);
                assertSame(control,actual.control());assertEquals(control.fresh().boot(),actual.request().boot().bootId());
                assertEquals(control.configurationSha256(),actual.request().controlledMetadata().approvedConfigurationSha256());
                assertEquals(control.metadata().expectedBindingUuid(),actual.request().plan().bindingUuid());
                control.consumeFreshIdentityOnce();assertThrows(IllegalArgumentException.class,control::consumeFreshIdentityOnce);
            }
            assertFalse(Files.exists(control.metadata().baseline().absolutePath()));
            assertFalse(Files.exists(control.metadata().format3RuntimeContract().absolutePath()));
            assertThrows(IllegalArgumentException.class,()->ProductionDedicatedBootstrapCallbacks$.MODULE$.commandV3(control,f.world.resolveSibling("wrong-world")));
        }
    }

    @Test void callerV3PreparationUsesActualBridgeOwnerAndExactOneShotGatesWithoutSubmission() throws Exception {
        for(boolean ordinary:new boolean[]{false,true}) {
            Fixture f=v3Fixture(ordinary);var input=ProductionBootstrapControl.loadV3(f.contract());
            var command=ProductionDedicatedBootstrapCallbacks$.MODULE$.commandV3(input,f.world);
            var adapterRef=new java.util.concurrent.atomic.AtomicReference<com.wi.floatingpod.storage.mysql.MySqlOwnedIoAdapter>();
            var expected=new IllegalStateException("EXPECTED pure gate boundary; no prepare or submit");
            var limits=new DedicatedBootstrapBridge.Limits(java.time.Duration.ofSeconds(5),java.time.Duration.ofSeconds(5),java.time.Duration.ofSeconds(5));
            var installation=DedicatedBootstrapBridge.install((actual,bridge)->{
                var boot=ordinary?input.ordinary().boot():input.fresh().boot();
                long generation=ordinary?input.ordinary().bootGeneration():input.fresh().bootGeneration();
                long connection=ordinary?input.ordinary().connectionGeneration():input.fresh().connectionGeneration();
                var operation=ordinary?input.ordinary().submitOperation():input.fresh().submitOperation();
                var identity=new com.wi.floatingpod.storage.mysql.MySqlOwnedIoIdentity(boot,generation,connection);
                var tag=new com.wi.floatingpod.server.io.IoOperationTag(boot,generation,connection,operation);
                var adapter=new com.wi.floatingpod.storage.mysql.MySqlOwnedIoAdapter(1,Thread.currentThread(),"pure V3 gate",identity,false);
                adapterRef.set(adapter);
                try {
                    var wrongTag=new com.wi.floatingpod.server.io.IoOperationTag(UUID.randomUUID(),generation,connection,operation);
                    var wrongControl=DedicatedBootstrapBridge.create(new Object(),()->{throw new AssertionError("never start game");});
                    org.junit.jupiter.api.function.Executable valid=()->{
                        if(ordinary)adapter.installOrdinaryCertifiedBootstrapPreparationV3(bridge,tag,(com.wi.floatingpod.storage.mysql.MySqlOwnedIoCommand.BootOrdinaryV3)command);
                        else adapter.installFreshBootstrapPreparationV3(bridge,tag,(com.wi.floatingpod.storage.mysql.MySqlOwnedIoCommand.BootFreshV3)command);
                    };
                    assertThrows(IllegalArgumentException.class,()->{
                        if(ordinary)adapter.installOrdinaryCertifiedBootstrapPreparationV3(wrongControl,tag,(com.wi.floatingpod.storage.mysql.MySqlOwnedIoCommand.BootOrdinaryV3)command);
                        else adapter.installFreshBootstrapPreparationV3(wrongControl,tag,(com.wi.floatingpod.storage.mysql.MySqlOwnedIoCommand.BootFreshV3)command);
                    });
                    assertThrows(IllegalArgumentException.class,()->{
                        if(ordinary)adapter.installOrdinaryCertifiedBootstrapPreparationV3(bridge,wrongTag,(com.wi.floatingpod.storage.mysql.MySqlOwnedIoCommand.BootOrdinaryV3)command);
                        else adapter.installFreshBootstrapPreparationV3(bridge,wrongTag,(com.wi.floatingpod.storage.mysql.MySqlOwnedIoCommand.BootFreshV3)command);
                    });
                    var otherFailure=new java.util.concurrent.atomic.AtomicReference<Throwable>();
                    var wrongOwner=Thread.ofPlatform().start(()->{try {assertThrows(IllegalArgumentException.class,valid);}catch(Throwable cause){otherFailure.set(cause);}});
                    wrongOwner.join(5000);assertFalse(wrongOwner.isAlive());assertNull(otherFailure.get());
                    assertDoesNotThrow(valid);assertThrows(IllegalArgumentException.class,valid);
                    assertTrue(adapter.checkpoint().resources().isEmpty());
                } finally {adapter.quiesce();}
                throw expected;
            },limits);
            try {
                var bridge=DedicatedBootstrapBridge.create(new Object(),()->{throw new AssertionError("never enter original server start");});
                bridge.startDeferred();assertTrue(bridge.awaitFinishedOffGame(java.time.Duration.ofSeconds(10)));
                assertEquals(DedicatedBootstrapBridge.Phase.TERMINAL_FAILED,bridge.phase());assertSame(expected,bridge.failure());
            } finally {
                DedicatedBootstrapBridge.uninstall(installation);
                var adapter=adapterRef.get();
                if(adapter!=null) {
                    assertTrue(adapter.awaitExitOffMain(5,java.util.concurrent.TimeUnit.SECONDS));
                    var checkpoint=adapter.checkpoint();assertTrue(checkpoint.io().stopped());assertFalse(checkpoint.io().running());
                    assertTrue(checkpoint.io().fatalCause().isEmpty());assertTrue(checkpoint.io().exitFailure().isEmpty());
                    assertEquals(com.wi.floatingpod.storage.mysql.MySqlOwnedIoResourceObservation$.NoVerifiedSessionOwned,checkpoint.resources().get());
                }
            }
        }
    }

}
