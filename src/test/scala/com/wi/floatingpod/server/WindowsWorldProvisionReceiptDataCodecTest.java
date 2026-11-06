package com.wi.floatingpod.server;

import org.junit.jupiter.api.Test;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.Arrays;
import java.util.Base64;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

/** 纯 DATA；固定旧向量由独立 BE framing 计算，不由被测 encoder 生成 expected。 */
class WindowsWorldProvisionReceiptDataCodecTest {
    static WindowsWorldProvisionReceiptDataCodec.PlanData plan(int format) {
        var p=B1SourceAnchorFixture.plan();
        return new WindowsWorldProvisionReceiptDataCodec.PlanData(p.creationOperationUuid(),p.bootstrapOperationUuid(),p.prepareWriteUuid(),
            p.certifyWriteUuid(),p.bindingUuid(),p.physicalFormat(),p.mysqlServerUuid(),p.schema(),format,p.ddlSha256(),p.provisioningUuid());
    }
    static WindowsWorldProvisionReceiptDataCodec.ReceiptData receipt(int format) {
        return new WindowsWorldProvisionReceiptDataCodec.ReceiptData(plan(format),B1SourceAnchorFixture.identity(1),B1SourceAnchorFixture.identity(2),
            1234,B1SourceAnchorFixture.FILETIME,new WindowsWorldProvisioner.BootIdentity(UUID.fromString("74b00f82-f777-4b09-8b21-7fbc11c68317"),1));
    }
    private static final String GOLDEN2="RlBXUkNQMDEAAAABD8Yvn35AS3yMEWxERrTYClx4Z8bLQUl7tftaPKiQCSQb5tWXaolOhoiBJHwXrWA9MYnOVlFdTQGl6MrGSSUQ7D6XxcUUDkZjno3+88lAgrAAAAAC7Sgaj+97Szi7FUh5capiEAAAABRmcF9iMV9uYXRpdmVfZml4dHVyZQAAAAKrq6urq6urq6urq6urq6urq6urq6urq6urq6urq6urq9ef3S1KfUnuqwDUWbaYLp4AAABPRlBfV0lOMzJfRklMRV9JRF9WMTt2b2x1bWU9MDAwMDAwMDAwMDAwMTIzNDtpZD0wMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMQAAAE9GUF9XSU4zMl9GSUxFX0lEX1YxO3ZvbHVtZT0wMDAwMDAwMDAwMDAxMjM0O2lkPTAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAyAAAAAAAABNIB21pkHvKAe3SwD4L3d0sJiyF/vBHGgxcAAAAAAAAAAd+4L/CveDbw2oQ49KnJm2pBGkVBUNvUfd6bsrNws9Hr";
    private static final String GOLDEN3="RlBXUkNQMDEAAAABD8Yvn35AS3yMEWxERrTYClx4Z8bLQUl7tftaPKiQCSQb5tWXaolOhoiBJHwXrWA9MYnOVlFdTQGl6MrGSSUQ7D6XxcUUDkZjno3+88lAgrAAAAAC7Sgaj+97Szi7FUh5capiEAAAABRmcF9iMV9uYXRpdmVfZml4dHVyZQAAAAOrq6urq6urq6urq6urq6urq6urq6urq6urq6urq6urq9ef3S1KfUnuqwDUWbaYLp4AAABPRlBfV0lOMzJfRklMRV9JRF9WMTt2b2x1bWU9MDAwMDAwMDAwMDAwMTIzNDtpZD0wMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMQAAAE9GUF9XSU4zMl9GSUxFX0lEX1YxO3ZvbHVtZT0wMDAwMDAwMDAwMDAxMjM0O2lkPTAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAyAAAAAAAABNIB21pkHvKAe3SwD4L3d0sJiyF/vBHGgxcAAAAAAAAAAUibsbYBL6LUH6pPDW52Vvxj7L9qGWz4z7J/WcsbGMMo";
    @Test void independentFixedSchema2And3ReceiptBytesRemainExactThroughLegacyAdapters()throws Exception {
        for(int format:new int[]{2,3}) {
            byte[] expected=Base64.getDecoder().decode(format==2?GOLDEN2:GOLDEN3);
            assertArrayEquals(expected,WindowsWorldProvisionReceiptDataCodec.encode(receipt(format)));
            var legacy=WindowsWorldProvisioner.decodeReceipt(expected);
            assertEquals(format,legacy.plan().schemaFormat());
            assertArrayEquals(expected,WindowsWorldProvisioner.encodeReceipt(legacy));
            assertArrayEquals(expected,WindowsWorldProvisionReceiptDataCodec.encode(WindowsWorldProvisionReceiptDataCodec.decode(expected)));
        }
    }
    @Test void permanentDataAcceptsOnly2And3And5WhileNativePlanAndAdapterReject5()throws Exception {
        for(int format:new int[]{2,3,5}) {
            var value=receipt(format);byte[] bytes=WindowsWorldProvisionReceiptDataCodec.encode(value);
            assertEquals(value,WindowsWorldProvisionReceiptDataCodec.decode(bytes));
            if(format==5)assertThrows(IOException.class,()->WindowsWorldProvisioner.decodeReceipt(bytes));
        }
        for(int format:new int[]{1,4,6})assertThrows(IllegalArgumentException.class,()->plan(format));
        var p=plan(5);
        assertThrows(IllegalArgumentException.class,()->new WindowsWorldProvisioner.Plan(p.creationOperationUuid(),p.bootstrapOperationUuid(),p.prepareWriteUuid(),
            p.certifyWriteUuid(),p.bindingUuid(),2,p.mysqlServerUuid(),p.schema(),5,p.ddlSha256(),p.provisioningUuid()));
    }
    @Test void schema5RecipeDataRejectsZeroPhaseAliasBadBindingSchemaHashAndProcessIdentity() {
        var p=plan(5);
        assertThrows(IllegalArgumentException.class,()->new WindowsWorldProvisionReceiptDataCodec.PlanData(new UUID(0,0),p.bootstrapOperationUuid(),p.prepareWriteUuid(),p.certifyWriteUuid(),p.bindingUuid(),2,p.mysqlServerUuid(),p.schema(),5,p.ddlSha256(),p.provisioningUuid()));
        assertThrows(IllegalArgumentException.class,()->new WindowsWorldProvisionReceiptDataCodec.PlanData(p.creationOperationUuid(),p.bootstrapOperationUuid(),p.prepareWriteUuid(),p.prepareWriteUuid(),p.bindingUuid(),2,p.mysqlServerUuid(),p.schema(),5,p.ddlSha256(),p.provisioningUuid()));
        for(String schema:new String[]{"Bad_Schema","1schema","schema-name"})assertThrows(IllegalArgumentException.class,()->new WindowsWorldProvisionReceiptDataCodec.PlanData(p.creationOperationUuid(),p.bootstrapOperationUuid(),p.prepareWriteUuid(),p.certifyWriteUuid(),p.bindingUuid(),2,p.mysqlServerUuid(),schema,5,p.ddlSha256(),p.provisioningUuid()));
        assertThrows(IllegalArgumentException.class,()->new WindowsWorldProvisionReceiptDataCodec.PlanData(p.creationOperationUuid(),p.bootstrapOperationUuid(),p.prepareWriteUuid(),p.certifyWriteUuid(),new UUID(0,1),2,p.mysqlServerUuid(),p.schema(),5,p.ddlSha256(),p.provisioningUuid()));
        assertThrows(IllegalArgumentException.class,()->new WindowsWorldProvisionReceiptDataCodec.PlanData(p.creationOperationUuid(),p.bootstrapOperationUuid(),p.prepareWriteUuid(),p.certifyWriteUuid(),p.bindingUuid(),2,p.mysqlServerUuid(),p.schema(),5,"00",p.provisioningUuid()));
        assertThrows(IllegalArgumentException.class,()->new WindowsWorldProvisionReceiptDataCodec.ReceiptData(p,"path",B1SourceAnchorFixture.identity(2),1234,B1SourceAnchorFixture.FILETIME,receipt(5).creationBoot()));
        assertThrows(IllegalArgumentException.class,()->new WindowsWorldProvisionReceiptDataCodec.ReceiptData(p,B1SourceAnchorFixture.identity(1),B1SourceAnchorFixture.identity(2),0,B1SourceAnchorFixture.FILETIME,receipt(5).creationBoot()));
    }
    static byte[] reseal(byte[] body) {
        byte[] result=Arrays.copyOf(body,body.length+32);System.arraycopy(WindowsWorldProvisioner.digest(body),0,result,body.length,32);return result;
    }
    @Test void realDataDecoderRejectsDigestTruncationTrailingVersionUtf8AndBounds()throws Exception {
        byte[] good=WindowsWorldProvisionReceiptDataCodec.encode(receipt(5)),bad=good.clone();bad[40]^=1;
        assertThrows(IOException.class,()->WindowsWorldProvisionReceiptDataCodec.decode(bad));
        assertThrows(IOException.class,()->WindowsWorldProvisionReceiptDataCodec.decode(Arrays.copyOf(good,good.length-1)));
        assertThrows(IOException.class,()->WindowsWorldProvisionReceiptDataCodec.decode(new byte[65537]));
        byte[] body=Arrays.copyOf(good,good.length-32),version=body.clone();version[11]=2;
        assertThrows(IOException.class,()->WindowsWorldProvisionReceiptDataCodec.decode(reseal(version)));
        assertThrows(IOException.class,()->WindowsWorldProvisionReceiptDataCodec.decode(reseal(Arrays.copyOf(body,body.length+1))));
        byte[] utf8=body.clone();utf8[116]=(byte)0xc0;
        var failure=assertThrows(IOException.class,()->WindowsWorldProvisionReceiptDataCodec.decode(reseal(utf8)));
        assertInstanceOf(CharacterCodingException.class,failure.getCause());
        byte[] bound=body.clone();ByteBuffer.wrap(bound).putInt(112,4097);
        assertThrows(IOException.class,()->WindowsWorldProvisionReceiptDataCodec.decode(reseal(bound)));
        for(int format:new int[]{1,4,6}) {byte[] family=body.clone();ByteBuffer.wrap(family).putInt(116+plan(5).schema().length(),format);
            assertThrows(IOException.class,()->WindowsWorldProvisionReceiptDataCodec.decode(reseal(family)));}
    }
}
