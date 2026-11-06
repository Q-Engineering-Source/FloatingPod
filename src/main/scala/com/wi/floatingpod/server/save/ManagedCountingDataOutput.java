package com.wi.floatingpod.server.save;

import java.io.DataOutput;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.util.Objects;

/** 记录实际encoder向原DataOutput提交的字节，不等于压缩buffer/磁盘/force结果。 */
public final class ManagedCountingDataOutput implements DataOutput {
    private final DataOutput actual;
    private final ManagedSourceSaveLedger ledger;
    private final ManagedSourceSaveLedger.Attempt attempt;
    private final ManagedSourceSaveLedger.Payload expected;
    private long submitted;
    private long compared;
    private boolean finished;
    public ManagedCountingDataOutput(DataOutput actual,ManagedSourceSaveLedger ledger,ManagedSourceSaveLedger.Attempt attempt) {
        this(actual,ledger,attempt,(ManagedSourceSaveLedger.Payload)null);
    }
    /** Recovery-only compare-and-forward over the exact immutable payload already retained by the original ledger. */
    public ManagedCountingDataOutput(DataOutput actual,ManagedSourceSaveLedger ledger,ManagedSourceSaveLedger.Attempt attempt,ManagedSourceSaveLedger.Payload expected) {
        this.actual=Objects.requireNonNull(actual);this.ledger=Objects.requireNonNull(ledger);this.attempt=Objects.requireNonNull(attempt);
        if(expected!=null&&(expected.length()<1||expected.length()>ledger.limits().maxDecompressedBytes()))throw new IllegalArgumentException("Prepared source payload must be non-empty and within the exact profile bound");
        this.expected=expected;
    }
    private void charge(long count)throws IOException {
        if(finished)throw new IOException("Original encoder wrote after its compare-and-forward boundary");
        if(count<0 || count>ledger.limits().maxDecompressedBytes()-submitted)throw new IOException("Actual encoder output exceeds explicit decompressed budget");
        submitted+=count;ledger.encoderBytes(attempt,submitted);
    }
    private void compareByte(int value)throws IOException {
        if(expected==null)return;
        if(compared>=expected.length()||expected.unsignedByteAt((int)compared)!=(value&0xFF))throw new IOException("Original encoder output differs from the exact prepared source payload");
        compared++;
    }
    private void compareShort(int value)throws IOException{compareByte(value>>>8);compareByte(value);}
    private void compareInt(int value)throws IOException{compareByte(value>>>24);compareByte(value>>>16);compareByte(value>>>8);compareByte(value);}
    private void compareLong(long value)throws IOException{compareInt((int)(value>>>32));compareInt((int)value);}
    private void compareBytes(byte[] bytes,int off,int len)throws IOException{
        if(expected==null)return;
        if(len>expected.length()-compared)throw new IOException("Original encoder output exceeds the exact prepared source payload");
        for(int i=0;i<len;i++)compareByte(bytes[off+i]);
    }
    @Override public void write(int b)throws IOException{compareByte(b);charge(1);if(expected==null)actual.write(b);}
    @Override public void write(byte[] b)throws IOException{Objects.requireNonNull(b);compareBytes(b,0,b.length);charge(b.length);if(expected==null)actual.write(b);}
    @Override public void write(byte[] b,int off,int len)throws IOException{Objects.checkFromIndexSize(off,len,b.length);compareBytes(b,off,len);charge(len);if(expected==null)actual.write(b,off,len);}
    @Override public void writeBoolean(boolean v)throws IOException{writeByte(v?1:0);}
    @Override public void writeByte(int v)throws IOException{write(v);}
    @Override public void writeShort(int v)throws IOException{compareShort(v);charge(2);if(expected==null)actual.writeShort(v);}
    @Override public void writeChar(int v)throws IOException{writeShort(v);}
    @Override public void writeInt(int v)throws IOException{compareInt(v);charge(4);if(expected==null)actual.writeInt(v);}
    @Override public void writeLong(long v)throws IOException{compareLong(v);charge(8);if(expected==null)actual.writeLong(v);}
    @Override public void writeFloat(float v)throws IOException{writeInt(Float.floatToIntBits(v));}
    @Override public void writeDouble(double v)throws IOException{writeLong(Double.doubleToLongBits(v));}
    @Override public void writeBytes(String s)throws IOException{Objects.requireNonNull(s);if(expected!=null){if(s.length()>expected.length()-compared)throw new IOException("Original encoder output exceeds the exact prepared source payload");for(int i=0;i<s.length();i++)compareByte(s.charAt(i));}charge(s.length());if(expected==null)actual.writeBytes(s);}
    @Override public void writeChars(String s)throws IOException{Objects.requireNonNull(s);if(expected!=null){if(2L*s.length()>expected.length()-compared)throw new IOException("Original encoder output exceeds the exact prepared source payload");for(int i=0;i<s.length();i++)compareShort(s.charAt(i));}charge(2L*s.length());if(expected==null)actual.writeChars(s);}
    @Override public void writeUTF(String s)throws IOException {
        Objects.requireNonNull(s);
        long encoded=0;
        for(int i=0;i<s.length();i++){int c=s.charAt(i);encoded+=c>=1 && c<=127 ? 1 : c<=2047 ? 2 : 3;}
        if(encoded>65535)throw new UTFDataFormatException("Actual modified UTF8 value exceeds DataOutput limit");
        if(expected!=null){long total=2+encoded;if(total>expected.length()-compared)throw new IOException("Original encoder output exceeds the exact prepared source payload");compareShort((int)encoded);for(int i=0;i<s.length();i++){int c=s.charAt(i);if(c>=1&&c<=127)compareByte(c);else if(c<=2047){compareByte(0xC0|((c>>6)&0x1F));compareByte(0x80|(c&0x3F));}else{compareByte(0xE0|((c>>12)&0x0F));compareByte(0x80|((c>>6)&0x3F));compareByte(0x80|(c&0x3F));}}}
        charge(2+encoded);if(expected==null)actual.writeUTF(s);
    }
    /** Must be called only after the original encoder returned normally. */
    public void finishCompareAndForward()throws IOException {
        if(finished)throw new IllegalStateException("Original encoder compare boundary was already finished");
        finished=true;
        if(expected!=null){if(submitted!=expected.length()||compared!=expected.length())throw new IOException("Original encoder output length differs from the exact prepared source payload");expected.writeTo(actual);}
    }
}
