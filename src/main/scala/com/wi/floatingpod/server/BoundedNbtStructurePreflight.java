package com.wi.floatingpod.server;

import java.io.IOException;
import java.io.DataOutput;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.HashSet;
import java.util.Arrays;
import java.util.Objects;
import com.wi.floatingpod.config.SourceLimitProfile;
import com.wi.floatingpod.server.source.SourceForestAllocationBudget.Scope;

/**
 * 在旧版 NBT 对象解析器分配数组或列表之前，先验证整段解压 NBT 的结构和显式扫描预算。
 */
public final class BoundedNbtStructurePreflight {
    private static final int TAG_END = 0;
    private static final int TAG_BYTE = 1;
    private static final int TAG_SHORT = 2;
    private static final int TAG_INT = 3;
    private static final int TAG_LONG = 4;
    private static final int TAG_FLOAT = 5;
    private static final int TAG_DOUBLE = 6;
    private static final int TAG_BYTE_ARRAY = 7;
    private static final int TAG_STRING = 8;
    private static final int TAG_LIST = 9;
    private static final int TAG_COMPOUND = 10;
    private static final int TAG_INT_ARRAY = 11;
    private static final int TAG_LONG_ARRAY = 12;
    private static final int MAX_TAG_ID = TAG_LONG_ARRAY;
    private static final int NO_NAMED_HEADER_OFFSET = -1;
    private static final long NODE_LOGICAL_RETAINED_BYTES = 64L;
    private static final long NODE_SPAN_OFFSETS_RETAINED_BYTES = 2L * Integer.BYTES;

    private BoundedNbtStructurePreflight() {
    }

    static void validate(byte[] decoded, ReadOnlyRegionCatalog.Limits limits) throws PreflightException {
        if (decoded == null || limits == null) {
            throw new IllegalArgumentException("decoded bytes and explicit limits are required");
        }
        Cursor cursor = new Cursor(decoded, limits.maxNbtTags(), limits.maxNbtDepth());

        int rootType = cursor.readUnsignedByte("root tag type");
        if (rootType != TAG_COMPOUND) {
            throw cursor.reject(Code.ROOT_NOT_COMPOUND, "root NBT tag must be a compound");
        }
        cursor.readModifiedUtf("root tag name");
        cursor.reserveTags(1L, "root compound");
        cursor.readCompoundPayload(1);
        if (cursor.remaining() != 0) {
            throw cursor.reject(Code.TRAILING_BYTES, "decoded NBT has bytes after the root compound");
        }
    }

    /**
     * 对数据库完整实体快照复用同一结构扫描；返回 vanilla NBT 树和解码工作区的保守费用。
     * 调用者必须先计入输入数组，再在创建 NBT 对象前从原共享账本预留返回费用。
     */
    public static long decodedAllocationBytes(byte[] decoded, SourceLimitProfile profile) throws PreflightException {
        Objects.requireNonNull(decoded, "snapshot bytes");
        Objects.requireNonNull(profile, "exact snapshot profile");
        if (decoded.length > profile.maxDecompressedBytes() || profile.maxNbtDepth() > 512L) {
            throw new IllegalArgumentException("Snapshot exceeds explicit bytes or patched NBT depth");
        }
        Cursor cursor = new Cursor(decoded, profile.maxMembers(), Math.toIntExact(profile.maxNbtDepth()));
        if (cursor.readUnsignedByte("snapshot root type") != TAG_COMPOUND) {
            throw cursor.reject(Code.ROOT_NOT_COMPOUND, "snapshot root must be a compound");
        }
        cursor.readModifiedUtf("snapshot root name");
        cursor.reserveTags(1L, "snapshot root");
        cursor.readCompoundPayload(1);
        if (cursor.remaining() != 0) {
            throw cursor.reject(Code.TRAILING_BYTES, "snapshot contains trailing NBT bytes");
        }
        // 每个tag包括对象、集合槽/节点及字段名；字符串/数组和readUTF临时区按输入字节计费。
        return Math.addExact(2048L, Math.addExact(Math.multiplyExact(cursor.reservedTags, 256L),
                Math.multiplyExact(decoded.length, 16L)));
    }

    /** 同一Cursor的有界semantic数据读取；不签来源、coverage或native证明。 */
    public static Node extract(byte[] decoded, SourceLimitProfile profile) throws PreflightException {
        return extract(decoded,profile,profile==null?0:profile.maxRetainedBytes());
    }
    /** 显式扣除调用者已有payload责任后的剩余workspace；不能扩大shared profile。 */
    public static Node extract(byte[] decoded, SourceLimitProfile profile, long availableRetainedBytes) throws PreflightException {
        return extract(decoded,profile,availableRetainedBytes,Scope.untracked());
    }
    public static Node extract(byte[] decoded,SourceLimitProfile profile,long availableRetainedBytes,Scope work)throws PreflightException {
        if(decoded==null || profile==null)throw new IllegalArgumentException("NBT bytes and exact profile required");
        if(availableRetainedBytes<0 || availableRetainedBytes>profile.maxRetainedBytes())throw new IllegalArgumentException("Invalid remaining workspace budget");
        if(decoded.length>profile.maxDecompressedBytes() || profile.maxNbtDepth()>512L)
            throw new IllegalArgumentException("Semantic NBT exceeds explicit bytes or patched 512-depth representation");
        long initial=Math.multiplyExact((long)decoded.length,2L);
        if(initial>availableRetainedBytes)throw new IllegalArgumentException("Semantic NBT input/copy retention budget exhausted");
        work.charge(Math.addExact(decoded.length,192L),"NBT owned input and cursor");byte[] owned=decoded.clone();Cursor cursor=new Cursor(owned,profile.maxMembers(),Math.toIntExact(profile.maxNbtDepth()),availableRetainedBytes-initial,work);
        int headerStart=cursor.position;
        int type=cursor.readUnsignedByte("root tag type");
        if(type!=TAG_COMPOUND)throw cursor.reject(Code.ROOT_NOT_COMPOUND,"root NBT tag must be a compound");
        String name=cursor.readModifiedUtf("root tag name");int headerEnd=cursor.position;cursor.reserveTags(1L,"root compound");
        Node root=cursor.readPayload(type,1,name,headerStart,headerEnd);
        if(cursor.remaining()!=0)throw cursor.reject(Code.TRAILING_BYTES,"decoded NBT has bytes after root");
        return root;
    }
    /** 防御只读完整typed NBT节点；原数组仅同一解析器持有，未知原字段不被删掉。 */
    public static final class Node {
        private final Scope work;private final byte[] source;private final int type,namedHeaderStart,namedHeaderEnd,start,end;private final String name;private final List<Node> children;
        private Node(byte[] source,int type,String name,int namedHeaderStart,int namedHeaderEnd,int start,int end,List<Node> children,Scope work) {
            this.work=work;
            this.source=source;this.type=type;this.name=name;this.namedHeaderStart=namedHeaderStart;this.namedHeaderEnd=namedHeaderEnd;this.start=start;this.end=end;this.children=List.copyOf(children);
        }
        public int type(){return type;}public String name(){return name;}public List<Node> children(){return children;}
        /** Whether this tag retained its original named-header span; list elements have no named header. */
        public boolean hasNamedHeader(){return namedHeaderStart!=NO_NAMED_HEADER_OFFSET;}
        /** The exact original type/name header length; list elements explicitly have no header. */
        public int originalHeaderLength(){requireNamedHeader();return namedHeaderEnd-namedHeaderStart;}
        /** The exact original payload span length, including LIST element type/count bytes where applicable. */
        public int originalPayloadLength(){return end-start;}
        /** Reads the retained element type byte from an actual list payload, including empty lists. */
        public int listElementType(){
            if(type!=TAG_LIST)throw new IllegalStateException("Exact list NBT required");
            if(start>=end)throw new IllegalStateException("Parsed list has no original element type byte");
            return source[start]&0xff;
        }
        /** Writes original type/name bytes without exposing or passing the owned source array. */
        public void writeOriginalHeaderTo(DataOutput output)throws IOException{
            requireNamedHeader();Objects.requireNonNull(output,"original NBT header output");writeOriginalSpanTo(output,namedHeaderStart,namedHeaderEnd);
        }
        /** Streams the exact original payload bytes without exposing or passing the owned source array. */
        public void writeOriginalPayloadTo(DataOutput output)throws IOException{
            Objects.requireNonNull(output,"original NBT payload output");writeOriginalSpanTo(output,start,end);
        }
        private void requireNamedHeader(){if(!hasNamedHeader())throw new IllegalStateException("NBT list element has no named header");}
        private void writeOriginalSpanTo(DataOutput output,int spanStart,int spanEnd)throws IOException{
            for(int index=spanStart;index<spanEnd;index++)output.writeByte(source[index]);
        }
        public byte[] payloadBytes(){work.charge(Math.addExact(end-start,64L),"NBT payload defensive copy");return Arrays.copyOfRange(source,start,end);}
        public Node child(String exactName) {
            if(type!=TAG_COMPOUND)throw new IllegalStateException("Exact compound required");
            for(Node child:children)if(child.name.equals(exactName))return child;return null;
        }
        public long integerBits() {
            if(type!=TAG_LONG && type!=TAG_DOUBLE)throw new IllegalStateException("Exact long/double NBT required");
            return ByteBuffer.wrap(source,start,8).getLong();
        }
        public int intBits() {
            if(type!=TAG_INT && type!=TAG_FLOAT)throw new IllegalStateException("Exact int/float NBT required");
            return ByteBuffer.wrap(source,start,4).getInt();
        }
        public String stringValue() throws IOException {
            if(type!=TAG_STRING)throw new IllegalStateException("Exact string NBT required");
            work.charge(Math.addExact(192L,Math.multiplyExact(end-start,4L)),"NBT stringValue streams and decoded string");try(DataInputStream input=new DataInputStream(new ByteArrayInputStream(source,start,end-start))){return input.readUTF();}
        }
        public boolean matchesStandaloneCompound(byte[] standalone) {
            if(type!=TAG_COMPOUND || standalone==null || standalone.length!=3+end-start || standalone[0]!=10 || standalone[1]!=0 || standalone[2]!=0)return false;
            for(int i=0;i<end-start;i++)if(source[start+i]!=standalone[3+i])return false;return true;
        }
    }

    public enum Code {
        TRUNCATED_INPUT,
        ROOT_NOT_COMPOUND,
        UNKNOWN_TAG,
        ILLEGAL_TAG_END,
        NEGATIVE_ARRAY_LENGTH,
        NEGATIVE_LIST_COUNT,
        TAG_END_LIST_WITH_ELEMENTS,
        TAG_BUDGET_EXCEEDED,
        DEPTH_LIMIT,
        ARRAY_LENGTH_OVERFLOW,
        ARRAY_TRACKER_MULTIPLICATION_OVERFLOW,
        TRUNCATED_ARRAY,
        TRUNCATED_LIST,
        MALFORMED_MODIFIED_UTF,
        TRAILING_BYTES,
        DUPLICATE_COMPOUND_NAME,
        RETAINED_BUDGET
    }

    public static final class PreflightException extends IOException {
        private final Code code;

        private PreflightException(Code code, String message) {
            super(message);
            this.code = code;
        }

        private PreflightException(Code code, String message, Throwable cause) {
            super(message, cause);
            this.code = code;
        }

        public Code code() {
            return code;
        }
    }

    private static final class Cursor {
        private final byte[] bytes;
        private final long maximumTags;
        private final int maximumDepth;
        private int position;
        private long reservedTags;
        private final boolean capture;
        private long semanticRemaining;
        private final Scope work;

        private Cursor(byte[] bytes, long maximumTags, int maximumDepth) {
            this.bytes = bytes;
            this.maximumTags = maximumTags;
            this.maximumDepth = maximumDepth;this.capture=false;this.semanticRemaining=0;this.work=Scope.untracked();
        }
        private Cursor(byte[] bytes,long maximumTags,int maximumDepth,long semanticRemaining,Scope work) {
            this.work=work;
            this.bytes=bytes;this.maximumTags=maximumTags;this.maximumDepth=maximumDepth;this.capture=true;this.semanticRemaining=semanticRemaining;
        }
        private void chargeSemantic(long bytes) throws PreflightException {
            chargeSemantic(bytes,"NBT semantic allocation");
        }
        private void chargeSemantic(long bytes,String reason) throws PreflightException {
            if(bytes<0 || bytes>semanticRemaining)throw reject(Code.RETAINED_BUDGET,"Semantic NBT retained workspace budget exhausted");work.charge(bytes,reason);semanticRemaining-=bytes;
        }

        private int remaining() {
            return bytes.length - position;
        }

        private List<Node> readCompoundPayload(int depth) throws PreflightException {
            requireDepth(depth);if(capture)work.charge(192L,"NBT compound collection objects");List<Node> children=capture?new ArrayList<>():List.of();HashSet<String> names=capture?new HashSet<>():null;
            while (true) {
                int headerStart=position;
                int tagId = readUnsignedByte("compound child type");
                if (tagId == TAG_END) {
                    return children;
                }
                requireKnownTag(tagId);
                reserveTags(1L, "compound child");
                String childName=readModifiedUtf("compound child name");
                int headerEnd=position;
                if(capture)work.charge(128L,"NBT compound list/set entries");
                if(capture && !names.add(childName))throw reject(Code.DUPLICATE_COMPOUND_NAME,"Duplicate compound key cannot provide unambiguous semantic data");
                Node child=readPayload(tagId,depth+1,childName,headerStart,headerEnd);if(capture)children.add(child);
            }
        }

        private Node readPayload(int tagId,int depth,String name,int namedHeaderStart,int namedHeaderEnd) throws PreflightException {
            if(tagId==TAG_END)throw reject(Code.ILLEGAL_TAG_END,"TAG_End is only a compound terminator/empty list type");
            requireKnownTag(tagId);int start=position;List<Node> children=List.of();
            switch(tagId) {
                case TAG_BYTE -> skip(1L,"byte payload");
                case TAG_SHORT -> skip(2L,"short payload");
                case TAG_INT,TAG_FLOAT -> skip(4L,"32-bit payload");
                case TAG_LONG,TAG_DOUBLE -> skip(8L,"64-bit payload");
                case TAG_BYTE_ARRAY -> readArray(1L,8L);
                case TAG_STRING -> readModifiedUtf("string payload");
                case TAG_LIST -> children=readListPayload(depth);
                case TAG_COMPOUND -> children=readCompoundPayload(depth);
                case TAG_INT_ARRAY -> readArray(4L,32L);
                case TAG_LONG_ARRAY -> readArray(8L,64L);
                default -> throw reject(Code.UNKNOWN_TAG,"unknown NBT tag id "+tagId);
            }
            if(!capture)return null;
            chargeSemantic(Math.addExact(NODE_LOGICAL_RETAINED_BYTES,NODE_SPAN_OFFSETS_RETAINED_BYTES),"NBT node logical 64B plus two original header offset int fields");
            work.charge(Math.addExact(64L,Math.multiplyExact(children.size(),32L)),"NBT immutable child-list representation");
            return new Node(bytes,tagId,name,namedHeaderStart,namedHeaderEnd,start,position,children,work);
        }

        private List<Node> readListPayload(int depth) throws PreflightException {
            requireDepth(depth);
            int elementType = readUnsignedByte("list element type");
            int elementCount = readInt("list element count");
            if (elementCount < 0) {
                throw reject(Code.NEGATIVE_LIST_COUNT, "NBT list element count is negative");
            }
            requireKnownListElementType(elementType);
            if (elementType == TAG_END && elementCount != 0) {
                throw reject(Code.TAG_END_LIST_WITH_ELEMENTS, "TAG_End list must have zero elements");
            }
            reserveTags(elementCount, "list elements");
            if(capture)work.charge(Math.addExact(96L,Math.multiplyExact(elementCount,32L)),"NBT list representation");List<Node> children=capture?new ArrayList<>():List.of();
            if (elementCount == 0) { return children; }

            long minimumPayloadBytes;
            try {
                minimumPayloadBytes = Math.multiplyExact((long) elementCount, minimumPayloadBytes(elementType));
            } catch (ArithmeticException overflow) {
                throw new PreflightException(Code.TRUNCATED_LIST, "NBT list minimum payload length overflows", overflow);
            }
            if (minimumPayloadBytes > remaining()) {
                throw reject(Code.TRUNCATED_LIST, "NBT list count cannot fit in the remaining decoded bytes");
            }
            for (int index = 0; index < elementCount; index++) {
                Node child=readPayload(elementType,depth+1,"",NO_NAMED_HEADER_OFFSET,NO_NAMED_HEADER_OFFSET);if(capture)children.add(child);
            }
            return children;
        }

        private void readArray(long elementWidthBytes, long legacyTrackerBitsPerElement) throws PreflightException {
            int elementCount = readInt("array element count");
            if (elementCount < 0) {
                throw reject(Code.NEGATIVE_ARRAY_LENGTH, "NBT array element count is negative");
            }
            long payloadBytes;
            long legacyTrackerBits;
            try {
                payloadBytes = Math.multiplyExact((long) elementCount, elementWidthBytes);
                legacyTrackerBits = Math.multiplyExact((long) elementCount, legacyTrackerBitsPerElement);
            } catch (ArithmeticException overflow) {
                throw new PreflightException(Code.ARRAY_LENGTH_OVERFLOW, "NBT array length multiplication overflows", overflow);
            }
            if (legacyTrackerBits > Integer.MAX_VALUE) {
                throw reject(
                    Code.ARRAY_TRACKER_MULTIPLICATION_OVERFLOW,
                    "NBT array length overflows the patched parser's int tracker multiplication"
                );
            }
            if (payloadBytes > remaining()) {
                throw reject(Code.TRUNCATED_ARRAY, "NBT array payload is shorter than its declared element count");
            }
            skip(payloadBytes, "array payload");
        }

        private String readModifiedUtf(String context) throws PreflightException {
            int encodedStart=position;int byteLength = readUnsignedShort(context + " byte length");
            if (byteLength > remaining()) {
                throw reject(Code.MALFORMED_MODIFIED_UTF, context + " is shorter than its modified-UTF length prefix");
            }
            int end;
            try {
                end = Math.toIntExact(Math.addExact((long) position, byteLength));
            } catch (ArithmeticException overflow) {
                throw new PreflightException(Code.MALFORMED_MODIFIED_UTF, context + " modified-UTF boundary overflows", overflow);
            }
            while (position < end) {
                int first = bytes[position++] & 0xFF;
                int leadingNibble = first >>> 4;
                if (leadingNibble <= 7) {
                    continue;
                }
                if (leadingNibble == 12 || leadingNibble == 13) {
                    requireModifiedUtfContinuation(end, context);
                    continue;
                }
                if (leadingNibble == 14) {
                    requireModifiedUtfContinuation(end, context);
                    requireModifiedUtfContinuation(end, context);
                    continue;
                }
                throw reject(Code.MALFORMED_MODIFIED_UTF, context + " contains an invalid modified-UTF lead byte");
            }
            if(!capture)return "";chargeSemantic(Math.addExact(40L,Math.multiplyExact((long)byteLength,2L)));work.charge(Math.addExact(192L,Math.multiplyExact(byteLength,2L)),"NBT modified UTF decode arrays and streams");
            try(DataInputStream input=new DataInputStream(new ByteArrayInputStream(bytes,encodedStart,position-encodedStart))){return input.readUTF();}
            catch(IOException cause){throw new PreflightException(Code.MALFORMED_MODIFIED_UTF,context+" decoding failed",cause);}
        }

        private void requireModifiedUtfContinuation(int end, String context) throws PreflightException {
            if (position >= end) {
                throw reject(Code.MALFORMED_MODIFIED_UTF, context + " contains a truncated modified-UTF sequence");
            }
            int continuation = bytes[position++] & 0xFF;
            if ((continuation & 0xC0) != 0x80) {
                throw reject(Code.MALFORMED_MODIFIED_UTF, context + " contains an invalid modified-UTF continuation byte");
            }
        }

        private int readUnsignedByte(String context) throws PreflightException {
            requireRemaining(1L, context);
            return bytes[position++] & 0xFF;
        }

        private int readUnsignedShort(String context) throws PreflightException {
            requireRemaining(2L, context);
            int result = ((bytes[position] & 0xFF) << 8) | (bytes[position + 1] & 0xFF);
            position += 2;
            return result;
        }

        private int readInt(String context) throws PreflightException {
            requireRemaining(4L, context);
            int result = ((bytes[position] & 0xFF) << 24)
                | ((bytes[position + 1] & 0xFF) << 16)
                | ((bytes[position + 2] & 0xFF) << 8)
                | (bytes[position + 3] & 0xFF);
            position += 4;
            return result;
        }

        private void requireRemaining(long count, String context) throws PreflightException {
            if (count < 0L || count > remaining()) {
                throw reject(Code.TRUNCATED_INPUT, "decoded NBT ended while reading " + context);
            }
        }

        private void skip(long count, String context) throws PreflightException {
            requireRemaining(count, context);
            try {
                position = Math.toIntExact(Math.addExact((long) position, count));
            } catch (ArithmeticException overflow) {
                throw new PreflightException(Code.TRUNCATED_INPUT, context + " boundary overflows", overflow);
            }
        }

        private void reserveTags(long count, String context) throws PreflightException {
            if (count < 0L) {
                throw reject(Code.TAG_BUDGET_EXCEEDED, context + " has a negative tag reservation");
            }
            long next;
            try {
                next = Math.addExact(reservedTags, count);
            } catch (ArithmeticException overflow) {
                throw new PreflightException(Code.TAG_BUDGET_EXCEEDED, "NBT tag reservation overflows", overflow);
            }
            if (next > maximumTags) {
                throw reject(Code.TAG_BUDGET_EXCEEDED, "NBT structure exceeds the explicit tag-scan budget");
            }
            reservedTags = next;
        }

        private void requireDepth(int depth) throws PreflightException {
            if (depth > maximumDepth) {
                throw reject(Code.DEPTH_LIMIT, "NBT structure exceeds the explicit depth limit");
            }
        }

        private void requireKnownTag(int tagId) throws PreflightException {
            if (tagId < TAG_BYTE || tagId > MAX_TAG_ID) {
                throw reject(Code.UNKNOWN_TAG, "unknown NBT tag id " + tagId);
            }
        }

        private void requireKnownListElementType(int tagId) throws PreflightException {
            if (tagId < TAG_END || tagId > MAX_TAG_ID) {
                throw reject(Code.UNKNOWN_TAG, "unknown NBT list element tag id " + tagId);
            }
        }

        private PreflightException reject(Code code, String message) {
            return new PreflightException(code, message);
        }

        private static long minimumPayloadBytes(int tagId) throws PreflightException {
            return switch (tagId) {
                case TAG_BYTE -> 1L;
                case TAG_SHORT -> 2L;
                case TAG_INT, TAG_FLOAT -> 4L;
                case TAG_LONG, TAG_DOUBLE -> 8L;
                case TAG_BYTE_ARRAY, TAG_INT_ARRAY, TAG_LONG_ARRAY -> 4L;
                case TAG_STRING -> 2L;
                case TAG_LIST -> 5L;
                case TAG_COMPOUND -> 1L;
                default -> throw new PreflightException(Code.UNKNOWN_TAG, "unknown NBT list element tag id " + tagId);
            };
        }
    }
}
