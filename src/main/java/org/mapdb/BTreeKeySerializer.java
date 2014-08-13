package org.mapdb;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Comparator;

/**
 * Custom serializer for BTreeMap keys which enables [Delta encoding](https://en.wikipedia.org/wiki/Delta_encoding).
 *
 * Keys in BTree Nodes are sorted, this enables number of tricks to save disk space.
 * For example for numbers we may store only difference between subsequent numbers, for string we can only take suffix, etc...
 *
 * @param <KEY> type of key
 * @param <KEY2> type of predigested keys
 * @param <KEYS> type of object which holds multiple keys (
 */
public abstract class BTreeKeySerializer<KEY,KEY2, KEYS>{

    public KEY2 preDigestKey(KEY key){
        return (KEY2) key;
    }

    /**
     * Serialize keys from single BTree Node.
     *
     * @param out output stream where to put ata
     * @param keys An object which represents keys
     *
     * @throws IOException
     */
    public abstract void serialize(DataOutput out, KEYS keys) throws IOException;

    /**
     * Deserializes keys for single BTree Node. To
     *
     * @param in input stream to read data from
     * @return an object which represents keys
     *
     * @throws IOException
     */
    public abstract KEYS deserialize(DataInput in, int nodeSize) throws IOException;


    public abstract int compare(KEYS keys, int pos1, int pos2);


    public abstract int compare(KEYS keys, int pos, KEY2 preDigestedKey);

    public abstract KEY getKey(KEYS keys, int pos);

    public static final BTreeKeySerializer BASIC = new BTreeKeySerializer.BasicKeySerializer(Serializer.BASIC, Fun.COMPARATOR_NON_NULL);

    public abstract Comparator comparator();

    public abstract Object emptyKeys();

    public abstract int length(Object keys);

    /** expand keys array by one and put `newKey` at position `pos` */
    public abstract Object putKey(Object keys, int pos, Object newKey);

    public abstract Object copyOfRange(Object keys, int from, int to);


    /**
     * Basic Key Serializer which just writes data without applying any compression.
     * Is used by default if no other Key Serializer is specified.
     */
    public static final class BasicKeySerializer extends BTreeKeySerializer<Object,Object, Object[]> implements Serializable {

        private static final long serialVersionUID = 1654710710946309279L;

        protected final Serializer serializer;
        protected final Comparator comparator;

        public BasicKeySerializer(Serializer serializer, Comparator comparator) {
            this.serializer = serializer;
            this.comparator = comparator;
        }

        /** used for deserialization*/
        BasicKeySerializer(SerializerBase serializerBase, DataInput is, SerializerBase.FastArrayList<Object> objectStack) throws IOException {
            objectStack.add(this);
            serializer = (Serializer) serializerBase.deserialize(is,objectStack);
            comparator = (Comparator) serializerBase.deserialize(is,objectStack);
        }

        @Override
        public void serialize(DataOutput out, Object[] keys) throws IOException {
            for(Object o:keys){
                serializer.serialize(out, o);
            }
        }

        @Override
        public Object[] deserialize(DataInput in, int nodeSize) throws IOException {
            Object[] keys = new Object[nodeSize];
            for(int i=0;i<keys.length;i++)
                keys[i] = serializer.deserialize(in,-1);
            return keys;
        }

        @Override
        public int compare(Object[] keys, int pos1, int pos2) {
            return comparator.compare(keys[pos1], keys[pos2]);

        }

        @Override
        public int compare(Object[] keys, int pos, Object preDigestedKey) {
            return comparator.compare(keys[pos],preDigestedKey);
        }

        @Override
        public Object getKey(Object[] keys, int pos) {
            return keys[pos];
        }

        @Override
        public Comparator comparator() {
            return comparator;
        }

        @Override
        public Object emptyKeys() {
            return new Object[0];
        }

        @Override
        public int length(Object keys) {
            return ((Object[])keys).length;
        }

        @Override
        public Object putKey(Object keys, int pos, Object newKey) {
            return BTreeMap.arrayPut((Object[]) keys, pos, newKey);
        }

        @Override
        public Object copyOfRange(Object keys, int from, int to) {
            return Arrays.copyOfRange((Object[]) keys,from,to);
        }
    }


    /**
     * Applies delta packing on {@code java.lang.Long}. All keys must be non negative.
     * Difference between consequential numbers is also packed itself, so for small diffs it takes only single byte per
     * number.
     */
    public static final  BTreeKeySerializer ZERO_OR_POSITIVE_LONG = new BTreeKeySerializer<Long,Long,long[]>() {

        @Override
        public void serialize(DataOutput out, long[] keys) throws IOException {
            long prev = keys[0];
            DataIO.packLong(out, prev);
            for(int i=1;i<keys.length;i++){
                long curr = keys[i];
                DataIO.packLong(out, curr - prev);
                prev = curr;
            }
        }

        @Override
        public long[] deserialize(DataInput in, int nodeSize) throws IOException {
            long[] ret = new long[nodeSize];
            long prev = 0 ;
            for(int i = 0; i<nodeSize; i++){
                prev += DataIO.unpackLong(in);
                ret[i] = prev;
            }
            return ret;
        }

        @Override
        public int compare(long[] keys, int pos1, int pos2) {
            return Fun.compareLong(keys[pos1], keys[pos2]);
        }

        @Override
        public int compare(long[] keys, int pos, Long preDigestedKey) {
            return Fun.compareLong(keys[pos], preDigestedKey);
        }

        @Override
        public Long getKey(long[] keys, int pos) {
            return new Long(keys[pos]);
        }

        @Override
        public Comparator comparator() {
            return Fun.COMPARATOR_NON_NULL;
        }

        @Override
        public Object emptyKeys() {
            return new long[0];
        }

        @Override
        public int length(Object keys) {
            return ((long[])keys).length;
        }

        @Override
        public Object putKey(Object keys, int pos, Object newKey) {
            return BTreeMap.arrayLongPut((long[])keys, pos, (Long) newKey);
        }

        @Override
        public Object copyOfRange(Object keys, int from, int to) {
            return Arrays.copyOfRange((long[]) keys,from,to);
        }
    };

    /**
     * Applies delta packing on {@code java.lang.Integer}. All keys must be non negative.
     * Difference between consequential numbers is also packed itself, so for small diffs it takes only single byte per
     * number.
     */
    public static final  BTreeKeySerializer ZERO_OR_POSITIVE_INT = new BTreeKeySerializer<Integer,Integer,int[]>() {
        @Override
        public void serialize(DataOutput out, int[] keys) throws IOException {
            int prev = keys[0];
            DataIO.packInt(out, prev);
            for(int i=1;i<keys.length;i++){
                int curr = keys[i];
                DataIO.packInt(out, curr - prev);
                prev = curr;
            }
        }

        @Override
        public int[] deserialize(DataInput in, int nodeSize) throws IOException {
            int[] ret = new int[nodeSize];
            int prev = 0 ;
            for(int i = 0; i<nodeSize; i++){
                prev += DataIO.unpackInt(in);
                ret[i] = prev;
            }
            return ret;
        }

        @Override
        public int compare(int[] keys, int pos1, int pos2) {
            return Fun.compareInt(keys[pos1], keys[pos2]);
        }

        @Override
        public int compare(int[] keys, int pos, Integer preDigestedKey) {
            return Fun.compareInt(keys[pos], preDigestedKey);
        }

        @Override
        public Integer getKey(int[] keys, int pos) {
            return new Integer(keys[pos]);
        }

        @Override
        public Comparator comparator() {
            return Fun.COMPARATOR_NON_NULL;
        }

        @Override
        public Object emptyKeys() {
            return new int[0];
        }

        @Override
        public int length(Object keys) {
            return ((int[])keys).length;
        }

        @Override
        public Object putKey(Object keys, int pos, Object newKey) {
            int array[] = (int[]) keys;
            final int[] ret = Arrays.copyOf(array,array.length+1);
            if(pos<array.length){
                System.arraycopy(array,pos,ret,pos+1,array.length-pos);
            }
            ret[pos] = (Integer) newKey;
            return ret;
        }

        @Override
        public Object copyOfRange(Object keys, int from, int to) {
           return Arrays.copyOfRange((int[]) keys,from,to);
        }

    };


    protected final static class StringKeys{
        final int[] offsets;
        final char[] chars;

        StringKeys(int[] offsets, char[] chars) {
            this.offsets = offsets;
            this.chars = chars;
        }
    }

    /**
     * Applies delta packing on {@code java.lang.String}. This serializer splits consequent strings
     * to two parts: shared prefix and different suffix. Only suffix is than stored.
     */
    public static final  BTreeKeySerializer STRING = BASIC;
//TODO reenable STRING key ser once finished
            private static final Object aa = new BTreeKeySerializer<String,char[], StringKeys>() {

        private final Charset UTF8_CHARSET = Charset.forName("UTF8");

//        @Override
//        public void serialize(DataOutput out, int start, int end, Object[] keys) throws IOException {
//            byte[] previous = null;
//            for (int i = start; i < end; i++) {
//                byte[] b = ((String) keys[i]).getBytes(UTF8_CHARSET);
//                leadingValuePackWrite(out, b, previous, 0);
//                previous = b;
//            }
//        }
//
//        @Override
//        public Object[] deserialize(DataInput in, int start, int end, int size) throws IOException {
//            Object[] ret = new Object[size];
//            byte[] previous = null;
//            for (int i = start; i < end; i++) {
//                byte[] b = leadingValuePackRead(in, previous, 0);
//                if (b == null) continue;
//                ret[i] = new String(b,UTF8_CHARSET);
//                previous = b;
//            }
//            return ret;
//        }


        @Override
        public char[] preDigestKey(String s) {
            return s.toCharArray();
        }

        @Override
        public void serialize(DataOutput out, StringKeys keys) throws IOException {
            ZERO_OR_POSITIVE_INT.serialize(out,keys.offsets);
            for(char c:keys.chars)
                DataIO.packInt(out,c);
        }

        @Override
        public StringKeys deserialize(DataInput in, int nodeSize) throws IOException {
            int[] offsets = (int[]) ZERO_OR_POSITIVE_INT.deserialize(in,nodeSize);
            int charLen = offsets[offsets.length-1];
            char[] chars = new char[charLen];
            for(int i=0;i<charLen;i++)
                chars[i] = (char) DataIO.unpackInt(in); //TODO native char packing?
            return new StringKeys(offsets,chars);
        }

        @Override
        public int compare(StringKeys keys, int pos1, int pos2) {
            int start1 = pos1==0 ? 0 : keys.offsets[pos1-1];
            int end1 = keys.offsets[pos1];
            int start2 = pos2==0 ? 0 : keys.offsets[pos2-1];
            int end2 = keys.offsets[pos2];

            String s1 = String.valueOf(keys.chars,start1, end1-start1);
            String s2 = String.valueOf(keys.chars,start2, end1-start2);
            return s1.compareTo(s2); //TODO optimize
        }

        @Override
        public int compare(StringKeys keys, int pos, char[] preDigestedKey) {
            int start1 = pos==0 ? 0 : keys.offsets[pos-1];
            int end1 = keys.offsets[pos];

            String s1 = String.valueOf(keys.chars,start1, end1-start1);
            String s2 = String.valueOf(preDigestedKey);
            return s1.compareTo(s2); //TODO optimize
        }

        @Override
        public String getKey(StringKeys keys, int pos) {
            int start1 = pos==0 ? 0 : keys.offsets[pos-1];
            int end1 = keys.offsets[pos];
            return String.valueOf(keys.chars,start1, end1-start1);
        }

        @Override
        public Comparator comparator() {
            return Fun.COMPARATOR_NON_NULL;
        }

        @Override
        public Object emptyKeys() {
            return new StringKeys(new int[0], new char[0]);
        }

        @Override
        public int length(Object keys) {
            return ((StringKeys)keys).offsets.length;
        }

        @Override
        public Object putKey(Object keys, int pos, Object newKey) {
            StringKeys keys2 = (StringKeys) keys;
            char[] newKey2 = (char[]) newKey;

            //handle empty input
            if(keys2.offsets.length==0){
                return new StringKeys(new int[newKey2.length],newKey2);
            }

            final int[] ri = new int[keys2.chars.length];
            final int charNewLen = keys2.chars[keys2.chars.length-1] + newKey2.length;
            final char[] rc = new char[charNewLen];

            final int charOffsetBeforePos = pos==0? 0 : keys2.offsets[pos-1] ;

            //copy before newKey
            if(pos!=0) {
                System.arraycopy(keys2.offsets, 0, ri, 0, pos - 1);
                System.arraycopy(keys2.chars, 0, rc, 0, charOffsetBeforePos);
            }
            //copy newKey
            ri[pos] = charOffsetBeforePos+newKey2.length;
            System.arraycopy(newKey2, 0, rc, charOffsetBeforePos, newKey2.length);

            //copy beyond newkey
            if(keys2.offsets.length!=pos){
                for(int i=pos;i<ri.length;i++){
 //                   ri[i] =
                }

            }

            return new StringKeys(ri,rc);
        }

        @Override
        public Object copyOfRange(Object keys, int from, int to) {
            throw new Error("TODO"); //TODO
        }

    };

    /**
     * Read previously written data from {@code leadingValuePackWrite()} method.
     *
     * author: Kevin Day
     */
    public static byte[] leadingValuePackRead(DataInput in, byte[] previous, int ignoreLeadingCount) throws IOException {
        int len = DataIO.unpackInt(in) - 1;  // 0 indicates null
        if (len == -1)
            return null;

        int actualCommon = DataIO.unpackInt(in);

        byte[] buf = new byte[len];

        if (previous == null) {
            actualCommon = 0;
        }


        if (actualCommon > 0) {
            in.readFully(buf, 0, ignoreLeadingCount);
            System.arraycopy(previous, ignoreLeadingCount, buf, ignoreLeadingCount, actualCommon - ignoreLeadingCount);
        }
        in.readFully(buf, actualCommon, len - actualCommon);
        return buf;
    }

    /**
     * This method is used for delta compression for keys.
     * Writes the contents of buf to the DataOutput out, with special encoding if
     * there are common leading bytes in the previous group stored by this compressor.
     *
     * author: Kevin Day
     */
    public static void leadingValuePackWrite(DataOutput out, byte[] buf, byte[] previous, int ignoreLeadingCount) throws IOException {
        if (buf == null) {
            DataIO.packInt(out, 0);
            return;
        }

        int actualCommon = ignoreLeadingCount;

        if (previous != null) {
            int maxCommon = buf.length > previous.length ? previous.length : buf.length;

            if (maxCommon > Short.MAX_VALUE) maxCommon = Short.MAX_VALUE;

            for (; actualCommon < maxCommon; actualCommon++) {
                if (buf[actualCommon] != previous[actualCommon])
                    break;
            }
        }


        // there are enough common bytes to justify compression
        DataIO.packInt(out, buf.length + 1);// store as +1, 0 indicates null
        DataIO.packInt(out, actualCommon);
        out.write(buf, 0, ignoreLeadingCount);
        out.write(buf, actualCommon, buf.length - actualCommon);

    }

    /**
     * Tuple2 Serializer which uses Default Serializer from DB and expect values to implement {@code Comparable} interface.
     */
    public static final Tuple2KeySerializer TUPLE2 = new Tuple2KeySerializer(null, null, null,null);

    /**
     * Applies delta compression on array of tuple. First tuple value may be shared between consequentive tuples, so only
     * first occurrence is serialized. An example:
     *
     * <pre>
     *     Value            Serialized as
     *     -------------------------
     *     Tuple(1, 1)       1, 1
     *     Tuple(1, 2)          2
     *     Tuple(1, 3)          3
     *     Tuple(1, 4)          4
     * </pre>
     *
     * @param <A> first tuple value
     * @param <B> second tuple value
     */
    public final  static class Tuple2KeySerializer<A,B> extends  BTreeKeySerializer<Fun.Tuple2<A,B>,Fun.Tuple2<A,B>,Fun.Tuple2<A,B>[]> implements Serializable {

        private static final long serialVersionUID = 2183804367032891772L;
        protected final Comparator<A> aComparator;
        protected final Comparator<B> bComparator;
        protected final Serializer<A> aSerializer;
        protected final Serializer<B> bSerializer;
        protected final Comparator<Fun.Tuple2<A, B>> comparator;

        /**
         * Construct new Tuple2 Key Serializer. You may pass null for some value,
         * In that case 'default' value will be used, Comparable comparator and Default Serializer from DB.
         *
         * @param aComparator comparator used for first tuple value
         * @param bComparator comparator used for second tuple value
         * @param aSerializer serializer used for first tuple value
         * @param bSerializer serializer used for second tuple value
         */
        public Tuple2KeySerializer(Comparator<A> aComparator,Comparator<B> bComparator,Serializer<A> aSerializer, Serializer<B> bSerializer){
            this.aComparator = aComparator;
            this.bComparator = bComparator;
            this.aSerializer = aSerializer;
            this.bSerializer = bSerializer;
            
            this.comparator = new Fun.Tuple2Comparator<A,B>(aComparator,bComparator);
        }

        /** used for deserialization, `extra` is to avoid argument collision */
        Tuple2KeySerializer(SerializerBase serializerBase, DataInput is, SerializerBase.FastArrayList<Object> objectStack, int extra) throws IOException {
            objectStack.add(this);
            aComparator = (Comparator<A>) serializerBase.deserialize(is,objectStack);
            bComparator = (Comparator<B>) serializerBase.deserialize(is,objectStack);
            aSerializer = (Serializer<A>) serializerBase.deserialize(is,objectStack);
            bSerializer = (Serializer<B>) serializerBase.deserialize(is,objectStack);
            comparator = new Fun.Tuple2Comparator<A,B>(aComparator,bComparator);
        }


        @Override
        public void serialize(DataOutput out,  Fun.Tuple2<A,B>[] keys) throws IOException {
            int acount=0;
            for(int i=0;i<keys.length;i++){
                Fun.Tuple2<A,B> t = (Fun.Tuple2<A, B>) keys[i];
                if(acount==0){
                    //write new A
                    aSerializer.serialize(out,t.a);
                    //count how many A are following
                    acount=1;
                    while(i+acount<keys.length && aComparator.compare(t.a, ((Fun.Tuple2<A, B>) keys[i+acount]).a)==0){
                        acount++;
                    }
                    DataIO.packInt(out, acount);
                }
                bSerializer.serialize(out,t.b);

                acount--;
            }
        }

        @Override
        public Fun.Tuple2<A,B>[] deserialize(DataInput in, int nodeSize) throws IOException {
            Fun.Tuple2<A,B>[] ret = new Fun.Tuple2[nodeSize];
            A a = null;
            int acount = 0;

            for(int i=0;i<nodeSize;i++){
                if(acount==0){
                    //read new A
                    a = aSerializer.deserialize(in,-1);
                    acount = DataIO.unpackInt(in);
                }
                B b = bSerializer.deserialize(in,-1);
                ret[i]= Fun.t2(a,b);
                acount--;
            }
            assert(acount==0);

            return ret;
        }

        @Override
        public int compare(Fun.Tuple2<A, B>[] keys, int pos1, int pos2) {
            return comparator.compare(keys[pos1],keys[pos2]);
        }

        @Override
        public int compare(Fun.Tuple2<A, B>[] keys, int pos, Fun.Tuple2<A, B> preDigestedKey) {
            return comparator.compare(keys[pos], preDigestedKey);
        }

        @Override
        public Fun.Tuple2<A, B> getKey(Fun.Tuple2<A, B>[] keys, int pos) {
            return keys[pos];
        }


        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Tuple2KeySerializer t = (Tuple2KeySerializer) o;

            return
                    Fun.eq(aComparator, t.aComparator) &&
                    Fun.eq(bComparator, t.bComparator) &&
                    Fun.eq(aSerializer, t.aSerializer) &&
                    Fun.eq(bSerializer, t.bSerializer);
        }

        @Override
        public int hashCode() {
            return aComparator.hashCode() + bComparator.hashCode() + aSerializer.hashCode() + bSerializer.hashCode();
        }

        @Override
        public Comparator comparator() {
            return comparator;
        }

        @Override
        public Object emptyKeys() {
            return new Fun.Tuple2[0];
        }

        @Override
        public int length(Object keys) {
            return ((Fun.Tuple2[])keys).length;
        }

        @Override
        public Object putKey(Object keys, int pos, Object newKey) {
            final Fun.Tuple2[] array = (Fun.Tuple2[]) keys;
            final Fun.Tuple2[] ret = Arrays.copyOf(array, array.length+1);
            if(pos<array.length){
                System.arraycopy(array, pos, ret, pos+1, array.length-pos);
            }
            ret[pos] = (Fun.Tuple2) newKey;
            return ret;
        }

        @Override
        public Object copyOfRange(Object keys, int from, int to) {
            return Arrays.copyOfRange((Fun.Tuple2[])keys,from,to);
        }


    }

    /**
     * Tuple3 Serializer which uses Default Serializer from DB and expect values to implement {@code Comparable} interface.
     */
    public static final Tuple3KeySerializer TUPLE3 = new Tuple3KeySerializer(null, null, null, null, null, null);

    /**
     * Applies delta compression on array of tuple. First and second tuple value may be shared between consequentive tuples, so only
     * first occurrence is serialized. An example:
     *
     * <pre>
     *     Value            Serialized as
     *     ----------------------------
     *     Tuple(1, 2, 1)       1, 2, 1
     *     Tuple(1, 2, 2)             2
     *     Tuple(1, 3, 3)          3, 3
     *     Tuple(1, 3, 4)             4
     * </pre>
     *
     * @param <A> first tuple value
     * @param <B> second tuple value
     * @param <C> third tuple value
     */
    public static class Tuple3KeySerializer<A,B,C> extends  BTreeKeySerializer<Fun.Tuple3<A,B,C>,Fun.Tuple3<A,B,C>,Fun.Tuple3<A,B,C>[]> implements Serializable {

        private static final long serialVersionUID = 2932442956138713885L;
        protected final Comparator<A> aComparator;
        protected final Comparator<B> bComparator;
        protected final Comparator<C> cComparator;
        protected final Serializer<A> aSerializer;
        protected final Serializer<B> bSerializer;
        protected final Serializer<C> cSerializer;
        protected final Comparator<Fun.Tuple3<A, B, C>> comparator;

        /**
         * Construct new Tuple3 Key Serializer. You may pass null for some value,
         * In that case 'default' value will be used, Comparable comparator and Default Serializer from DB.
         *
         * @param aComparator comparator used for first tuple value
         * @param bComparator comparator used for second tuple value
         * @param cComparator comparator used for third tuple value
         * @param aSerializer serializer used for first tuple value
         * @param bSerializer serializer used for second tuple value
         * @param cSerializer serializer used for third tuple value
         */
        public Tuple3KeySerializer(Comparator<A> aComparator, Comparator<B> bComparator,
                                   Comparator<C> cComparator,  Serializer<A> aSerializer,
                                   Serializer<B> bSerializer, Serializer<C> cSerializer){
            this.aComparator = aComparator;
            this.bComparator = bComparator;
            this.cComparator = cComparator;
            this.aSerializer = aSerializer;
            this.bSerializer = bSerializer;
            this.cSerializer = cSerializer;
            this.comparator = new Fun.Tuple3Comparator<A,B,C>(aComparator,bComparator,cComparator);
        }

        /** used for deserialization */
        Tuple3KeySerializer(SerializerBase serializerBase, DataInput is, SerializerBase.FastArrayList<Object> objectStack) throws IOException {
            objectStack.add(this);
            aComparator = (Comparator<A>) serializerBase.deserialize(is,objectStack);
            bComparator = (Comparator<B>) serializerBase.deserialize(is,objectStack);
            cComparator = (Comparator<C>) serializerBase.deserialize(is,objectStack);
            aSerializer = (Serializer<A>) serializerBase.deserialize(is,objectStack);
            bSerializer = (Serializer<B>) serializerBase.deserialize(is,objectStack);
            cSerializer = (Serializer<C>) serializerBase.deserialize(is,objectStack);
            this.comparator = new Fun.Tuple3Comparator<A,B,C>(aComparator,bComparator,cComparator);
        }

        @Override
        public void serialize(DataOutput out, Fun.Tuple3<A,B,C>[] keys) throws IOException {
            int acount=0;
            int bcount=0;
            for(int i=0;i<keys.length;i++){
                Fun.Tuple3<A,B,C> t = (Fun.Tuple3<A, B,C>) keys[i];
                if(acount==0){
                    //write new A
                    aSerializer.serialize(out,t.a);
                    //count how many A are following
                    acount=1;
                    while(i+acount<keys.length && aComparator.compare(t.a, ((Fun.Tuple3<A, B, C>) keys[i+acount]).a)==0){
                        acount++;
                    }
                    DataIO.packInt(out, acount);
                }
                if(bcount==0){
                    //write new B
                    bSerializer.serialize(out,t.b);
                    //count how many B are following
                    bcount=1;
                    while(i+bcount<keys.length && bComparator.compare(t.b, ((Fun.Tuple3<A, B,C>) keys[i+bcount]).b)==0){
                        bcount++;
                    }
                    DataIO.packInt(out, bcount);
                }


                cSerializer.serialize(out,t.c);

                acount--;
                bcount--;
            }

        }

        @Override
        public Fun.Tuple3<A,B,C>[] deserialize(DataInput in, int nodeSize) throws IOException {
            Fun.Tuple3<A,B,C>[] ret = new Fun.Tuple3[nodeSize];
            A a = null;
            int acount = 0;
            B b = null;
            int bcount = 0;

            for(int i=0;i<nodeSize;i++){
                if(acount==0){
                    //read new A
                    a = aSerializer.deserialize(in,-1);
                    acount = DataIO.unpackInt(in);
                }
                if(bcount==0){
                    //read new B
                    b = bSerializer.deserialize(in,-1);
                    bcount = DataIO.unpackInt(in);
                }
                C c = cSerializer.deserialize(in,-1);
                ret[i]= Fun.t3(a, b, c);
                acount--;
                bcount--;
            }
            assert(acount==0);
            assert(bcount==0);

            return ret;
        }

        @Override
        public int compare(Fun.Tuple3<A, B, C>[] keys, int pos1, int pos2) {
            return comparator.compare(keys[pos1],keys[pos2]);
        }

        @Override
        public int compare(Fun.Tuple3<A, B, C>[] keys, int pos, Fun.Tuple3<A, B, C> preDigestedKey) {
            return comparator.compare(keys[pos], preDigestedKey);
        }

        @Override
        public Fun.Tuple3<A, B, C> getKey(Fun.Tuple3<A, B, C>[] keys, int pos) {
            return keys[pos];
        }


        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Tuple3KeySerializer t = (Tuple3KeySerializer) o;

            return
                    Fun.eq(aComparator, t.aComparator) &&
                    Fun.eq(bComparator, t.bComparator) &&
                    Fun.eq(cComparator, t.cComparator) &&
                    Fun.eq(aSerializer, t.aSerializer) &&
                    Fun.eq(bSerializer, t.bSerializer) &&
                    Fun.eq(cSerializer, t.cSerializer);
        }

        @Override
        public int hashCode() {
            return aComparator.hashCode()+ bComparator.hashCode() + cComparator.hashCode()
                    +aSerializer.hashCode() + bSerializer.hashCode()+cSerializer.hashCode();
        }

        @Override
        public Comparator comparator() {
            return comparator;
        }

        @Override
        public Object emptyKeys() {
            return new Fun.Tuple3[0];
        }

        @Override
        public int length(Object keys) {
            return ((Fun.Tuple3[])keys).length;
        }

        @Override
        public Object putKey(Object keys, int pos, Object newKey) {
            final Fun.Tuple3[] array = (Fun.Tuple3[]) keys;
            final Fun.Tuple3[] ret = Arrays.copyOf(array, array.length+1);
            if(pos<array.length){
                System.arraycopy(array, pos, ret, pos+1, array.length-pos);
            }
            ret[pos] = (Fun.Tuple3) newKey;
            return ret;
        }

        @Override
        public Object copyOfRange(Object keys, int from, int to) {
            return Arrays.copyOfRange((Fun.Tuple3[])keys,from,to);
        }


    }

    /**
     * Tuple4 Serializer which uses Default Serializer from DB and expect values to implement {@code Comparable} interface.
     */
    public static final Tuple4KeySerializer TUPLE4 = new Tuple4KeySerializer(null, null, null, null, null, null, null,null);


    /**
     * Applies delta compression on array of tuple. First, second and third tuple value may be shared between consequential tuples,
     * so only first occurrence is serialized. An example:
     *
     * <pre>
     *     Value                Serialized as
     *     ----------------------------------
     *     Tuple(1, 2, 1, 1)       1, 2, 1, 1
     *     Tuple(1, 2, 1, 2)                2
     *     Tuple(1, 3, 3, 3)          3, 3, 3
     *     Tuple(1, 3, 4, 4)             4, 4
     * </pre>
     *
     * @param <A> first tuple value
     * @param <B> second tuple value
     * @param <C> third tuple value
     */
    public static class Tuple4KeySerializer<A,B,C,D> extends  BTreeKeySerializer<Fun.Tuple4<A,B,C,D>,Fun.Tuple4<A,B,C,D>,Fun.Tuple4<A,B,C,D>[]> implements Serializable {

        private static final long serialVersionUID = -1835761249723528530L;
        protected final Comparator<A> aComparator;
        protected final Comparator<B> bComparator;
        protected final Comparator<C> cComparator;
        protected final Comparator<D> dComparator;
        protected final Serializer<A> aSerializer;
        protected final Serializer<B> bSerializer;
        protected final Serializer<C> cSerializer;
        protected final Serializer<D> dSerializer;

        protected final Comparator<Fun.Tuple4<A, B, C, D>> comparator;

        /**
         * Construct new Tuple4 Key Serializer. You may pass null for some value,
         * In that case 'default' value will be used, Comparable comparator and Default Serializer from DB.
         *
         * @param aComparator comparator used for first tuple value
         * @param bComparator comparator used for second tuple value
         * @param cComparator comparator used for third tuple value
         * @param dComparator comparator used for fourth tuple value
         * @param aSerializer serializer used for first tuple value
         * @param bSerializer serializer used for second tuple value
         * @param cSerializer serializer used for third tuple value
         * @param dSerializer serializer used for fourth tuple value
         */
        public Tuple4KeySerializer(Comparator<A> aComparator, Comparator<B> bComparator, Comparator<C> cComparator,
                                   Comparator<D> dComparator,
                                   Serializer<A> aSerializer, Serializer<B> bSerializer, Serializer<C> cSerializer, Serializer<D> dSerializer){
            this.aComparator = aComparator;
            this.bComparator = bComparator;
            this.cComparator = cComparator;
            this.dComparator = dComparator;
            this.aSerializer = aSerializer;
            this.bSerializer = bSerializer;
            this.cSerializer = cSerializer;
            this.dSerializer = dSerializer;
            this.comparator = new Fun.Tuple4Comparator<A,B,C,D>(aComparator,bComparator,cComparator,dComparator);
        }

        /** used for deserialization */
        Tuple4KeySerializer(SerializerBase serializerBase, DataInput is, SerializerBase.FastArrayList<Object> objectStack) throws IOException {
            objectStack.add(this);
            aComparator = (Comparator<A>) serializerBase.deserialize(is,objectStack);
            bComparator = (Comparator<B>) serializerBase.deserialize(is,objectStack);
            cComparator = (Comparator<C>) serializerBase.deserialize(is,objectStack);
            dComparator = (Comparator<D>) serializerBase.deserialize(is,objectStack);
            aSerializer = (Serializer<A>) serializerBase.deserialize(is,objectStack);
            bSerializer = (Serializer<B>) serializerBase.deserialize(is,objectStack);
            cSerializer = (Serializer<C>) serializerBase.deserialize(is,objectStack);
            dSerializer = (Serializer<D>) serializerBase.deserialize(is,objectStack);
            this.comparator = new Fun.Tuple4Comparator<A,B,C,D>(aComparator,bComparator,cComparator,dComparator);
        }


        @Override
        public void serialize(DataOutput out, Fun.Tuple4<A,B,C,D>[] keys) throws IOException {
            int acount=0;
            int bcount=0;
            int ccount=0;
            for(int i=0;i<keys.length;i++){
                Fun.Tuple4<A,B,C,D> t = (Fun.Tuple4<A, B,C,D>) keys[i];
                if(acount==0){
                    //write new A
                    aSerializer.serialize(out,t.a);
                    //count how many A are following
                    acount=1;
                    while(i+acount<keys.length && aComparator.compare(t.a, ((Fun.Tuple4<A, B, C,D>) keys[i+acount]).a)==0){
                        acount++;
                    }
                    DataIO.packInt(out, acount);
                }
                if(bcount==0){
                    //write new B
                    bSerializer.serialize(out,t.b);
                    //count how many B are following
                    bcount=1;
                    while(i+bcount<keys.length && bComparator.compare(t.b, ((Fun.Tuple4<A, B,C,D>) keys[i+bcount]).b)==0){
                        bcount++;
                    }
                    DataIO.packInt(out, bcount);
                }
                if(ccount==0){
                    //write new C
                    cSerializer.serialize(out,t.c);
                    //count how many C are following
                    ccount=1;
                    while(i+ccount<keys.length && cComparator.compare(t.c, ((Fun.Tuple4<A, B,C,D>) keys[i+ccount]).c)==0){
                        ccount++;
                    }
                    DataIO.packInt(out, ccount);
                }


                dSerializer.serialize(out,t.d);

                acount--;
                bcount--;
                ccount--;
            }
        }

        @Override
        public Fun.Tuple4<A,B,C,D>[] deserialize(DataInput in, int nodeSize) throws IOException {
            Fun.Tuple4<A,B,C,D>[] ret = new Fun.Tuple4[nodeSize];
            A a = null;
            int acount = 0;
            B b = null;
            int bcount = 0;
            C c = null;
            int ccount = 0;


            for(int i=0;i<nodeSize;i++){
                if(acount==0){
                    //read new A
                    a = aSerializer.deserialize(in,-1);
                    acount = DataIO.unpackInt(in);
                }
                if(bcount==0){
                    //read new B
                    b = bSerializer.deserialize(in,-1);
                    bcount = DataIO.unpackInt(in);
                }
                if(ccount==0){
                    //read new C
                    c = cSerializer.deserialize(in,-1);
                    ccount = DataIO.unpackInt(in);
                }

                D d = dSerializer.deserialize(in,-1);
                ret[i]= Fun.t4(a, b, c, d);
                acount--;
                bcount--;
                ccount--;
            }
            assert(acount==0);
            assert(bcount==0);
            assert(ccount==0);

            return ret;
        }

        @Override
        public int compare(Fun.Tuple4<A, B, C, D>[] keys, int pos1, int pos2) {
            return comparator.compare(keys[pos1],keys[pos2]);
        }

        @Override
        public int compare(Fun.Tuple4<A, B, C, D>[] keys, int pos, Fun.Tuple4<A, B, C, D> preDigestedKey) {
            return comparator.compare(keys[pos], preDigestedKey);
        }

        @Override
        public Fun.Tuple4<A, B, C, D> getKey(Fun.Tuple4<A, B, C, D>[] keys, int pos) {
            return keys[pos];
        }



        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Tuple4KeySerializer t = (Tuple4KeySerializer) o;

            return
                    Fun.eq(aComparator, t.aComparator) &&
                    Fun.eq(bComparator, t.bComparator) &&
                    Fun.eq(cComparator, t.cComparator) &&
                    Fun.eq(dComparator, t.dComparator) &&
                    Fun.eq(aSerializer, t.aSerializer) &&
                    Fun.eq(bSerializer, t.bSerializer) &&
                    Fun.eq(cSerializer, t.cSerializer) &&
                    Fun.eq(dSerializer, t.dSerializer);
        }


        @Override
        public int hashCode() {
            return aComparator.hashCode()+ bComparator.hashCode() + cComparator.hashCode() + dComparator.hashCode()
                    +aSerializer.hashCode() + bSerializer.hashCode()+cSerializer.hashCode() + dSerializer.hashCode();

        }

        @Override
        public Comparator comparator() {
            return comparator;
        }

        @Override
        public Object emptyKeys() {
            return new Fun.Tuple4[0];
        }

        @Override
        public int length(Object keys) {
            return ((Fun.Tuple4[])keys).length;
        }

        @Override
        public Object putKey(Object keys, int pos, Object newKey) {
            final Fun.Tuple4[] array = (Fun.Tuple4[]) keys;
            final Fun.Tuple4[] ret = Arrays.copyOf(array, array.length+1);
            if(pos<array.length){
                System.arraycopy(array, pos, ret, pos+1, array.length-pos);
            }
            ret[pos] = (Fun.Tuple4) newKey;
            return ret;
        }

        @Override
        public Object copyOfRange(Object keys, int from, int to) {
            return Arrays.copyOfRange((Fun.Tuple4[])keys,from,to);
        }


    }


    /**
     * Applies delta compression on array of tuple. First, second and third tuple value may be shared between consequential tuples,
     * so only first occurrence is serialized. An example:
     *
     * <pre>
     *     Value                Serialized as
     *     ----------------------------------
     *     Tuple(1, 2, 1, 1)       1, 2, 1, 1
     *     Tuple(1, 2, 1, 2)                2
     *     Tuple(1, 3, 3, 3)          3, 3, 3
     *     Tuple(1, 3, 4, 4)             4, 4
     * </pre>
     *
     * @param <A> first tuple value
     * @param <B> second tuple value
     * @param <C> third tuple value
     */
    public static class Tuple5KeySerializer<A,B,C,D,E> extends  BTreeKeySerializer<Fun.Tuple5<A,B,C,D,E>,Fun.Tuple5<A,B,C,D,E>,Fun.Tuple5<A,B,C,D,E>[]> implements Serializable {

        private static final long serialVersionUID = 8607477718850453705L;
        protected final Comparator<A> aComparator;
        protected final Comparator<B> bComparator;
        protected final Comparator<C> cComparator;
        protected final Comparator<D> dComparator;
        protected final Comparator<E> eComparator;
        protected final Serializer<A> aSerializer;
        protected final Serializer<B> bSerializer;
        protected final Serializer<C> cSerializer;
        protected final Serializer<D> dSerializer;
        protected final Serializer<E> eSerializer;

        protected final Comparator<Fun.Tuple5<A, B, C, D, E>> comparator;

        /**
         * Construct new Tuple4 Key Serializer. You may pass null for some value,
         * In that case 'default' value will be used, Comparable comparator and Default Serializer from DB.
         *
         */
        public Tuple5KeySerializer(Comparator<A> aComparator, Comparator<B> bComparator, Comparator<C> cComparator,
                                   Comparator<D> dComparator, Comparator<E> eComparator,
                                   Serializer<A> aSerializer, Serializer<B> bSerializer, Serializer<C> cSerializer,
                                   Serializer<D> dSerializer, Serializer<E> eSerializer){
            this.aComparator = aComparator;
            this.bComparator = bComparator;
            this.cComparator = cComparator;
            this.dComparator = dComparator;
            this.eComparator = eComparator;
            this.aSerializer = aSerializer;
            this.bSerializer = bSerializer;
            this.cSerializer = cSerializer;
            this.dSerializer = dSerializer;
            this.eSerializer = eSerializer;
            this.comparator = new Fun.Tuple5Comparator<A,B,C,D,E>(aComparator,bComparator,cComparator,dComparator,eComparator);
        }

        /** used for deserialization */
        Tuple5KeySerializer(SerializerBase serializerBase, DataInput is, SerializerBase.FastArrayList<Object> objectStack) throws IOException {
            objectStack.add(this);
            aComparator = (Comparator<A>) serializerBase.deserialize(is,objectStack);
            bComparator = (Comparator<B>) serializerBase.deserialize(is,objectStack);
            cComparator = (Comparator<C>) serializerBase.deserialize(is,objectStack);
            dComparator = (Comparator<D>) serializerBase.deserialize(is, objectStack);
            eComparator = (Comparator<E>) serializerBase.deserialize(is, objectStack);
            aSerializer = (Serializer<A>) serializerBase.deserialize(is,objectStack);
            bSerializer = (Serializer<B>) serializerBase.deserialize(is,objectStack);
            cSerializer = (Serializer<C>) serializerBase.deserialize(is,objectStack);
            dSerializer = (Serializer<D>) serializerBase.deserialize(is,objectStack);
            eSerializer = (Serializer<E>) serializerBase.deserialize(is,objectStack);
            this.comparator = new Fun.Tuple5Comparator<A,B,C,D,E>(aComparator,bComparator,cComparator,dComparator,eComparator);
        }

        @Override
        public void serialize(DataOutput out,  Fun.Tuple5<A,B,C,D,E>[] keys) throws IOException {
            int acount=0;
            int bcount=0;
            int ccount=0;
            int dcount=0;
            for(int i=0;i<keys.length;i++){
                Fun.Tuple5<A,B,C,D,E> t = (Fun.Tuple5<A, B,C,D,E>) keys[i];
                if(acount==0){
                    //write new A
                    aSerializer.serialize(out,t.a);
                    //count how many A are following
                    acount=1;
                    while(i+acount<keys.length && aComparator.compare(t.a, ((Fun.Tuple5<A, B, C,D, E>) keys[i+acount]).a)==0){
                        acount++;
                    }
                    DataIO.packInt(out, acount);
                }
                if(bcount==0){
                    //write new B
                    bSerializer.serialize(out,t.b);
                    //count how many B are following
                    bcount=1;
                    while(i+bcount<keys.length && bComparator.compare(t.b, ((Fun.Tuple5<A, B,C,D, E>) keys[i+bcount]).b)==0){
                        bcount++;
                    }
                    DataIO.packInt(out, bcount);
                }
                if(ccount==0){
                    //write new C
                    cSerializer.serialize(out,t.c);
                    //count how many C are following
                    ccount=1;
                    while(i+ccount<keys.length && cComparator.compare(t.c, ((Fun.Tuple5<A, B,C,D, E>) keys[i+ccount]).c)==0){
                        ccount++;
                    }
                    DataIO.packInt(out, ccount);
                }

                if(dcount==0){
                    //write new D
                    dSerializer.serialize(out,t.d);
                    //count how many D are following
                    dcount=1;
                    while(i+dcount<keys.length && dComparator.compare(t.d, ((Fun.Tuple5<A, B,C,D,E>) keys[i+dcount]).d)==0){
                        dcount++;
                    }
                    DataIO.packInt(out, dcount);
                }


                eSerializer.serialize(out,t.e);

                acount--;
                bcount--;
                ccount--;
                dcount--;
            }
        }

        @Override
        public Fun.Tuple5<A,B,C,D,E>[] deserialize(DataInput in,  int nodeSize) throws IOException {
            Fun.Tuple5<A,B,C,D,E>[] ret = new Fun.Tuple5[nodeSize];
            A a = null;
            int acount = 0;
            B b = null;
            int bcount = 0;
            C c = null;
            int ccount = 0;
            D d = null;
            int dcount = 0;

            for(int i=0;i<nodeSize;i++){
                if(acount==0){
                    //read new A
                    a = aSerializer.deserialize(in,-1);
                    acount = DataIO.unpackInt(in);
                }
                if(bcount==0){
                    //read new B
                    b = bSerializer.deserialize(in,-1);
                    bcount = DataIO.unpackInt(in);
                }
                if(ccount==0){
                    //read new C
                    c = cSerializer.deserialize(in,-1);
                    ccount = DataIO.unpackInt(in);
                }
                if(dcount==0){
                    //read new D
                    d = dSerializer.deserialize(in,-1);
                    dcount = DataIO.unpackInt(in);
                }


                E e = eSerializer.deserialize(in,-1);
                ret[i]= Fun.t5(a, b, c, d, e);
                acount--;
                bcount--;
                ccount--;
                dcount--;
            }
            assert(acount==0);
            assert(bcount==0);
            assert(ccount==0);
            assert(dcount==0);

            return ret;
        }

        @Override
        public int compare(Fun.Tuple5<A, B, C, D, E>[] keys, int pos1, int pos2) {
            return comparator.compare(keys[pos1],keys[pos2]);
        }

        @Override
        public int compare(Fun.Tuple5<A, B, C, D, E>[] keys, int pos, Fun.Tuple5<A, B, C, D, E> preDigestedKey) {
            return comparator.compare(keys[pos], preDigestedKey);
        }

        @Override
        public Fun.Tuple5<A, B, C, D, E> getKey(Fun.Tuple5<A, B, C, D, E>[] keys, int pos) {
            return keys[pos];
        }



        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Tuple5KeySerializer t = (Tuple5KeySerializer) o;

            return
                    Fun.eq(aComparator, t.aComparator) &&
                    Fun.eq(bComparator, t.bComparator) &&
                    Fun.eq(cComparator, t.cComparator) &&
                    Fun.eq(dComparator, t.dComparator) &&
                    Fun.eq(eComparator, t.eComparator) &&
                    Fun.eq(aSerializer, t.aSerializer) &&
                    Fun.eq(bSerializer, t.bSerializer) &&
                    Fun.eq(cSerializer, t.cSerializer) &&
                    Fun.eq(dSerializer, t.dSerializer) &&
                    Fun.eq(eSerializer, t.eSerializer);
        }

        @Override
        public int hashCode() {
            return aComparator.hashCode()+ bComparator.hashCode() + cComparator.hashCode() + dComparator.hashCode()
                    +eComparator.hashCode()
                    +aSerializer.hashCode() + bSerializer.hashCode()+cSerializer.hashCode() + dSerializer.hashCode()
                    +eSerializer.hashCode();
        }

        @Override
        public Comparator comparator() {
            return comparator;
        }

        @Override
        public Object emptyKeys() {
            return new Fun.Tuple5[0];
        }

        @Override
        public int length(Object keys) {
            return ((Fun.Tuple5[])keys).length;
        }

        @Override
        public Object putKey(Object keys, int pos, Object newKey) {
            final Fun.Tuple5[] array = (Fun.Tuple5[]) keys;
            final Fun.Tuple5[] ret = Arrays.copyOf(array, array.length+1);
            if(pos<array.length){
                System.arraycopy(array, pos, ret, pos+1, array.length-pos);
            }
            ret[pos] = (Fun.Tuple5) newKey;
            return ret;
        }

        @Override
        public Object copyOfRange(Object keys, int from, int to) {
            return Arrays.copyOfRange((Fun.Tuple5[])keys,from,to);
        }


    }

    /**
     * Applies delta compression on array of tuple. First, second and third tuple value may be shared between consequential tuples,
     * so only first occurrence is serialized. An example:
     *
     * <pre>
     *     Value                Serialized as
     *     ----------------------------------
     *     Tuple(1, 2, 1, 1)       1, 2, 1, 1
     *     Tuple(1, 2, 1, 2)                2
     *     Tuple(1, 3, 3, 3)          3, 3, 3
     *     Tuple(1, 3, 4, 4)             4, 4
     * </pre>
     *
     * @param <A> first tuple value
     * @param <B> second tuple value
     * @param <C> third tuple value
     */
    public static class Tuple6KeySerializer<A,B,C,D,E,F> extends  BTreeKeySerializer<Fun.Tuple6<A,B,C,D,E,F>,Fun.Tuple6<A,B,C,D,E,F>,Fun.Tuple6<A,B,C,D,E,F>[]> implements Serializable {

        private static final long serialVersionUID = 3666600849149868404L;
        protected final Comparator<A> aComparator;
        protected final Comparator<B> bComparator;
        protected final Comparator<C> cComparator;
        protected final Comparator<D> dComparator;
        protected final Comparator<E> eComparator;
        protected final Comparator<F> fComparator;
        protected final Serializer<A> aSerializer;
        protected final Serializer<B> bSerializer;
        protected final Serializer<C> cSerializer;
        protected final Serializer<D> dSerializer;
        protected final Serializer<E> eSerializer;
        protected final Serializer<F> fSerializer;

        protected final Comparator<Fun.Tuple6<A, B, C, D, E, F>> comparator;

        /**
         * Construct new Tuple4 Key Serializer. You may pass null for some value,
         * In that case 'default' value will be used, Comparable comparator and Default Serializer from DB.
         *
         */
        public Tuple6KeySerializer(Comparator<A> aComparator, Comparator<B> bComparator, Comparator<C> cComparator,
                                   Comparator<D> dComparator, Comparator<E> eComparator, Comparator<F> fComparator,
                                   Serializer<A> aSerializer, Serializer<B> bSerializer, Serializer<C> cSerializer,
                                   Serializer<D> dSerializer, Serializer<E> eSerializer,Serializer<F> fSerializer){
            this.aComparator = aComparator;
            this.bComparator = bComparator;
            this.cComparator = cComparator;
            this.dComparator = dComparator;
            this.eComparator = eComparator;
            this.fComparator = fComparator;
            this.aSerializer = aSerializer;
            this.bSerializer = bSerializer;
            this.cSerializer = cSerializer;
            this.dSerializer = dSerializer;
            this.eSerializer = eSerializer;
            this.fSerializer = fSerializer;
            this.comparator = new Fun.Tuple6Comparator<A,B,C,D,E,F>(aComparator,bComparator,cComparator,dComparator,eComparator,fComparator);
        }

        /** used for deserialization */
        Tuple6KeySerializer(SerializerBase serializerBase, DataInput is, SerializerBase.FastArrayList<Object> objectStack) throws IOException {
            objectStack.add(this);
            aComparator = (Comparator<A>) serializerBase.deserialize(is,objectStack);
            bComparator = (Comparator<B>) serializerBase.deserialize(is,objectStack);
            cComparator = (Comparator<C>) serializerBase.deserialize(is,objectStack);
            dComparator = (Comparator<D>) serializerBase.deserialize(is,objectStack);
            eComparator = (Comparator<E>) serializerBase.deserialize(is,objectStack);
            fComparator = (Comparator<F>) serializerBase.deserialize(is,objectStack);
            aSerializer = (Serializer<A>) serializerBase.deserialize(is,objectStack);
            bSerializer = (Serializer<B>) serializerBase.deserialize(is,objectStack);
            cSerializer = (Serializer<C>) serializerBase.deserialize(is,objectStack);
            dSerializer = (Serializer<D>) serializerBase.deserialize(is,objectStack);
            eSerializer = (Serializer<E>) serializerBase.deserialize(is,objectStack);
            fSerializer = (Serializer<F>) serializerBase.deserialize(is,objectStack);

            this.comparator = new Fun.Tuple6Comparator<A,B,C,D,E,F>(aComparator,bComparator,cComparator,dComparator,eComparator,fComparator);
        }

        @Override
        public void serialize(DataOutput out, Fun.Tuple6<A,B,C,D,E,F>[] keys) throws IOException {
            int acount=0;
            int bcount=0;
            int ccount=0;
            int dcount=0;
            int ecount=0;
            for(int i=0;i<keys.length;i++){
                Fun.Tuple6<A,B,C,D,E,F> t = (Fun.Tuple6<A, B,C,D,E,F>) keys[i];
                if(acount==0){
                    //write new A
                    aSerializer.serialize(out,t.a);
                    //count how many A are following
                    acount=1;
                    while(i+acount<keys.length && aComparator.compare(t.a, ((Fun.Tuple6<A, B, C,D, E,F>) keys[i+acount]).a)==0){
                        acount++;
                    }
                    DataIO.packInt(out, acount);
                }
                if(bcount==0){
                    //write new B
                    bSerializer.serialize(out,t.b);
                    //count how many B are following
                    bcount=1;
                    while(i+bcount<keys.length && bComparator.compare(t.b, ((Fun.Tuple6<A, B,C,D, E,F>) keys[i+bcount]).b)==0){
                        bcount++;
                    }
                    DataIO.packInt(out, bcount);
                }
                if(ccount==0){
                    //write new C
                    cSerializer.serialize(out,t.c);
                    //count how many C are following
                    ccount=1;
                    while(i+ccount<keys.length && cComparator.compare(t.c, ((Fun.Tuple6<A, B,C,D, E,F>) keys[i+ccount]).c)==0){
                        ccount++;
                    }
                    DataIO.packInt(out, ccount);
                }

                if(dcount==0){
                    //write new C
                    dSerializer.serialize(out,t.d);
                    //count how many D are following
                    dcount=1;
                    while(i+dcount<keys.length && dComparator.compare(t.d, ((Fun.Tuple6<A, B,C,D,E,F>) keys[i+dcount]).d)==0){
                        dcount++;
                    }
                    DataIO.packInt(out, dcount);
                }

                if(ecount==0){
                    //write new C
                    eSerializer.serialize(out,t.e);
                    //count how many E are following
                    ecount=1;
                    while(i+ecount<keys.length && eComparator.compare(t.e, ((Fun.Tuple6<A, B,C,D,E,F>) keys[i+ecount]).e)==0){
                        ecount++;
                    }
                    DataIO.packInt(out, ecount);
                }


                fSerializer.serialize(out,t.f);

                acount--;
                bcount--;
                ccount--;
                dcount--;
                ecount--;
            }
        }

        @Override
        public Fun.Tuple6<A,B,C,D,E,F>[] deserialize(DataInput in, int nodeSize) throws IOException {
            Fun.Tuple6<A,B,C,D,E,F>[] ret = new Fun.Tuple6[nodeSize];
            A a = null;
            int acount = 0;
            B b = null;
            int bcount = 0;
            C c = null;
            int ccount = 0;
            D d = null;
            int dcount = 0;
            E e = null;
            int ecount = 0;


            for(int i=0;i<nodeSize;i++){
                if(acount==0){
                    //read new A
                    a = aSerializer.deserialize(in,-1);
                    acount = DataIO.unpackInt(in);
                }
                if(bcount==0){
                    //read new B
                    b = bSerializer.deserialize(in,-1);
                    bcount = DataIO.unpackInt(in);
                }
                if(ccount==0){
                    //read new C
                    c = cSerializer.deserialize(in,-1);
                    ccount = DataIO.unpackInt(in);
                }
                if(dcount==0){
                    //read new D
                    d = dSerializer.deserialize(in,-1);
                    dcount = DataIO.unpackInt(in);
                }

                if(ecount==0){
                    //read new E
                    e = eSerializer.deserialize(in,-1);
                    ecount = DataIO.unpackInt(in);
                }


                F f = fSerializer.deserialize(in,-1);
                ret[i]= Fun.t6(a, b, c, d, e, f);
                acount--;
                bcount--;
                ccount--;
                dcount--;
                ecount--;
            }
            assert(acount==0);
            assert(bcount==0);
            assert(ccount==0);
            assert(dcount==0);
            assert(ecount==0);

            return ret;
        }


        @Override
        public int compare(Fun.Tuple6<A, B, C, D, E, F>[] keys, int pos1, int pos2) {
            return comparator.compare(keys[pos1],keys[pos2]);
        }

        @Override
        public int compare(Fun.Tuple6<A, B, C, D, E, F>[] keys, int pos, Fun.Tuple6<A, B, C, D, E, F> preDigestedKey) {
            return comparator.compare(keys[pos], preDigestedKey);
        }

        @Override
        public Fun.Tuple6<A, B, C, D, E, F> getKey(Fun.Tuple6<A, B, C, D, E, F>[] keys, int pos) {
            return keys[pos];
        }



        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Tuple6KeySerializer t = (Tuple6KeySerializer) o;

            return
                    Fun.eq(aComparator, t.aComparator) &&
                    Fun.eq(bComparator, t.bComparator) &&
                    Fun.eq(cComparator, t.cComparator) &&
                    Fun.eq(dComparator, t.dComparator) &&
                    Fun.eq(eComparator, t.eComparator) &&
                    Fun.eq(fComparator, t.fComparator) &&
                    Fun.eq(aSerializer, t.aSerializer) &&
                    Fun.eq(bSerializer, t.bSerializer) &&
                    Fun.eq(cSerializer, t.cSerializer) &&
                    Fun.eq(dSerializer, t.dSerializer) &&
                    Fun.eq(eSerializer, t.eSerializer) &&
                    Fun.eq(fSerializer, t.fSerializer);

        }

        @Override
        public int hashCode() {
            return aComparator.hashCode()+ bComparator.hashCode() + cComparator.hashCode() + dComparator.hashCode()
                    +eComparator.hashCode() + fComparator.hashCode()
                    +aSerializer.hashCode() + bSerializer.hashCode()+cSerializer.hashCode() + dSerializer.hashCode()
                    +eSerializer.hashCode() + fSerializer.hashCode();
        }

        @Override
        public Comparator comparator() {
            return comparator;
        }


        @Override
        public Object emptyKeys() {
            return new Fun.Tuple6[0];
        }

        @Override
        public int length(Object keys) {
            return ((Fun.Tuple6[])keys).length;
        }

        @Override
        public Object putKey(Object keys, int pos, Object newKey) {
            final Fun.Tuple6[] array = (Fun.Tuple6[]) keys;
            final Fun.Tuple6[] ret = Arrays.copyOf(array, array.length+1);
            if(pos<array.length){
                System.arraycopy(array, pos, ret, pos+1, array.length-pos);
            }
            ret[pos] = (Fun.Tuple6) newKey;
            return ret;
        }

        @Override
        public Object copyOfRange(Object keys, int from, int to) {
            return Arrays.copyOfRange((Fun.Tuple6[])keys,from,to);
        }
    }


}