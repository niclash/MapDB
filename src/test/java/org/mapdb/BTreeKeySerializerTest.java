package org.mapdb;

import org.junit.Test;

import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

@SuppressWarnings({"rawtypes","unchecked"})
public class BTreeKeySerializerTest {

    @Test public void testLong(){
        DB db = DBMaker.newMemoryDB()
                .cacheDisable()
                .make();
        Map m = db.createTreeMap("test")
                .keySerializer(BTreeKeySerializer.ZERO_OR_POSITIVE_LONG)
                .make();

        for(long i = 0; i<1000;i++){
            m.put(i*i,i*i+1);
        }

        for(long i = 0; i<1000;i++){
            assertEquals(i * i + 1, m.get(i * i));
        }
    }


    @Test public void testString(){


        DB db = DBMaker.newMemoryDB()
                .cacheDisable()
                .make();
        Map m =  db.createTreeMap("test")
                .keySerializer(BTreeKeySerializer.STRING)
                .make();


        List<String> list = new ArrayList <String>();
        for(long i = 0; i<1000;i++){
            String s = ""+ Math.random()+(i*i*i);
            m.put(s,s+"aa");
        }

        for(String s:list){
            assertEquals(s+"aa",m.get(s));
        }
    }

    final BTreeKeySerializer.Tuple2KeySerializer tuple2_serializer = new BTreeKeySerializer.Tuple2KeySerializer(
            Fun.COMPARATOR_NON_NULL, Fun.COMPARATOR_NON_NULL,
            Serializer.BASIC, Serializer.BASIC);

    @Test public void tuple2_simple() throws IOException {
        List<Fun.Tuple2<String,Integer>> v = new ArrayList<Fun.Tuple2<String, Integer>>();

        v.add(null);
        v.add(Fun.t2("aa",1));
        v.add(Fun.t2("aa",2));
        v.add(Fun.t2("aa",3));
        v.add(Fun.t2("bb",3));
        v.add(Fun.t2("zz",1));
        v.add(Fun.t2("zz",2));
        v.add(Fun.t2("zz",3));
        v.add(null);

        DataIO.DataOutputByteArray out = new DataIO.DataOutputByteArray();

        tuple2_serializer.serialize(out, v.toArray(new Fun.Tuple2[0]));

        DataInput in = new DataIO.DataInputByteArray(out.copyBytes());
        Object[] nn = tuple2_serializer.deserialize(in, v.size());

        assertArrayEquals(v.toArray(), nn);

    }
    @Test public void tuple2() throws IOException {
        List<Fun.Tuple2<String,Integer>> v = new ArrayList<Fun.Tuple2<String, Integer>>();

        v.add(null);

        for(String s: new String[]{"aa","bb","oper","zzz"}){
            for(int i = 2;i<1000;i+=i/2){
                v.add(Fun.t2(s,i));
            }
        }

        v.add(null);

        DataIO.DataOutputByteArray out = new DataIO.DataOutputByteArray();
        tuple2_serializer.serialize(out, new Fun.Tuple2[0]);

        DataInput in = new DataIO.DataInputByteArray(out.copyBytes());
        Object[] nn = tuple2_serializer.deserialize(in, v.size());

        assertArrayEquals(v.toArray(), nn);

    }


    final BTreeKeySerializer.Tuple3KeySerializer tuple3_serializer = new BTreeKeySerializer.Tuple3KeySerializer(
            Fun.COMPARATOR_NON_NULL, Fun.COMPARATOR_NON_NULL, Fun.COMPARATOR_NON_NULL,
            Serializer.BASIC, Serializer.BASIC, Serializer.BASIC);

    @Test public void tuple3_simple() throws IOException {
        List<Fun.Tuple3<String,Integer, Double>> v = new ArrayList<Fun.Tuple3<String, Integer, Double>>();

        v.add(null);
        v.add(Fun.t3("aa",1,1D));
        v.add(Fun.t3("aa",1,2D));
        v.add(Fun.t3("aa",2,2D));
        v.add(Fun.t3("aa",3,2D));
        v.add(Fun.t3("aa",3,3D));
        v.add(Fun.t3("zz",1,2D));
        v.add(Fun.t3("zz",2,2D));
        v.add(Fun.t3("zz",3,2D));
        v.add(null);

        DataIO.DataOutputByteArray out = new DataIO.DataOutputByteArray();

        tuple3_serializer.serialize(out, new Fun.Tuple3[0]);

        DataInput in = new DataIO.DataInputByteArray(out.copyBytes());
        Object[] nn = tuple3_serializer.deserialize(in, v.size());

        assertArrayEquals(v.toArray(), nn);

    }
    @Test public void tuple3() throws IOException {
        List<Fun.Tuple3<String,Integer,Double>> v = new ArrayList<Fun.Tuple3<String, Integer,Double>>();

        v.add(null);

        for(String s: new String[]{"aa","bb","oper","zzz"}){
            for(int i = 2;i<1000;i+=i/2){
                for(double d = 2D;i<1000;i+=i/2){
                    v.add(Fun.t3(s,i,d));
                }
            }
        }

        v.add(null);

        DataIO.DataOutputByteArray out = new DataIO.DataOutputByteArray();
        tuple3_serializer.serialize(out, new Fun.Tuple3[0]);

        DataInput in = new DataIO.DataInputByteArray(out.copyBytes());
        Object[] nn = tuple3_serializer.deserialize(in,v.size());

        assertArrayEquals(v.toArray(), nn);

    }

    final BTreeKeySerializer.Tuple4KeySerializer tuple4_serializer = new BTreeKeySerializer.Tuple4KeySerializer(
            Fun.COMPARATOR_NON_NULL, Fun.COMPARATOR_NON_NULL, Fun.COMPARATOR_NON_NULL, Fun.COMPARATOR_NON_NULL,
            Serializer.BASIC, Serializer.BASIC, Serializer.BASIC, Serializer.BASIC);

    @Test public void tuple4_simple() throws IOException {
        List<Fun.Tuple4<String,Integer, Double, Long>> v = new ArrayList<Fun.Tuple4<String, Integer, Double,Long>>();

        v.add(null);
        v.add(Fun.t4("aa",1,1D,1L));
        v.add(Fun.t4("aa",1,1D,2L));
        v.add(Fun.t4("aa",1,2D,2L));
        v.add(Fun.t4("aa",2,2D,2L));
        v.add(Fun.t4("aa",3,2D,2L));
        v.add(Fun.t4("aa",3,3D,2L));
        v.add(Fun.t4("zz",1,2D,2L));
        v.add(Fun.t4("zz",2,2D,2L));
        v.add(Fun.t4("zz",3,2D,2L));
        v.add(null);

        DataIO.DataOutputByteArray out = new DataIO.DataOutputByteArray();

        tuple4_serializer.serialize(out, new Fun.Tuple4[0]);

        DataInput in = new DataIO.DataInputByteArray(out.copyBytes());
        Object[] nn = tuple4_serializer.deserialize(in, v.size());

        assertArrayEquals(v.toArray(), nn);

    }
    @Test public void tuple4() throws IOException {
        List<Fun.Tuple4<String,Integer,Double, Long>> v = new ArrayList<Fun.Tuple4<String, Integer,Double,Long>>();

        v.add(null);

        for(String s: new String[]{"aa","bb","oper","zzz"}){
            for(int i = 2;i<1000;i+=i/2){
                for(double d = 2D;i<1000;i+=i/2){
                    for(long l = 3L;i<10000;i+=i/2){
                        v.add(Fun.t4(s,i,d,l));
                    }
                }
            }
        }

        v.add(null);

        DataIO.DataOutputByteArray out = new DataIO.DataOutputByteArray();
        tuple4_serializer.serialize(out, new Fun.Tuple4[0]);

        DataInput in = new DataIO.DataInputByteArray(out.copyBytes());
        Object[] nn = tuple4_serializer.deserialize(in, v.size());

        assertArrayEquals(v.toArray(), nn);

    }

    final BTreeKeySerializer.Tuple5KeySerializer tuple5_serializer = new BTreeKeySerializer.Tuple5KeySerializer(
            Fun.COMPARATOR_NON_NULL, Fun.COMPARATOR_NON_NULL, Fun.COMPARATOR_NON_NULL, Fun.COMPARATOR_NON_NULL,
Fun.COMPARATOR_NON_NULL,
            Serializer.BASIC, Serializer.BASIC, Serializer.BASIC, Serializer.BASIC, Serializer.BASIC);

    @Test public void tuple5_simple() throws IOException {
        List<Fun.Tuple5<String,Integer, Double, Long,String>> v = new ArrayList<Fun.Tuple5<String, Integer, Double,Long,String>>();

        v.add(null);
        v.add(Fun.t5("aa",1,1D,1L,"zz"));
        v.add(Fun.t5("aa",1,1D,2L,"zz"));
        v.add(Fun.t5("aa",1,2D,2L,"zz"));
        v.add(Fun.t5("aa",2,2D,2L,"zz"));
        v.add(Fun.t5("aa",3,2D,2L,"zz"));
        v.add(Fun.t5("aa",3,3D,2L,"zz"));
        v.add(Fun.t5("zz",1,2D,2L,"zz"));
        v.add(Fun.t5("zz",2,2D,2L,"zz"));
        v.add(Fun.t5("zz",3,2D,2L,"zz"));
        v.add(null);

        DataIO.DataOutputByteArray out = new DataIO.DataOutputByteArray();

        tuple5_serializer.serialize(out,new Fun.Tuple5[0]);

        DataInput in = new DataIO.DataInputByteArray(out.copyBytes());
        Object[] nn = tuple5_serializer.deserialize(in, v.size());

        assertArrayEquals(v.toArray(), nn);

    }
    @Test public void tuple5() throws IOException {
        List<Fun.Tuple5<String,Integer,Double, Long,String>> v = new ArrayList<Fun.Tuple5<String, Integer,Double,Long,String>>();

        v.add(null);

        String[] ss = new String[]{"aa","bb","oper","zzz"};
        for(String s: ss){
            for(int i = 2;i<1000;i+=i/2){
                for(double d = 2D;i<1000;i+=i/2){
                    for(long l = 3L;i<10000;i+=i/2){
                        for(String s2: ss){
                            v.add(Fun.t5(s,i,d,l,s2));
                        }
                    }
                }
            }
        }

        v.add(null);

        DataIO.DataOutputByteArray out = new DataIO.DataOutputByteArray();
        tuple5_serializer.serialize(out, new Fun.Tuple5[0]);

        DataInput in = new DataIO.DataInputByteArray(out.copyBytes());
        Object[] nn = tuple5_serializer.deserialize(in,v.size());

        assertArrayEquals(v.toArray(), nn);

    }

    final BTreeKeySerializer.Tuple6KeySerializer tuple6_serializer = new BTreeKeySerializer.Tuple6KeySerializer(
            Fun.COMPARATOR_NON_NULL, Fun.COMPARATOR_NON_NULL, Fun.COMPARATOR_NON_NULL, Fun.COMPARATOR_NON_NULL,
Fun.COMPARATOR_NON_NULL, Fun.COMPARATOR_NON_NULL,
            Serializer.BASIC, Serializer.BASIC, Serializer.BASIC, Serializer.BASIC, Serializer.BASIC, Serializer.BASIC);

    @Test public void tuple6_simple() throws IOException {
        List<Fun.Tuple6<String,Integer, Double, Long,String,String>> v = new ArrayList<Fun.Tuple6<String, Integer, Double,Long,String,String>>();

        v.add(null);
        v.add(Fun.t6("aa",1,1D,1L,"zz","asd"));
        v.add(Fun.t6("aa",1,1D,2L,"zz","asd"));
        v.add(Fun.t6("aa",1,2D,2L,"zz","asd"));
        v.add(Fun.t6("aa",2,2D,2L,"zz","asd"));
        v.add(Fun.t6("aa",3,2D,2L,"zz","asd"));
        v.add(Fun.t6("aa",3,3D,2L,"zz","asd"));
        v.add(Fun.t6("zz",1,2D,2L,"zz","asd"));
        v.add(Fun.t6("zz",2,2D,2L,"zz","asd"));
        v.add(Fun.t6("zz",3,2D,2L,"zz","asd"));
        v.add(null);

        DataIO.DataOutputByteArray out = new DataIO.DataOutputByteArray();

        tuple6_serializer.serialize(out, new Fun.Tuple6[0] );

        DataInput in = new DataIO.DataInputByteArray(out.copyBytes());
        Object[] nn = tuple6_serializer.deserialize(in,  v.size());

        assertArrayEquals(v.toArray(), nn);

    }
    @Test public void tuple6() throws IOException {
        List<Fun.Tuple6<String,Integer,Double, Long,String,String>> v = new ArrayList<Fun.Tuple6<String, Integer,Double,Long,String,String>>();

        v.add(null);

        String[] ss = new String[]{"aa","bb","oper","zzz","asd"};
        for(String s: ss){
            for(int i = 2;i<1000;i+=i/2){
                for(double d = 2D;i<1000;i+=i/2){
                    for(long l = 3L;i<10000;i+=i/2){
                        for(String s2: ss){
                            for(String s3: ss){
                                v.add(Fun.t6(s,i,d,l,s2,s3));
                            }
                        }
                    }
                }
            }
        }

        v.add(null);

        DataIO.DataOutputByteArray out = new DataIO.DataOutputByteArray();
        tuple6_serializer.serialize(out, new Fun.Tuple6[0]);

        DataInput in = new DataIO.DataInputByteArray(out.copyBytes());
        Object[] nn = tuple6_serializer.deserialize(in,  v.size());

        assertArrayEquals(v.toArray(), nn);

    }



}