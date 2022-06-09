package it.unipi.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.hash.MurmurHash;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;


public class BloomFilter {


    public static class BloomFilterMapper extends Mapper<Object, Text, Text, IntArrayWritable> {
        private IntWritable[] ff1 ;
        private IntWritable[] ff2 ;
        private IntWritable[] ff3 ;
        private IntWritable[] ff4;
        private IntWritable[] ff5;
        private IntWritable[] ff6 ;
        private IntWritable[] ff7 ;
        private IntWritable[] ff8;
        private IntWritable[] ff9;
        private IntWritable[] ff10 ;

        IntArrayWritable f1;
        IntArrayWritable f2;
        IntArrayWritable f3;
        IntArrayWritable f4;
        IntArrayWritable f5;
        IntArrayWritable f6;
        IntArrayWritable f7;
        IntArrayWritable f8;
        IntArrayWritable f9;
        IntArrayWritable f10;


        public void setup(Context context) throws IOException, InterruptedException {
            //prendo i valori di m dalla conf del primo mapreduce
            //creo 10 bloom filter dimensionati
            Configuration conf = context.getConfiguration();
            ff1  = new IntWritable[Integer.parseInt(conf.get("m1.0"))];
            for (int i =0; i<Integer.parseInt(conf.get("m1.0"));i++ ){
                ff1[i]= new IntWritable(0);
            }

            ff2  = new IntWritable[Integer.parseInt(conf.get("m2.0"))];
            for (int i =0; i<Integer.parseInt(conf.get("m2.0"));i++ ){
                ff2[i]= new IntWritable(0);
            }

            ff3  = new IntWritable[Integer.parseInt(conf.get("m3.0"))];
            for (int i =0; i<Integer.parseInt(conf.get("m3.0"));i++ ){
                ff3[i]= new IntWritable(0);
            }

            ff4  = new IntWritable[Integer.parseInt(conf.get("m4.0"))];
            for (int i =0; i<Integer.parseInt(conf.get("m4.0"));i++ ){
                ff4[i]= new IntWritable(0);
            }

            ff5  = new IntWritable[Integer.parseInt(conf.get("m5.0"))];
            for (int i =0; i<Integer.parseInt(conf.get("m5.0"));i++ ){
                ff5[i]= new IntWritable(0);
            }
            ff6  = new IntWritable[Integer.parseInt(conf.get("m6.0"))];
            for (int i =0; i<Integer.parseInt(conf.get("m6.0"));i++ ){
                ff6[i]= new IntWritable(0);
            }

            ff7  = new IntWritable[Integer.parseInt(conf.get("m7.0"))];
            for (int i =0; i<Integer.parseInt(conf.get("m7.0"));i++ ){
                ff7[i]= new IntWritable(0);
            }

            ff8  = new IntWritable[Integer.parseInt(conf.get("m8.0"))];
            for (int i =0; i<Integer.parseInt(conf.get("m8.0"));i++ ){
                ff8[i]= new IntWritable(0);
            }

            ff9  = new IntWritable[Integer.parseInt(conf.get("m9.0"))];
            for (int i =0; i<Integer.parseInt(conf.get("m9.0"));i++ ){
                ff9[i]= new IntWritable(0);
            }

            ff10 = new IntWritable[Integer.parseInt(conf.get("m10.0"))];
            for (int i =0; i<Integer.parseInt(conf.get("m10.0"));i++ ){
                ff10[i]= new IntWritable(0);
            }

            f1 = new IntArrayWritable();
            f2 = new IntArrayWritable();
            f3 = new IntArrayWritable();
            f4 = new IntArrayWritable();
            f5 = new IntArrayWritable();
            f6 = new IntArrayWritable();
            f7 = new IntArrayWritable();
            f8 = new IntArrayWritable();
            f9 = new IntArrayWritable();
            f10 = new IntArrayWritable();
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String record1 = value.toString();
            String[] record = record1.split("\\s+");
            String movieId = record[0];
            String rating  = Double.toString(Math.round(Double.parseDouble(record[1])));
            Configuration conf = context.getConfiguration();

            if(rating.compareTo("1.0")==0){
                add(ff1, movieId,Integer.parseInt(conf.get("m"+rating)));
                f1.set(ff1);
            }else if (rating.compareTo("2.0")==0){
                add(ff2, movieId,Integer.parseInt(conf.get("m"+rating)));
                f2.set(ff2);
            }else if (rating.compareTo("3.0")==0){
                add(ff3, movieId,Integer.parseInt(conf.get("m"+rating)));
                f3.set(ff3);
            }else if (rating.compareTo("4.0")==0){
                add(ff4, movieId,Integer.parseInt(conf.get("m"+rating)));
                f4.set(ff4);
            }else if (rating.compareTo("5.0")==0){
                add(ff5, movieId,Integer.parseInt(conf.get("m"+rating)));
                f5.set(ff5);
            }else if (rating.compareTo("6.0")==0){
                add(ff6, movieId,Integer.parseInt(conf.get("m"+rating)));
                f6.set(ff6);
            }else if (rating.compareTo("7.0")==0){
                add(ff7, movieId,Integer.parseInt(conf.get("m"+rating)));
                f7.set(ff7);
            }else if (rating.compareTo("8.0")==0){
                add(ff8, movieId,Integer.parseInt(conf.get("m"+rating)));
                f8.set(ff8);
            }else if (rating.compareTo("9.0")==0){
                add(ff9, movieId,Integer.parseInt(conf.get("m"+rating)));
                f9.set(ff9);
            }else if (rating.compareTo("10.0")==0){
                add(ff10, movieId,Integer.parseInt(conf.get("m"+rating)));
                f10.set(ff10);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {


            context.write(new Text("1.0"),f1);
            context.write(new Text("2.0"),f2);
            context.write(new Text("3.0"),f3);
            context.write(new Text("4.0"),f4);
            context.write(new Text("5.0"),f5);
            context.write(new Text("6.0"),f6);
            context.write(new Text("7.0"),f7);
            context.write(new Text("8.0"),f8);
            context.write(new Text("9.0"),f9);
            context.write(new Text("10.0"),f10);
        }

    }
    public static void add (IntWritable[] newFilter, String movieId, int dimension){

        MurmurHash hasher = new MurmurHash();
        byte[] bytearr = movieId.getBytes();
        int k=7;

        for (int i = 0; i < k; i++) {
            int position = hasher.hash(bytearr, 9,  i*50) % dimension;
            if (position < 0) {
               position = Math.abs(position) ;
               newFilter[position].set(1);

            } else {
                newFilter[position].set(1);
            }
        }
    }

    public static class IntArrayWritable extends ArrayWritable {
        public IntArrayWritable() {
            super(IntWritable.class);
        }


        public String toString() {
            StringBuilder sb = new StringBuilder("");

            for (String s : super.toStrings()) {
                sb.append(s).append(" ");
            }

            // sb.append("]");
            return sb.toString();
        }
    }

    public static class BloomFilterReducer extends Reducer<Text, IntArrayWritable, Text, Text> {

        public void reduce(Text key, Iterable<IntArrayWritable> values, Context context) throws IOException, InterruptedException {
            // Merge all filters by logical OR
            Configuration conf = context.getConfiguration();
            IntArrayWritable finalArray = new IntArrayWritable();

            IntWritable[] bitArray = new IntWritable[Integer.parseInt(conf.get("m"+key))];
            for (int i=0; i<bitArray.length;i++){
                bitArray[i]=new IntWritable(0);
            }
            for(IntArrayWritable tmp : values){
                or(bitArray,tmp);
            }
            finalArray.set(bitArray);
            context.write(key, new Text(finalArray.toString()));
       }
    }

    public static void or( IntWritable[] finalArray, IntArrayWritable array){

        for( int i =0 ; i<finalArray.length;i++){
           // int value = Integer.parseInt(array.toString().split(" ")[i]);
            int value = Integer.parseInt(array.get()[i].toString());
            if(value == 1)
                finalArray[i].set(1);
        }

    }


}
