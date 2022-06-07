package it.unipi.hadoop;

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


public class BloomFilter
{


    public static class BloomFilterMapper extends Mapper<Object, Text, Text, Object>
    {
        Filter[] arrayf = new Filter[10];
        Filter f1 ;
        Filter f2 ;
        Filter f3 ;
        Filter f4;
        Filter f5;
        Filter f6 ;
        Filter f7 ;
        Filter f8;
        Filter f9;
        Filter f10 ;
        public void setup(Context context) throws IOException, InterruptedException
        {
            //prendo i valori di m dalla conf del primo mapreduce
            //creo 10 bloom filter dimensionati
            Configuration conf = context.getConfiguration();
            Filter f1 = new Filter(Integer.parseInt(conf.get("m1")),0.01);
            Filter f2 = new Filter(Integer.parseInt(conf.get("m2")),0.01);
            Filter f3 = new Filter(Integer.parseInt(conf.get("m3")),0.01);
            Filter f4 = new Filter(Integer.parseInt(conf.get("m4")),0.01);
            Filter f5 = new Filter(Integer.parseInt(conf.get("m5")),0.01);
            Filter f6 = new Filter(Integer.parseInt(conf.get("m6")),0.01);
            Filter f7 = new Filter(Integer.parseInt(conf.get("m7")),0.01);
            Filter f8 = new Filter(Integer.parseInt(conf.get("m8")),0.01);
            Filter f9 = new Filter(Integer.parseInt(conf.get("m9")),0.01);
            Filter f10 = new Filter(Integer.parseInt(conf.get("m10")),0.01);
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String record1 = value.toString();
            String[] record = record1.split("\\s+");
            String movieId = record[0];
            String rating  = Double.toString(Math.round(Double.parseDouble(record[1])));
            if(rating=="1"){
                this.f1.add(movieId);
                Text testo = new Text(rating);
                context.write(testo, f1);
            }else if (rating=="2"){
                this.f2.add(movieId);
                Text testo = new Text(rating);
                context.write(testo, f2);
            }else if (rating=="3"){
                this.f3.add(movieId);
                Text testo = new Text(rating);
                context.write(testo, f3);
            }else if (rating=="4"){
                this.f4.add(movieId);
                Text testo = new Text(rating);
                context.write(testo, f4);
            }else if (rating=="5"){
                this.f5.add(movieId);
                Text testo = new Text(rating);
                context.write(testo, f5);
            }else if (rating=="6"){
                this.f6.add(movieId);
                Text testo = new Text(rating);
                context.write(testo, f6);
            }else if (rating=="7"){
                this.f7.add(movieId);
                Text testo = new Text(rating);
                context.write(testo, f7);
            }else if (rating=="8"){
                this.f8.add(movieId);
                Text testo = new Text(rating);
                context.write(testo, f8);
            }else if (rating=="9"){
                this.f9.add(movieId);
                Text testo = new Text(rating);
                context.write(testo, f9);
            }else{
                this.f10.add(movieId);
                Text testo = new Text(rating);
                context.write(testo, f10);
            }


            /*
            Configuration conf = context.getConfiguration();
            MurmurHash hasher = new MurmurHash();
            String record1 = value.toString();

            String[] record = record1.split("\\s+");
            String movieId = record[0];
            String rating  = Double.toString(Math.round(Double.parseDouble(record[1])));

            int k=7;
            IntWritable[] posValue = new IntWritable[k];
            IntArrayWritable array = new IntArrayWritable();
            String name = "m" + rating;
            byte[] bytearr = movieId.getBytes();
            for (int i=0 ; i<k; i++){
                int a = hasher.hash(bytearr, 9, 50 * i)% Integer.parseInt(conf.get(name));
                if (a<0){
                    posValue[i] = new IntWritable(Integer.parseInt(conf.get(name)) + a);
                }else{
                    IntWritable valueh = new IntWritable(a);
                    posValue[i] = valueh;
                }
            }
            array.set(posValue);
            Text testo = new Text(rating);
            context.write(testo, array);
             */
        }

    }

    public static class IntArrayWritable extends ArrayWritable {
        public IntArrayWritable() {
            super(IntWritable.class);
        }
    }

    public static class BooleanArrayWritable extends ArrayWritable {
        public BooleanArrayWritable() {
            super(BooleanWritable.class);
        }
    }

    /*
    public void reduce(Text key, Iterable<ArrayWritable> posValue, Context context) throws IOException, InterruptedException {
            int m;
            //m = pickDimension(pickFile("hdfs:///user/Pepp/output/output1"),(int) Double.parseDouble(key.toString()))[1];
            Configuration conf = context.getConfiguration();
            String name = "m" + key;
            BooleanWritable[] bitArray = new BooleanWritable[(int) Double.parseDouble(conf.get(name))];
            ArrayWritable array = new ArrayWritable(BooleanWritable.class);
            for(ArrayWritable tmp : posValue){
                for(int i=0;i<tmp.get().length; i++){
                    String[] arraypos = (String[]) tmp.toArray();
                    Integer position = (int)Double.parseDouble(String.valueOf(arraypos[i]));
                    bitArray[position].set(true);
                }
            }
            array.set(bitArray);
            context.write(key, array);
        }
     */

    /*
     public void reduce(Text key, Iterable<ArrayWritable> posValue, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String name = "m" + key;
            BooleanWritable[] bitArray = new BooleanWritable[(int) Double.parseDouble(conf.get(name))];
            ArrayWritable tmp = new ArrayWritable();
            while (posValue.iterator().hasNext()){
                tmp = posValue.iterator().next();
            }
            String[] arraypos = (String[]) tmp.toArray();
            for(int i=0;i<tmp.get().length; i++){
                Integer position = (int)Double.parseDouble(String.valueOf(arraypos[i]));
                bitArray[position].set(true);
            }
            ArrayWritable array = new ArrayWritable(BooleanWritable.class);
            array.set(bitArray);
            context.write(key, array);
        }
     */

    public static class BloomFilterReducer extends Reducer<Text, Object, Text, Object> {
        private static String FILTER_OUTPUT_FILE_CONF = "bloomfilter.output.file";
        Filter[] arrayf = new Filter[10];
        Filter f1 ;
        Filter f2 ;
        Filter f3 ;
        Filter f4;
        Filter f5;
        Filter f6 ;
        Filter f7 ;
        Filter f8;
        Filter f9;
        Filter f10 ;
        public void setup(Context context) throws IOException, InterruptedException
        {
            //prendo i valori di m dalla conf del primo mapreduce
            //creo 10 bloom filter dimensionati
            Configuration conf = context.getConfiguration();
            Filter f1 = new Filter(Integer.parseInt(conf.get("m1")),0.01);
            Filter f2 = new Filter(Integer.parseInt(conf.get("m2")),0.01);
            Filter f3 = new Filter(Integer.parseInt(conf.get("m3")),0.01);
            Filter f4 = new Filter(Integer.parseInt(conf.get("m4")),0.01);
            Filter f5 = new Filter(Integer.parseInt(conf.get("m5")),0.01);
            Filter f6 = new Filter(Integer.parseInt(conf.get("m6")),0.01);
            Filter f7 = new Filter(Integer.parseInt(conf.get("m7")),0.01);
            Filter f8 = new Filter(Integer.parseInt(conf.get("m8")),0.01);
            Filter f9 = new Filter(Integer.parseInt(conf.get("m9")),0.01);
            Filter f10 = new Filter(Integer.parseInt(conf.get("m10")),0.01);
        }


        public void reduce(Text key, Iterable<Object> values, Context context) throws IOException, InterruptedException {
            // Merge all filters by logical OR
            Path outputFilePath = new Path(context.getConfiguration().get(FILTER_OUTPUT_FILE_CONF));
            FileSystem fs = FileSystem.get(context.getConfiguration());


            if(key.toString()=="1"){
                for (Object value : values) {
                    f1.or((Filter) value);
                }
                context.write(key, f1);
            }else if(key.toString()=="2"){
                for (Object value : values) {
                    f2.or((Filter) value);
                }
                context.write(key, f2);
            }else if(key.toString()=="3"){

                for (Object value : values) {
                    f3.or((Filter) value);
                }
                context.write(key, f3);
            }else if(key.toString()=="4"){

                for (Object value : values) {
                    f4.or((Filter) value);
                }
                context.write(key, f4);
            }else if(key.toString()=="5"){

                for (Object value : values) {
                    f5.or((Filter) value);
                }
                context.write(key, f5);
            }else if(key.toString()=="6"){

                for (Object value : values) {
                    f6.or((Filter) value);
                }
                context.write(key, f6);
            }else if(key.toString()=="7"){

                for (Object value : values) {
                    f7.or((Filter) value);
                }
                context.write(key, f7);
            }else if(key.toString()=="8"){

                for (Object value : values) {
                    f8.or((Filter) value);
                }
                context.write(key, f8);
            }else if(key.toString()=="9"){

                for (Object value : values) {
                    f9.or((Filter) value);
                }
                context.write(key, f9);
            }else if(key.toString()=="10"){

                for (Object value : values) {
                    f10.or((Filter) value);
                }
                context.write(key, f10);
            }





            /*
            BooleanArrayWritable array = new BooleanArrayWritable();
            ArrayList<IntArrayWritable> finalList = new ArrayList<>();
            while (posValue.iterator().hasNext()){
                finalList.add(posValue.iterator().next());
            }

            Configuration conf = context.getConfiguration();
            String name = "m" + key;
            BooleanWritable[] bitArray = new BooleanWritable[Integer.parseInt(conf.get(name))];


            for(IntArrayWritable tmp : finalList) {
                for (int i = 0; i < tmp.get().length; i++) {
                    int position = Integer.parseInt(tmp.get()[i].toString());
                    bitArray[position] = new BooleanWritable(true);
                }
            }





            for(int i=0;i<=bitArray.length;i++){
                if(bitArray[i].get())
                this.arrayValori
            }

            array.set(bitArray);
            context.write(key, array);
            */
        }
    }

}
