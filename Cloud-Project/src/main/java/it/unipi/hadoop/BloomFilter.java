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


public class BloomFilter
{


    public static class BloomFilterMapper extends Mapper<Object, Text, Text, Filter>
    {
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
            f2 = new Filter(Integer.parseInt(conf.get("m1")),0.01);
            f2 = new Filter(Integer.parseInt(conf.get("m2")),0.01);
            f3 = new Filter(Integer.parseInt(conf.get("m3")),0.01);
            f4 = new Filter(Integer.parseInt(conf.get("m4")),0.01);
            f5 = new Filter(Integer.parseInt(conf.get("m5")),0.01);
            f6 = new Filter(Integer.parseInt(conf.get("m6")),0.01);
            f7 = new Filter(Integer.parseInt(conf.get("m7")),0.01);
            f8 = new Filter(Integer.parseInt(conf.get("m8")),0.01);
            f9 = new Filter(Integer.parseInt(conf.get("m9")),0.01);
            f10 = new Filter(Integer.parseInt(conf.get("m10")),0.01);
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

        }

    }





    public static class BloomFilterReducer extends Reducer<Text, Filter, Text, Object> {
        private static String FILTER_OUTPUT_FILE_CONF = "bloomfilter.output.file";

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

        public void setup(Context context) throws IOException, InterruptedException {
            //prendo i valori di m dalla conf del primo mapreduce
            //creo 10 bloom filter dimensionati
            Configuration conf = context.getConfiguration();
            f1 = new Filter(Integer.parseInt(conf.get("m1")),0.01);
            f2 = new Filter(Integer.parseInt(conf.get("m2")),0.01);
            f3 = new Filter(Integer.parseInt(conf.get("m3")),0.01);
            f4 = new Filter(Integer.parseInt(conf.get("m4")),0.01);
            f5 = new Filter(Integer.parseInt(conf.get("m5")),0.01);
            f6 = new Filter(Integer.parseInt(conf.get("m6")),0.01);
            f7 = new Filter(Integer.parseInt(conf.get("m7")),0.01);
            f8 = new Filter(Integer.parseInt(conf.get("m8")),0.01);
            f9 = new Filter(Integer.parseInt(conf.get("m9")),0.01);
            f10 = new Filter(Integer.parseInt(conf.get("m10")),0.01);
        }


        public void reduce(Text key, Iterable<Filter> values, Context context) throws IOException, InterruptedException {
            // Merge all filters by logical OR
            Path outputFilePath = new Path(context.getConfiguration().get(FILTER_OUTPUT_FILE_CONF));
            FileSystem fs = FileSystem.get(context.getConfiguration());


            if(key.toString()=="1"){
                for (Filter value : values) {
                    f1.or(value);
                }
                context.write(key, f1);
            }else if(key.toString()=="2"){
                for (Filter value : values) {
                    f2.or(value);
                }
                context.write(key, f2);
            }else if(key.toString()=="3"){

                for (Filter value : values) {
                    f3.or(value);
                }
                context.write(key, f3);
            }else if(key.toString()=="4"){

                for (Filter value : values) {
                    f4.or(value);
                }
                context.write(key, f4);
            }else if(key.toString()=="5"){

                for (Filter value : values) {
                    f5.or(value);
                }
                context.write(key, f5);
            }else if(key.toString()=="6"){

                for (Filter value : values) {
                    f6.or(value);
                }
                context.write(key, f6);
            }else if(key.toString()=="7"){

                for (Filter value : values) {
                    f7.or(value);
                }
                context.write(key, f7);
            }else if(key.toString()=="8"){
                for (Filter value : values) {
                    f8.or(value);
                }
                context.write(key, f8);
            }else if(key.toString()=="9"){
                for (Filter value : values) {
                    f9.or(value);
                }
                context.write(key, f9);
            }else if(key.toString()=="10"){

                for (Filter value : values) {
                    f10.or(value);
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
