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


    public static class BloomFilterMapper extends Mapper<Object, Text, Text, Filter> {
        private Filter ff1 ;
        private Filter ff2 ;
        private Filter ff3 ;
        private Filter ff4;
        private Filter ff5;
        private Filter ff6 ;
        private Filter ff7 ;
        private Filter ff8;
        private Filter ff9;
        private Filter ff10 ;

        public void setup(Context context) throws IOException, InterruptedException {
            //prendo i valori di m dalla conf del primo mapreduce
            //creo 10 bloom filter dimensionati
            Configuration conf = context.getConfiguration();
             ff1 = new Filter(Integer.parseInt(conf.get("m1.0")),Integer.parseInt(conf.get("n1.0")),0.01,1.0);
             ff2 = new Filter(Integer.parseInt(conf.get("m2.0")),Integer.parseInt(conf.get("n2.0")),0.01,2.0);
             ff3 = new Filter(Integer.parseInt(conf.get("m3.0")),Integer.parseInt(conf.get("n3.0")),0.01,3.0);
             ff4 = new Filter(Integer.parseInt(conf.get("m4.0")),Integer.parseInt(conf.get("n4.0")),0.01,4.0);
             ff5 = new Filter(Integer.parseInt(conf.get("m5.0")),Integer.parseInt(conf.get("n5.0")),0.01,5.0);
             ff6 = new Filter(Integer.parseInt(conf.get("m6.0")),Integer.parseInt(conf.get("n6.0")),0.01,6.0);
             ff7 = new Filter(Integer.parseInt(conf.get("m7.0")),Integer.parseInt(conf.get("n7.0")),0.01,7.0);
             ff8 = new Filter(Integer.parseInt(conf.get("m8.0")),Integer.parseInt(conf.get("n8.0")),0.01,8.0);
             ff9 = new Filter(Integer.parseInt(conf.get("m9.0")),Integer.parseInt(conf.get("n9.0")),0.01,9.0);
             ff10 = new Filter(Integer.parseInt(conf.get("m10.0")),Integer.parseInt(conf.get("n10.0")),0.01,10.0);
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String record1 = value.toString();
            String[] record = record1.split("\\s+");
            String movieId = record[0];
            String rating  = Double.toString(Math.round(Double.parseDouble(record[1])));
            if(rating.compareTo("1.0")==0){
                this.ff1.add(movieId);
            }else if (rating.compareTo("2.0")==0){
                this.ff2.add(movieId);
            }else if (rating.compareTo("3.0")==0){
                this.ff3.add(movieId);
            }else if (rating.compareTo("4.0")==0){
                this.ff4.add(movieId);
            }else if (rating.compareTo("5.0")==0){
                this.ff5.add(movieId);
            }else if (rating.compareTo("6.0")==0){
                this.ff6.add(movieId);
            }else if (rating.compareTo("7.0")==0){
                this.ff7.add(movieId);
            }else if (rating.compareTo("8.0")==0){
                this.ff8.add(movieId);
            }else if (rating.compareTo("9.0")==0){
                this.ff9.add(movieId);
            }else if (rating.compareTo("10.0")==0){
                this.ff10.add(movieId);
            }

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {



            // Write the filter to HDFS once all maps are finished

            context.write(new Text("1.0"),ff1);
            context.write(new Text("2.0"),ff2);
            context.write(new Text("3.0"),ff3);
            context.write(new Text("4.0"),ff4);
            context.write(new Text("5.0"),ff5);
            context.write(new Text("6.0"),ff6);
            context.write(new Text("7.0"),ff7);
            context.write(new Text("8.0"),ff8);
            context.write(new Text("9.0"),ff9);
            context.write(new Text("10.0"), ff10);


        }

    }





    public static class BloomFilterReducer extends Reducer<Text, Filter, Text, Text> {
        private Filter f1 ;
        private Filter f2 ;
        private Filter f3 ;
        private Filter f4;
        private Filter f5;
        private Filter f6 ;
        private Filter f7 ;
        private Filter f8;
        private Filter f9;
        private Filter f10 ;

        public void setup(Reducer.Context context) throws IOException, InterruptedException {
            //prendo i valori di m dalla conf del primo mapreduce
            //creo 10 bloom filter dimensionati
            Configuration conf = context.getConfiguration();
            f1 = new Filter(Integer.parseInt(conf.get("m1.0")),Integer.parseInt(conf.get("n1.0")),0.01,1.0);
            f2 = new Filter(Integer.parseInt(conf.get("m2.0")),Integer.parseInt(conf.get("n2.0")),0.01,2.0);
            f3 = new Filter(Integer.parseInt(conf.get("m3.0")),Integer.parseInt(conf.get("n3.0")),0.01,3.0);
            f4 = new Filter(Integer.parseInt(conf.get("m4.0")),Integer.parseInt(conf.get("n4.0")),0.01,4.0);
            f5 = new Filter(Integer.parseInt(conf.get("m5.0")),Integer.parseInt(conf.get("n5.0")),0.01,5.0);
            f6 = new Filter(Integer.parseInt(conf.get("m6.0")),Integer.parseInt(conf.get("n6.0")),0.01,6.0);
            f7 = new Filter(Integer.parseInt(conf.get("m7.0")),Integer.parseInt(conf.get("n7.0")),0.01,7.0);
            f8 = new Filter(Integer.parseInt(conf.get("m8.0")),Integer.parseInt(conf.get("n8.0")),0.01,8.0);
            f9 = new Filter(Integer.parseInt(conf.get("m9.0")),Integer.parseInt(conf.get("n9.0")),0.01,9.0);
            f10 = new Filter(Integer.parseInt(conf.get("m10.0")),Integer.parseInt(conf.get("n10.0")),0.01,10.0);
        }


        public void reduce(Text key, Iterable<Filter> values, Context context) throws IOException, InterruptedException {
            // Merge all filters by logical OR
            FileSystem fs = FileSystem.get(context.getConfiguration());


            if(key.toString().compareTo("1.0")==0){
                for (Filter value : values) {
                    f1.or(value);
                }
                context.write(key, new Text(f1.toString()));
            }else if(key.toString().compareTo("2.0")==0){
                for (Filter value : values) {
                    f2.or(value);
                }
                context.write(key, new Text(f2.toString()));
            }else if(key.toString().compareTo("3.0")==0){

                for (Filter value : values) {
                    f3.or(value);
                }
                context.write(key, new Text(f3.toString()));
            }else if(key.toString().compareTo("4.0")==0){

                for (Filter value : values) {
                    f4.or(value);
                }
                context.write(key, new Text(f4.toString()));
            }else if(key.toString().compareTo("5.0")==0){

                for (Filter value : values) {
                    f5.or(value);
                }
                context.write(key, new Text(f5.toString()));
            }else if(key.toString().compareTo("6.0")==0){

                for (Filter value : values) {
                    f6.or(value);
                }
                context.write(key, new Text(f6.toString()));
            }else if(key.toString().compareTo("7.0")==0){

                for (Filter value : values) {
                    f7.or(value);
                }
                context.write(key, new Text(f7.toString()));
            }else if(key.toString().compareTo("8.0")==0){
                for (Filter value : values) {
                    f8.or(value);
                }
                context.write(key, new Text(f8.toString()));
            }else if(key.toString().compareTo("9.0")==0){
                for (Filter value : values) {
                    f9.or(value);
                }
                context.write(key, new Text(f9.toString()));
            }else if(key.toString().compareTo("10.0")==0){

                for (Filter value : values) {
                    f10.or(value);
                }
                context.write(key, new Text(f10.toString()));
            }
        }
    }

}
