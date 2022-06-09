package it.unipi.hadoop;

import java.io.*;
import java.util.ArrayList;
import java.util.Scanner;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class CountingMR {

    public static class NewMapper extends Mapper<Object, Text, Text, Text> {
        private final Text reducerKey = new Text();
        private final Text reducerValue = new Text();

        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            String record = value.toString();
            String[] tokens = new String[2];
            tokens[0] = record.split("\t")[0];
            tokens[1] = record.split("\t")[1];
            tokens[1] = Double.toString(Math.round(Double.parseDouble(tokens[1])));

            reducerKey.set(tokens[1]);
            reducerValue.set(tokens[0]);
            context.write(reducerKey, reducerValue);
        }
    }



    public static class NewReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
            int n = 0,m;
            double pvalue = 0.01;

            for (final Text val : values) {
                n++;
            }
            m = (int) (-(n * Math.log(pvalue)) / (Math.pow(Math.log(2), 2))) + 1;



            String mstr = Integer.toString(m);


            String testo =  mstr ;
            Text result = new Text(testo);
            context.write(key, result);
        }
    }

    /* public static int[] pickDimension(ArrayList<String> list, int rating) {
         ArrayList<String> result = new ArrayList<String>();
         int[] mk = new int[2];
         String[] tokens = new String[4];
         for (int j = 0; j < list.size(); j++) {
             tokens[0] = result.get(j).split("\t")[0];
             if (Integer.parseInt(tokens[0]) == rating) {
                 mk[0] = Integer.parseInt(result.get(j).split("\t")[2]);
                 mk[1] = Integer.parseInt(result.get(j).split("\t")[3]);
                 return mk;
             }
         }
         return mk;
     }
 */
    public static ArrayList<String> pickFile(String stringPath) throws IOException {
        ArrayList<String> listValues = new ArrayList<>();
        Configuration conf = new Configuration();
        /*AGGIUNTA*/
        try {
            FileSystem fs = FileSystem.get(conf);
            InputStream stream = fs.open(new Path(stringPath));
            BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
            Scanner s = new Scanner(reader);
            while (s.hasNextLine()){
                String line = s.nextLine();
                listValues.add(line);
                reader.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        }
        return listValues;
    }


    public static void main(final String[] args) throws Exception {

        Configuration conf1=new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf1, args).getRemainingArgs();
        Job j1=Job.getInstance(conf1);
        j1.setJarByClass(CountingMR.class);
        j1.setMapperClass(NewMapper.class);
        j1.setReducerClass(NewReducer.class);

        j1.setMapOutputKeyClass(Text.class);
        j1.setMapOutputValueClass(Text.class);
        j1.setOutputKeyClass(Text.class);
        j1.setOutputValueClass(Text.class);
        Path outputPath=new Path("/user/Pepp/Counting");

        FileInputFormat.addInputPath(j1,new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(j1,outputPath);

        j1.waitForCompletion(true);

        //JOB2
        //Inserisco in un arraylist i valori in output dal primo reducer
        // rating   n   m   k
        ArrayList<String> listValues = new ArrayList<>();
        listValues= pickFile("hdfs:///user/Pepp/Counting/part-r-00000");
        //Creo array con tutti i valori di m dei 10 filtri
        //l'indice +1 equivale al rating
        int[] takeValues = new int[10];

        for (int i=0; i<listValues.size(); i++){
            String row = listValues.get(i);
            int index = (int)Double.parseDouble(String.valueOf(row.split("\t")[0]))-1;
            takeValues[index] = Integer.parseInt(row.split("\t")[1]);
        }



        Configuration conf2=new Configuration();
        Job j2=Job.getInstance(conf2);
        j2.setJarByClass(BloomFilter.class);
        j2.setMapperClass(BloomFilter.BloomFilterMapper.class);
        j2.setReducerClass(BloomFilter.BloomFilterReducer.class);
        j2.setInputFormatClass(NLineInputFormat.class);
        //j2.getConfiguration().setStrings("mapreduce.reduce.shuffle.memory.limit.percent", "0.15");
        NLineInputFormat.addInputPath(j2, new Path(otherArgs[0]));
        //N.tot =1,247,686      (8)  311922(4)
        j2.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 311922);
        j2.getConfiguration().setInt("m1.0",takeValues[0] );
        j2.getConfiguration().setInt("m2.0",takeValues[1] );
        j2.getConfiguration().setInt("m3.0",takeValues[2] );
        j2.getConfiguration().setInt("m4.0",takeValues[3] );
        j2.getConfiguration().setInt("m5.0",takeValues[4] );
        j2.getConfiguration().setInt("m6.0",takeValues[5] );
        j2.getConfiguration().setInt("m7.0",takeValues[6] );
        j2.getConfiguration().setInt("m8.0",takeValues[7] );
        j2.getConfiguration().setInt("m9.0",takeValues[8] );
        j2.getConfiguration().setInt("m10.0",takeValues[9] );


        //mapper
        j2.setMapOutputKeyClass(Text.class);
        j2.setMapOutputValueClass(BloomFilter.IntArrayWritable.class);
       //reducer
        j2.setOutputKeyClass(Text.class);
        j2.setOutputValueClass(Text.class);
        Path outputPath1=new Path("/user/Pepp/Bloom");
        FileOutputFormat.setOutputPath(j2, outputPath1);
        System.exit(j2.waitForCompletion(true)?0:1);

    }
}

