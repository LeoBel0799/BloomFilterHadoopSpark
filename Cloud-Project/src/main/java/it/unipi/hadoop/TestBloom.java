package it.unipi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.hash.Hash;
import org.apache.hadoop.util.hash.MurmurHash;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;


public class TestBloom {

    public static void testingFilters(Configuration conf, String inputFile, String pathBloomFilter, String pval, int[] takeValues) throws IOException {
        System.out.println("\n######### START TESTING... #########\n");
        FileSystem hadoop = FileSystem.get(conf);
        BufferedReader bloomFilterOutputReader= new BufferedReader(new InputStreamReader(hadoop.open(new Path(pathBloomFilter)))); //to read the filters
        BufferedReader inputFileReader = new BufferedReader(new InputStreamReader(hadoop.open(new Path(inputFile)))); //to read the dataset
        Hash hasher  = new MurmurHash();
        int k = (int) (-1*Math.log(Double.parseDouble(pval))/(Math.log(2))) + 1;
        double fp[] = new double[10];
        int numTotFilms = 0;
        int numFilmRating[] = new int[10];
        int[][] bloomFilters = createFiltersFromFile(takeValues);


        try {
            String line;
            line = bloomFilterOutputReader.readLine();
            while (line != null) {
                String[] rowKeyFilter = line.split("\t"); //key value split
                String rating = Double.toString(Math.round(Double.parseDouble(rowKeyFilter[0])));
                int index = Integer.parseInt(rating.split("\\.")[0]);
                String[] filter = rowKeyFilter[1].split(" ");
                for(int j = 0; j < filter.length; j++) {
                    bloomFilters[index-1][j] = Integer.parseInt(filter[j]);
                }
                line = bloomFilterOutputReader.readLine();
            }
            bloomFilterOutputReader.close();
        }
        catch (IOException e) {
            bloomFilterOutputReader.close();
            throw new RuntimeException(e);
        }

        try {
            String line;
            line = inputFileReader.readLine();
            while (line != null) {
                numTotFilms++;
                String[] rowIdRating = line.split("\t");
                String filmId = rowIdRating[0];
                int filmRating = (int) Math.round((Double.parseDouble(rowIdRating[1])));
                numFilmRating[filmRating - 1]++;
                Boolean notFoundZero;
                for(int i = 0; i < bloomFilters.length; i++) {
                    notFoundZero = true;
                    for (int j = 0; j < k; j++) {
                        int position = (hasher.hash(filmId.getBytes(), filmId.length(), j) % takeValues[i] + takeValues[i]) % takeValues[i];
                        if ((bloomFilters[i][position] != 1) && (i+1 != filmRating)) {
                            notFoundZero = false;
                            break;
                        }
                    }
                    if(notFoundZero && (i+1 != filmRating)){
                        fp[i]++;
                    }
                }
                line = inputFileReader.readLine();
            }
            inputFileReader.close();
        }
        catch (IOException e) {
            inputFileReader.close();
            throw new RuntimeException(e);
        }

        System.out.println("\n######### BLOOMFILTERS' TESTING RESULT #########\n");
        for(int i = 0; i < 10; i++) {
            //compute the false positive rate
            double fpr = fp[i] / (numTotFilms - numFilmRating[i]);
            int j = i + 1;
            System.out.println("Film rating:  " + j + " ---> Number of False positives =  " + fp[i] + " fpr --->  " + fpr  + "\n");
        }
        System.out.println("\n######### TEST ENDED #########\n");

    }

    private static int[][] createFiltersFromFile(int[] takeValues){
        int[][] bloomFilters = new int[10][];
        for (int i = 0; i < bloomFilters.length; ++i) {
            bloomFilters[i] = new int[takeValues[i]];
        }
        return bloomFilters;
    }
}
