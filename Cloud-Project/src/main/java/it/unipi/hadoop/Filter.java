
package it.unipi.hadoop;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.hash.MurmurHash;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.io.Text;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Scanner;


/* Class BloomFilter */

public class Filter {

    public boolean[] bitArray;
    public int m;   // number of bit in the array
    public int k;   // number of hash function
    public int n;   // number of films to put in the filter of a certain rating


    /* Constructor */

    public Filter(int m, double pvalue) throws IOException {
        m = m;
        k = (int) ((m / n) * Math.log(2)) + 1;
        bitArray = new boolean[m];
    }

    public boolean[] getBitArray() {
        return bitArray;
    }

    public void setBitArray(boolean[] bitArray) {
        this.bitArray = bitArray;
    }

    /* Function to get size of objects added */

    public int getSize() {
        return m;
    }


    /* Function to add an object */

    public void add(String idFilm) {
        int hashValue;
        MurmurHash hasher = new MurmurHash();
        for (int i = 0; i < k; i++) {
            hashValue = hasher.hash(idFilm.getBytes(), m, 50 * i);
            bitArray[hashValue] = true;
        }
    }


    /* Function to check is an object is present */

    public boolean isMember(String idFilm) {
        int hashValue;
        MurmurHash hasher = new MurmurHash();
        for (int i = 0; i < k; i++) {
            hashValue = hasher.hash(idFilm.getBytes(), m, 50 * i);
            if (bitArray[hashValue] != true) {
                return false;
            }
        }
        return true;
    }


    /* Or for merging filters*/

    public boolean[] or(Filter filter) {
        if (filter != null && filter.m == this.m && filter.k == this.k) {
            for (int i = 0; i < filter.m; i++) {
                this.bitArray[i] = this.bitArray[i] || filter.bitArray[i];
            }
            filter = null;
        } else {
            throw new IllegalArgumentException("Impossible to merge filters");
        }
        return bitArray;
    }

    private ArrayList<String> pickFile(String stringPath) throws IOException {
        Configuration conf = new Configuration();
        String strLine;
        ArrayList<String> list = new ArrayList<>();
        try {
            Path inputFile = new Path(stringPath);
            FileSystem fs = FileSystem.get(inputFile.toUri(), conf);
            String line = null;
            Scanner fstream = new Scanner(String.valueOf(fs));

            if (!fs.exists(inputFile)) {
                System.out.println("Input file not found!");
                throw new IOException("Input file not found!");
            } else {
                while ((fstream.hasNext())) {
                    list.add(fstream.next());
                }

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return list;
    }

    private int pickDimension(ArrayList<String> list, int rating) {
        ArrayList<String> result = new ArrayList<String>();
        int count = 0;
        for (int i = 0; i < list.size(); i++) {
            result.add(list.get(i));
        }
        String[] tokens = new String[2];
        for (int j = 0; j < result.size(); j++) {
            tokens[0] = result.get(j).split("\t")[0];
            tokens[1] = result.get(j).split("\t")[1];
            if (Integer.parseInt(tokens[0]) == rating) {
                count = Integer.parseInt(tokens[1]);
                return count;
            }
        }
        return count;
    }

    public String printString(){
        String result = "";
        for(boolean tmp : bitArray){
            if(tmp == true)
                result += "1";
            else
                result += "0";
        }
        return result;
    }

}




