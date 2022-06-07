
package it.unipi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.hash.Hash;
import org.apache.hadoop.util.hash.MurmurHash;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Scanner;
import org.apache.hadoop.util.hash.MurmurHash;


/* Class BloomFilter */

public class Filter implements Writable {

    public BitSet bitArray;
    public int m;   // number of bit in the array
    public int k;   // number of hash function
    public int n;   // number of films to put in the filter of a certain rating

    private static final byte[] bitvalues = new byte[] {
            (byte)0x01,
            (byte)0x02,
            (byte)0x04,
            (byte)0x08,
            (byte)0x10,
            (byte)0x20,
            (byte)0x40,
            (byte)0x80
    };

    /* Constructor */
    public Filter(int m, int n,double pvalue, int rating) throws IOException {
        this.m = m;
        this.n = n;
        k = (int) ((m / n) * Math.log(2)) + 1;
        this.bitArray = new BitSet(m);
    }


    /* Function to add an object */
    public void add(String idFilm) {
        MurmurHash hasher = new MurmurHash();
        //valore 7 perche hash = 7 per tutti i filters
        for(int i =0;i<7;i++){
            //Sets the bit at the specified index to true.
            int a = Math.abs(hasher.hash(idFilm.getBytes(),9,i*50));
            this.bitArray.set(((a)%m));
        }
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

    /*private int pickDimension(ArrayList<String> list, int rating) {
        int count = 0;
        String[] tokens = new String[2];
        for (int j = 0; j < list.size(); j++) {
            tokens[0] = list.get(j).split("\\s+")[0];
            tokens[1] = list.get(j).split("\t")[1];
            if (Integer.parseInt(tokens[0]) == rating) {
                count = Integer.parseInt(tokens[1]);
                return count;
            }
        }
        return count;
    }*/


    /* Function to check is an object is present */
    public boolean isMember(String idFilm) {
        int hashValue;
        MurmurHash hasher = new MurmurHash();
        for (int i = 0; i < k; i++) {
            hashValue = hasher.hash(idFilm.getBytes(), m, 50 * i);
            if (bitArray.get(hashValue) != true) {
                return false;
            }
        }
        return true;
    }


    /* Or for merging filters*/
    public void or(Filter filter) {
        if(filter == null || filter.m != this.m || filter.k != this.k) {
            throw new IllegalArgumentException("filters cannot be or-ed");
        }
        bitArray.or((filter).bitArray);
    }


    public void write(DataOutput out) throws IOException {
        byte[] bytes = new byte[getNBytes()];
        for(int i = 0, byteIndex = 0, bitIndex = 0; i < m; i++, bitIndex++) {
            if (bitIndex == 8) {
                bitIndex = 0;
                byteIndex++;
            }
            if (bitIndex == 0) {
                bytes[byteIndex] = 0;
            }
            if (bitArray.get(i)) {
                bytes[byteIndex] |= bitvalues[bitIndex];
            }
        }
        out.write(bytes);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        BitSet bits = new BitSet(this.m);
        byte[] bytes = new byte[getNBytes()];
        in.readFully(bytes);
        for (int i = 0, byteIndex = 0, bitIndex = 0; i < m; i++, bitIndex++) {
            if (bitIndex == 8) {
                bitIndex = 0;
                byteIndex++;
            }
            if ((bytes[byteIndex] & bitvalues[bitIndex]) != 0) {
                bits.set(i);
            }
        }
    }

    /* @return number of bytes needed to hold bit vector */
     private int getNBytes() {
        return (m + 7) / 8;
     }
}





