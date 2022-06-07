
package it.unipi.hadoop;

import org.apache.hadoop.util.hash.MurmurHash;
import java.io.IOException;
import java.util.BitSet;


/* Class BloomFilter */

public class Filter{

    public BitSet bitArray;
    public int m;   // number of bit in the array
    public int k;   // number of hash function
    public int n;   // number of films to put in the filter of a certain rating


    /* Constructor */

    public Filter(int m, double pvalue) throws IOException {
        this.m = m;
        k = (int) ((m / n) * Math.log(2)) + 1;
        bitArray = new BitSet(m);
    }


    /* Function to get size of objects added */




    /* Function to add an object */

    public void add(String idFilm) {
        int hashValue;
        MurmurHash hasher = new MurmurHash();
        for (int i = 0; i < k; i++) {
            hashValue = hasher.hash(idFilm.getBytes(), m, 50 * i);
            bitArray.set(hashValue,true);
        }
    }


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
    public Boolean or(Filter filter) {
        Boolean bit = false;//this.bitArray.get(i);

        if (filter != null && filter.m == this.m && filter.k == this.k) {
            for (int i = 0; i < filter.m; i++) {
                bit  = this.bitArray.get(i) || filter.bitArray.get(i);
            }
        } else {
            throw new IllegalArgumentException("Impossible to merge filters");
        }
        return bit;
    }
}





