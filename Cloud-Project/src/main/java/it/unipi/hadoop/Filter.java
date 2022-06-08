
package it.unipi.hadoop;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.bloom.HashFunction;
import org.apache.hadoop.util.hash.Hash;
import org.apache.hadoop.util.hash.MurmurHash;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.BitSet;
import java.util.stream.IntStream;


/* Class BloomFilter */

public class Filter implements Writable {

    public BitSet bitArray ;
    public int m;   // number of bit in the array
    public int k;   // number of hash function
    public int n;   // number of films to put in the filter of a certain rating
    public int hashType;
    public HashFunction hash;


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
    public Filter(int m, int n,double pvalue, double rating) throws IOException {
        this.m = m;
        this.n = n;
        k = (int) ((m / n) * Math.log(2)) + 1;
        this.bitArray = new BitSet(m);
    }

    public Filter(){
        super();
    }

    @Override
    public String toString() {
            final StringBuilder buffer = new StringBuilder(m);
            IntStream.range(0, m).mapToObj(i -> bitArray.get(i) ? '1' : '0').forEach(buffer::append);
            return buffer.toString();
    }


    /* Function to add an object */
    public void add(String idFilm) {
        MurmurHash hasher = new MurmurHash();
        //valore 7 perche hash = 7 per tutti i filters
        for(int i =0;i<7;i++){
            //Sets the bit at the specified index to true.
            int position = Math.abs(hasher.hash(idFilm.getBytes(),9,i*50)%m);
            this.bitArray.set(position,true);
        }
    }

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
    public void or(BitSet filter, int dimension) {
        if (this.m != dimension) {
            throw new IllegalArgumentException("These 2 filters cannot be merged!" + dimension + " " + this.m);
        }else {
            //this.bitArray.or(filter);
            for (int i=0; i<filter.size(); i++){
                if (filter.get(i)==true || this.bitArray.get(i) == true)
                    this.bitArray.set(i, true);
            }
        }
    }


    public void write(DataOutput out) throws IOException {
            out.writeInt(-1);
            out.writeInt(this.k);
            out.writeByte(Hash.MURMUR_HASH);
            out.writeInt(this.m);

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
        MurmurHash hasher = new MurmurHash();
        int ver = in.readInt();
        if (ver > 0) { // old unversioned format
            this.k = ver;
            hashType = Hash.MURMUR_HASH;
        } else if (ver == -1) {
            this.k = in.readInt();
            this.hashType = in.readByte();
        } else {
            throw new IOException("Unsupported version: " + ver);
        }
        this.m = in.readInt();
        this.hash = new HashFunction(this.m, this.k, this.hashType);
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

    private int getNBytes () {
        return (m + 7) / 8;
    }
}





