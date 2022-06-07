package it.unipi.hadoop;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CustomWritable extends Filter implements Writable{

    private String bit;

    public CustomWritable(int m, double pvalue) throws IOException {
        super(m, pvalue);
    }

    @Override
    public void write(DataOutput output) throws IOException {
        for (int i=0; i<bitArray.size(); i++){
            if (bitArray.get(i)) {
                bit += "1";
            }else{
                bit += "0";
            }
        }
        output.writeBytes(bit);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }
}
