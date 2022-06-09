package it.unipi.hadoop;

import org.apache.hadoop.util.hash.MurmurHash;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Path;
import java.util.Scanner;

public class TestBloomFilter {
    public void test (Filter[] filters) throws FileNotFoundException {


        //FP
        MurmurHash hasher = new MurmurHash();
        //apro file e itero le linee
        Scanner scannerFilm = new Scanner(new File("C:\\Users\\Domenico\\Desktop\\input.txt"));
        int FP[] = new int[10];
        for (int j=0;j<10;j++){
            FP[j]=0;
        }
        while (scannerFilm.hasNextLine()) {
            // process the line
            String line = scannerFilm.nextLine();
            String[] tokens = new String[2];
            tokens[0] = line.split("\t")[0]; //id_film
            tokens[1] = line.split("\t")[1]; //rating
            tokens[1] = Double.toString(Math.round(Double.parseDouble(tokens[1])));
            //converto per ricavare il filtro
            int ratingInt = Integer.parseInt(tokens[1]);
            //testo su tutti i filtri tranne quello a cui appartiene davvero
            for(int i=0;i<9;i++){
                if((ratingInt-1)!=i){
                    if(filters[i].isMember(tokens[0])==true  ){
                        FP[ratingInt-1]++;
                    }
                }

            }

        }


        //TN
        Scanner scannerResultReducer = new Scanner(new File("C:\\Users\\Domenico\\Desktop\\demofile.txt"));
        int realValue[] = new int[10];
        while (scannerResultReducer.hasNextLine()) {
            String line = scannerFilm.nextLine();
            String[] tokens = new String[2];
            tokens[0] = line.split("\t")[0]; //key
            tokens[1] = line.split("\t")[1]; //number of film in associated rating
            int keyInt = Integer.parseInt(tokens[0]);
            realValue[keyInt]=Integer.parseInt(tokens[1]);
        }
        int[] TN = new int[10];
        int tot = 1247687;
        for (int i=0;i<10;i++){
            TN[i]=tot-realValue[i];
        }

        //FPR=FP/(FP+TN)
        double fpr[] = new double[10];
        for(int i=0;i<10;i++){
            fpr[i] = FP[i]/(FP[i]+TN[i]);
            System.out.println(fpr[i]);
        }





    }
}
