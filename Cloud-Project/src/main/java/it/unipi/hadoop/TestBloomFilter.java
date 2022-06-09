package it.unipi.hadoop;

import org.apache.hadoop.util.hash.MurmurHash;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class TestBloomFilter {
    //lista di filtri
    List<int []> bloomFilters = new ArrayList<int []>();



    //to implement
    public void createBloomFromFile(Path path) throws FileNotFoundException {
        //file generato dal secondo mapReduce
        Scanner scannerValue = new Scanner(new File("C:\\Users\\Domenico\\Desktop\\input.txt"));
        for(int nFiltro=0;nFiltro<10;nFiltro++){
            
        }


    }

    public int isMemberBloom(int filter[], String id_film ){
        int hashValue;
        MurmurHash hasher = new MurmurHash();
        for (int i = 0; i < 7; i++) {
            hashValue = hasher.hash(id_film.getBytes(), filter.length, 50 * i);
            if (filter[hashValue] != 1) {
                return 0;
            }
        }
        return 1;
    }



    public void test () throws FileNotFoundException {
        //FP
        MurmurHash hasher = new MurmurHash();
        //file input
        Scanner scannerFilm = new Scanner(new File("C:\\Users\\Domenico\\Desktop\\input.txt"));
        //setto a 0 i vari Fp
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
                    if(isMemberBloom(this.bloomFilters.get(i),tokens[0])==1  ){
                        FP[ratingInt-1]++;
                    }
                }

            }

        }


        //TN
        //file generato dal primo mapreduce
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
