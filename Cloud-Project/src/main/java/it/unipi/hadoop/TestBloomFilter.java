package it.unipi.hadoop;

import org.apache.hadoop.util.hash.MurmurHash;

import java.io.*;
import java.util.Scanner;
import java.util.ArrayList;
import java.util.List;

public class TestBloomFilter {
    //lista di filtri
    ArrayList<Integer>[] bloomFilters = new ArrayList[10];




    public void createBloomFromFile() throws IOException {
        // initializing
        for (int i = 0; i < 10; i++) {
            bloomFilters[i] = new ArrayList<Integer>();
        }
        //file generato dal secondo mapReduce
        //File myObj = new File("C:\\Users\\Domenico\\Desktop\\Cloud-Project-2nd-Impl\\Cloud-Project\\src\\main\\java\\it\\unipi\\hadoop\\part-r-00000.txt");
        //Scanner scannerValue = new Scanner(myObj);
        BufferedReader objReader = null;


        try {
            objReader = new BufferedReader(new FileReader("C:\\Users\\Domenico\\Desktop\\Cloud-Project-2nd-Impl\\Cloud-Project\\src\\main\\java\\it\\unipi\\hadoop\\part-r-00000"));
            String strCurrentLine = objReader.readLine();
            int i=0;
            while (strCurrentLine != null) {
                //String line = objReader.readLine();
                String myArray1[] = strCurrentLine.split("\t");
                String myArray2[] = myArray1[1].split(" ");
                for (int j = 0; j < myArray2.length; j++) {
                    //int index = (int) Double.parseDouble(myArray1[0]);
                    //this.bloomFilters[index-1].add(Integer.parseInt(myArray2[j]));
                    this.bloomFilters[i].add(Integer.parseInt(myArray2[j]));
                }
                i++;
                strCurrentLine = objReader.readLine();
            }
        }catch (IOException e){
            e.printStackTrace();
        } finally {

        try {
            if (objReader != null)
                objReader.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }}


        for(int l=0;l<10;l++){
            System.out.println("bloom "+l+" :"+"\n");
            for(int j=0;j<bloomFilters[l].size();j++){
                //System.out.println(bloomFilters[l].get(j)+" ");

            }
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
        /*
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
                    //if(isMemberBloom(this.bloomFilters[i],tokens[0])==1  ){
                     //   FP[ratingInt-1]++;
                   // }
                }

            }

        }


         */

        //TN
        //file generato dal primo mapreduce
        Scanner scannerResultReducer = new Scanner(new File("C:\\Users\\Domenico\\Desktop\\Cloud-Project-2nd-Impl\\Cloud-Project\\src\\main\\java\\it\\unipi\\hadoop\\part-r-00001"));
        int realValue[] = new int[10];
        int m=0;
        while (scannerResultReducer.hasNextLine()) {
            String line = scannerResultReducer.nextLine();
            String myArray1[] = line.split("\t");
            //int keyInt = Integer.parseInt(tokens[0]);
            realValue[m]=Integer.parseInt(myArray1[1]);
            m++;
        }

        //to implement
        int[] TN = new int[10];
        int tot = 1247687;
        for (int i=0;i<10;i++){
            TN[i]=tot-realValue[i];
        }

        //FPR=FP/(FP+TN)
        double fpr[] = new double[10];
        for(int i=0;i<10;i++){
            //fpr[i] = FP[i]/(FP[i]+TN[i]);
            //System.out.println(fpr[i]);
            System.out.println(TN[i]);
        }





    }
}
