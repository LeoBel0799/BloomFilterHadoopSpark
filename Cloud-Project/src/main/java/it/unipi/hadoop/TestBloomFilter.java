package it.unipi.hadoop;

import org.apache.hadoop.util.hash.MurmurHash;

import java.io.*;
import java.text.DecimalFormat;
import java.util.Scanner;
import java.util.ArrayList;

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
            objReader = new BufferedReader(new FileReader("hdfs:///user/Pepp/Bloom/part-r-00000"));
            String strCurrentLine = objReader.readLine();
            //int i=0;
            while (strCurrentLine != null) {
                //String line = objReader.readLine();
                String myArray1[] = strCurrentLine.split("\t");
                String myArray2[] = myArray1[1].split(" ");
                for (int j = 0; j < myArray2.length; j++) {
                    int index = (int) Double.parseDouble(myArray1[0]);
                    this.bloomFilters[index - 1].add(Integer.parseInt(myArray2[j]));
                    //this.bloomFilters[i].add(Integer.parseInt(myArray2[j]));
                }
                //i++;
                strCurrentLine = objReader.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {

            try {
                if (objReader != null)
                    objReader.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }


        for (int l = 0; l < 10; l++) {
            System.out.println("bloom " + l + " :" + "\n");
            for (int j = 0; j < bloomFilters[l].size(); j++) {
                System.out.println(bloomFilters[l].get(j)+" ");

            }
        }
    }

    public int isMemberBloom(ArrayList<Integer> filter, String id_film) {
        int hashValue;
        MurmurHash hasher = new MurmurHash();
        for (int i = 0; i < 7; i++) {
            hashValue = (hasher.hash(id_film.getBytes(), 9, 50 * i));

            if (filter.get(Math.abs(hashValue)%filter.size()) != 1) {
                return 0;
            }
        }
        return 1;
    }



    public double[] test() throws IOException {
        this.createBloomFromFile();
        //file generato dal primo mapreduce
        Scanner scannerResultReducer = new Scanner(new File("hdfs:///user/Pepp/Counting/part-r-00000"));
        int realValue[] = new int[10];
        while (scannerResultReducer.hasNextLine()) {
            String line = scannerResultReducer.nextLine();
            String myArray1[] = line.split("\t");
            int keyInt =(int) Math.round(Double.parseDouble(myArray1[0]));
            realValue[keyInt-1] = Integer.parseInt(myArray1[1]);
        }
        for(int y=0;y<realValue.length;y++){
            System.out.println("valore :" + realValue[y]);
        }

        //FP
        MurmurHash hasher = new MurmurHash();
        //file input
        Scanner scannerFilm = new Scanner(new File("hdfs:///user/Pepp/data.tsv"));
        //setto a 0 i vari Fp
        int FP[] = new int[10];
        for (int j = 0; j < 10; j++) {
            FP[j] = 0;
        }
        String line0 = scannerFilm.nextLine();
        while (scannerFilm.hasNextLine()) {
            // process the line
            String line = scannerFilm.nextLine();
            String myArray[] = line.split("\t");
            //converto per ricavare il filtro
            int ratingInt =(int) Math.round(Double.parseDouble(myArray[1]));

            //testo su tutti i filtri tranne quello a cui appartiene davvero
            for (int i = 0; i < 9; i++) {
                if ((ratingInt - 1) != i) {
                    if (isMemberBloom(this.bloomFilters[i], myArray[0]) == 1) {
                        FP[ratingInt - 1]++;
                    }
                }

            }
        }

        System.out.println("\n\n");

        for(int y=0;y<FP.length;y++){
            System.out.println("FP :" + y +" = "+ FP[y] );
        }

        DecimalFormat df = new DecimalFormat("0.000");
        System.out.println("\n\n");
        double ris[] = new double[10];
        for(int y=0;y<FP.length;y++){
            ris[y]=((FP[y]*1000)/(realValue[y]));
            df.format((FP[y]/realValue[y]));
            System.out.println("valore :" + y +" = "+ df.format(((FP[y]*100)/realValue[y])) );
        }
        return ris;



    }
}




