package com.emse.consolidator;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.javatuples.Triplet;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Array;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Lambda function entry point. You can change to use other pojo type or implement
 * a different RequestHandler.
 *
 * @see <a href=https://docs.aws.amazon.com/lambda/latest/dg/java-handler.html>Lambda Java Handler</a> for more information
 */
public class App {

    public static ListObjectsResponse listFiles(S3Client s3,String bucket){
        ListObjectsRequest listBucketsRequest = ListObjectsRequest.builder().bucket(bucket).build();
        ListObjectsResponse listObjectsResponse =  s3.listObjects(listBucketsRequest);
        return listObjectsResponse;
    }

    public  static List<String> listeNameOfFiles(ListObjectsResponse listObjectsResponse){
        return listObjectsResponse.contents().stream().map(S3Object::key).collect(Collectors.toList());
    }

    public static void readCSVAndCompute(String bucketName,S3Client s3,String date){

        ArrayList<String> listeNomsFichiersATraiter = getTreatedFiles(s3,bucketName);
        listeNomsFichiersATraiter.forEach(fichier -> {
            if (!shouldTreate(fichier,date)){
                listeNomsFichiersATraiter.remove(fichier);
            }
        });



        Double totalRetailerProfit =0.0;
        String mostProfitableStore ="";
        String leastProfitableStore ="";
        Double valueMostProfitableStore =0.0;
        Double valueLeastProfitableStore =999999999999999999999999999999999999999999999999999999999.0;
        HashMap<String, Integer> productsTotalQuantity = new HashMap<>();
        HashMap<String, Double> productsTotalSold = new HashMap<>();
        HashMap<String, Double> productsTotalProfit = new HashMap<>();
        for (Integer i = 0; i < 50; i++) { //init des produits
            productsTotalQuantity.put("p"+i.toString(), 0);
            productsTotalSold.put("p"+i.toString(), 0.0);
            productsTotalProfit.put("p"+i.toString(), 0.0);

        }

        String store;
        String[] ligne;

        for (String fileName:listeNomsFichiersATraiter){

            AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
            com.amazonaws.services.s3.model.S3Object s3Object = s3Client.getObject(bucketName, fileName);
            InputStreamReader streamReader = new InputStreamReader(s3Object.getObjectContent(), StandardCharsets.UTF_8);
            BufferedReader reader = new BufferedReader(streamReader);

            try {
                ligne = reader.readLine().split(";");
            }
            catch (IOException ioe){
                ioe.printStackTrace();
                ligne = ";;;;".split(";");
            }


            store= ligne[0];
            totalRetailerProfit = totalRetailerProfit + Double.parseDouble(ligne[3]);
            if (valueMostProfitableStore < Double.parseDouble(ligne[3])){
                valueMostProfitableStore=Double.parseDouble(ligne[3]);
                mostProfitableStore=store;
            }
            if (valueLeastProfitableStore > Double.parseDouble(ligne[3])){
                valueLeastProfitableStore=Double.parseDouble(ligne[3]);
                leastProfitableStore=store;
            }


            reader.lines().forEach(line -> {
                String[] data = line.split(";");
                productsTotalQuantity.put(data[0],productsTotalQuantity.get(data[0])+Integer.parseInt(data[1]));
                productsTotalSold.put(data[0],productsTotalSold.get(data[0])+Double.parseDouble(data[2]));
                productsTotalProfit.put(data[0],productsTotalProfit.get(data[0])+Double.parseDouble(data[3]));
            });

        };

        System.out.println(productsTotalProfit);
        System.out.println(productsTotalSold);
        System.out.println(productsTotalQuantity);
        System.out.println(totalRetailerProfit); //bon
        System.out.println(leastProfitableStore); //bon
        System.out.println(mostProfitableStore);    //bon


    }

    public static ArrayList<String> getTreatedFiles(S3Client s3,String bucket){
        ArrayList<String> fichierbon = new ArrayList<>();
        listeNameOfFiles(listFiles(s3,bucket)).forEach(nom -> {
            try{
                String a= nom.split("_")[1];
                fichierbon.add(nom);
            }
            catch(ArrayIndexOutOfBoundsException az){};
        });
        return fichierbon;
    }

    public static Date getDate(String sDate){
        Date startDate=new Date();
        try {
            DateFormat df = new SimpleDateFormat("dd-MM-yyyy");
            startDate = df.parse(sDate);
            return startDate;
        }
        catch (ParseException eee){};
        return startDate;
    }

    public static boolean shouldTreate(String nomFichier, String date){
        if ((getDate(nomFichier.split("_")[0]).getDay())==(getDate(date).getDay())){
            return true;
        }
        return false;
    }

    public static void main(String[] args) {

        String bucket = "databucket8906";
        S3Client s3 = S3Client.builder().httpClient(UrlConnectionHttpClient.builder().build()).build();
        S3Object s3Object = S3Object.builder().build();
        readCSVAndCompute(bucket,s3,"01-10-2022");
        //System.out.println(listeNameOfFiles(listFiles(s3,bucket)));

    }
}
