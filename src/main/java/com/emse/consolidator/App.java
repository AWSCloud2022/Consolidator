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
import java.util.ArrayList;
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

    public static void readCSVAndCompute(String bucketName, String fileName,S3Client s3){

        try {
            Double totalRetailerProfit =0.0;
            String mostProfitableStore ="";
            String leastProfitableStore ="";
            HashMap<String, Triplet<Integer, Double, Double>> listproduct = new HashMap<>();
            listproduct.put("pt1", new Triplet<>(0,0.0,0.0));

            Double valueMostProfitableStore =0.0;
            Double valueLeastProfitableStore =0.0;
            String store;
            String[] ligne;

            AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
            com.amazonaws.services.s3.model.S3Object s3Object = s3Client.getObject(bucketName, fileName);
            InputStreamReader streamReader = new InputStreamReader(s3Object.getObjectContent(), StandardCharsets.UTF_8);
            BufferedReader reader = new BufferedReader(streamReader);
            ligne = reader.readLine().split(";");
            store= ligne[0];
            totalRetailerProfit += Double.parseDouble(ligne[3]);
            if (valueMostProfitableStore < Double.parseDouble(ligne[3])){
                valueMostProfitableStore=Double.parseDouble(ligne[3]);
                mostProfitableStore=store;
            }
            if (valueLeastProfitableStore > Double.parseDouble(ligne[3])){
                valueLeastProfitableStore=Double.parseDouble(ligne[3]);
                leastProfitableStore=store;
            }

            ligne = reader.readLine().split(";");
            while(ligne.length==4){ //tant qu'on est pas à la fin du fichier


                ligne = reader.readLine().split(";");
            }
        }
        catch (IOException ioe){
            ioe.printStackTrace();
        }
    }


    public static void main(String[] args) {

        //String bucket = "databucket8906";
        //S3Client s3 = S3Client.builder().httpClient(UrlConnectionHttpClient.builder().build()).build();
        //S3Object s3Object = S3Object.builder().build();
        //System.out.println(listeNameOfFiles(listFiles(s3,bucket)));
        //readCSVAndCompute(bucket,"01-10-2022-store1.csv",s3);

        HashMap<String, Triplet<Integer, Double, Double>> listproduct = new HashMap<>();
        listproduct.put("pt1", new Triplet<>(0,0.0,0.0));
        Triplet triplet = listproduct.get("pt1");
        Integer valeur=triplet.getValue0();
        triplet = triplet.setAt0();
        listproduct.put("pt1",triplet);

        System.out.println(listproduct.get("pt1").getValue0());
    }
}
