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
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class Consolidator {

    public static ListObjectsResponse listFiles(S3Client s3, String bucket){
        ListObjectsRequest listBucketsRequest = ListObjectsRequest.builder().bucket(bucket).build();
        return s3.listObjects(listBucketsRequest);
    }

    public static List<String> listNameOfFiles(ListObjectsResponse listObjectsResponse){
        return listObjectsResponse.contents().stream().map(S3Object::key).collect(Collectors.toList());
    }

    public static void readCSVAndCompute(String bucketName, S3Client s3, String date){
        ArrayList<String> filesToProcess = getProcessedFiles(s3, bucketName);
        filesToProcess.removeIf(fileName -> (!shouldProcess(fileName, date)));

        double totalRetailerProfit = 0.0;
        String mostProfitableStore = "";
        String leastProfitableStore = "";
        double valueMostProfitableStore = 0.0;
        double valueLeastProfitableStore = Double.POSITIVE_INFINITY;
        HashMap<String, Integer> productsTotalQuantity = new HashMap<>();
        HashMap<String, Double> productsTotalSold = new HashMap<>();
        HashMap<String, Double> productsTotalProfit = new HashMap<>();
        for (int i = 0; i < 50; i++) { //initialize products
            productsTotalQuantity.put("p" + i, 0);
            productsTotalSold.put("p" + i, 0.0);
            productsTotalProfit.put("p" + i, 0.0);
        }

        String store;
        String[] line;
        double value;

        for (String fileName : filesToProcess){
            AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
            com.amazonaws.services.s3.model.S3Object s3Object = s3Client.getObject(bucketName, fileName);
            InputStreamReader streamReader = new InputStreamReader(s3Object.getObjectContent(), StandardCharsets.UTF_8);
            BufferedReader reader = new BufferedReader(streamReader);

            try {
                line = reader.readLine().split(";");
                store = line[0];
                value = Double.parseDouble(line[3]);
            }
            catch (IOException | NumberFormatException exception) {
                store = "p0";
                value = 0.0;
            }

            totalRetailerProfit += value;

            if (valueMostProfitableStore < value){
                valueMostProfitableStore = value;
                mostProfitableStore = store;
            }
            if (valueLeastProfitableStore > value){
                valueLeastProfitableStore = value;
                leastProfitableStore = store;
            }

            reader.lines().forEach(liner -> {
                String[] data = liner.split(";");
                productsTotalQuantity.put(data[0], productsTotalQuantity.get(data[0]) + Integer.parseInt(data[1]));
                productsTotalSold.put(data[0], productsTotalSold.get(data[0]) + Double.parseDouble(data[2]));
                productsTotalProfit.put(data[0], productsTotalProfit.get(data[0]) + Double.parseDouble(data[3]));
            });
        }

        System.out.print("\n\nThe total profit of the day is ");
        System.out.printf("%.1f",totalRetailerProfit);
        System.out.printf("\n\nThe most profitable store is "+mostProfitableStore+" with a profit of ");
        System.out.printf("%.1f",valueMostProfitableStore);
        System.out.printf("\nThe least profitable store is "+leastProfitableStore+" with a profit of ");
        System.out.printf("%.1f",valueLeastProfitableStore);
        System.out.println("\n\nResult by products:\n[Product]\t[Total Quantity]\t[Total Sold]\t[Total Profit]");
        for (String product : productsTotalProfit.keySet()) {
            System.out.printf(product+"\t\t\t"+productsTotalQuantity.get(product)+"\t\t\t\t");
            System.out.printf("%.1f",productsTotalSold.get(product));
            System.out.print("\t\t");
            System.out.printf("%.1f",productsTotalProfit.get(product));
            System.out.print("\n");
        }
    }

    public static ArrayList<String> getProcessedFiles(S3Client s3, String bucket){
        ArrayList<String> validFiles = new ArrayList<>();
        listNameOfFiles(listFiles(s3, bucket)).forEach(nom -> {
            if (nom.split("_").length > 2) validFiles.add(nom);
        });
        return validFiles;
    }

    public static Date getDate(String sDate){
        Date startDate = new Date();
        try {
            DateFormat df = new SimpleDateFormat("dd-MM-yyyy");
            startDate = df.parse(sDate);
            return startDate;
        }
        catch (ParseException ignored){}
        return startDate;
    }

    public static boolean shouldProcess(String fileName, String date){
        return (getDate(fileName.split("_")[0]).getDay()) == (getDate(date).getDay());
    }

    public static void main(String[] args) {
        String bucket = "databucket8906";
        S3Client s3 = S3Client.builder().httpClient(UrlConnectionHttpClient.builder().build()).build();
        //S3Object s3Object = S3Object.builder().build();

        readCSVAndCompute(bucket,s3,"02-10-2022");

    }
}
