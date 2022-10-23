package com.emse.consolidator;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3ObjectSummary;

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
    public static void main(String[] args) {
        //Enter the bucket name you wish to read the csv files from
        String bucketName = "";
        //Enter the date from which you wish to get statistics from (01-10-2022 or 02-10-2022)
        String date = "";

        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();

        readCSVAndCompute(s3Client, bucketName, date);
    }

    public static List<String> listFileNames(AmazonS3 s3Client, String bucketname){
        return s3Client.listObjects(bucketname).getObjectSummaries()
                .stream().map(S3ObjectSummary::getKey).collect(Collectors.toList());
    }

    public static ArrayList<String> getProcessedFiles(AmazonS3 s3Client, String bucketName){
        ArrayList<String> validFiles = new ArrayList<>();
        listFileNames(s3Client, bucketName).forEach(file -> {
            if (file.split("_").length >= 2) validFiles.add(file);
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

    public static boolean shouldConsolidate(String fileName, String date){
        return (getDate(fileName.split("_")[0]) == getDate(date));
    }

    public static void readCSVAndCompute(AmazonS3 s3Client, String bucketName, String date){
        List<String> filesToProcess = getProcessedFiles(s3Client, bucketName);
        filesToProcess.removeIf(fileName -> (!shouldConsolidate(fileName, date)));

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
}
