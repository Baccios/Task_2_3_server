package com.task2_3.server;

import java.io.*;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import com.opencsv.CSVWriter;
import net.lingala.zip4j.exception.ZipException;
import net.lingala.zip4j.core.ZipFile;

import javax.swing.text.Document;
import java.io.BufferedReader;
import java.io.FileReader;


public class Scraper {
    private String months [];
    private String lastUpdatedYear;
    private int lastUpdatedMonth;
    private String url ="https://www.transtats.bts.gov/DL_SelectFields.asp";

    public void Scraper() {
        months[0] = null ;
        months[1] = "January";
        months[2] = "February";
        months[3] = "March";
        months[4] = "April";
        months[5] = "May";
        months[6] = "June";
        months[7] = "July";
        months[8] = "August";
        months[9] = "September";
        months[10] = "October";
        months[11] = "November";
        months[12] = "December";

        int startingYear = 2018;

        lastUpdatedMonth = 1;
        lastUpdatedYear = Integer.toString(startingYear);
    }

    public void testScraper() throws Exception {
       lastUpdatedYear= "2019";
       lastUpdatedMonth = 1;
       getZipFile();
       unZip(System.getProperty("user.dir")+"/scraperDownloads/"+"report_1_2019.zip");
       cleanDownloads("report_1_2019.zip");
       elaborateDocument("On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_2019_1");
    }

    public void startScraping() throws Exception {
        //This method should be used to initialize the system, after that use the "periodicScraping" method
        int actualYear = Calendar.getInstance().get(Calendar.YEAR);
        int lastYear = Integer.parseInt(lastUpdatedYear);
        //Initialize the system with raw data from the past years in reference to the actual date
        while (actualYear > lastYear){
            while (lastUpdatedMonth <= 12) {
                scrape();
                lastUpdatedMonth++;
                updateScraperState(lastUpdatedMonth,lastYear);
            }
            lastUpdatedMonth = 1;
            lastYear = lastYear + 1;
            updateScraperState(lastUpdatedMonth,lastYear);
        }

    }

    private void elaborateDocument(String documentName) {

        //files info
        String filePathToScrape = System.getProperty("user.dir") + "/scraperFiles/" + documentName + ".csv";
        String filePath = System.getProperty("user.dir") + "/scraperFiles/" + documentName + "_SCRAPED.csv";
        String line = "";
        String cvsSplitBy = ",";

        //creates new csv file
        File file = new File(filePath);

        try {
            // create FileWriter object with file as parameter
            FileWriter outputfile = new FileWriter(file);

            // create CSVWriter object filewriter object as parameter
            CSVWriter writer = new CSVWriter(outputfile);

            // create a List which contains String array
            List<String[]> data = new ArrayList<String[]>();

            //list of data to scrape
            String quarter;
            String fl_date;
            String op_unique_carrier;
            String crs_dep_time;
            String dep_time;
            String dep_delay;
            String dep_del15;
            String crs_arr_time;
            String arr_time;
            String arr_delay;
            String arr_del15;
            String cancelled;
            String cancellation_code;
            String crs_elapsed_time;
            String actual_elapsed_time;
            String distance;
            String carrier_delay;
            String weather_delay;
            String nas_delay;
            String security_delay;
            String late_aircraft_delay;
            String dest_iata;
            String dest_airport_id;
            String dest_city_name;
            String dest_state_nm;
            String origin_iata;
            String origin_airport_id;
            String origin_city_name;
            String origin_state_nm;

            try (BufferedReader br = new BufferedReader(new FileReader(filePathToScrape))) {

                while ((line = br.readLine()) != null) {

                    // use comma as separator
                    String[] flightInfo = line.split(cvsSplitBy);

                    quarter = flightInfo[1];
                    fl_date = flightInfo[5];
                    op_unique_carrier = flightInfo[8];
                    crs_dep_time = flightInfo[29];
                    dep_time = flightInfo[30];
                    dep_delay = flightInfo[31].replaceAll("-", "");
                    dep_del15 = flightInfo[33];
                    crs_arr_time = flightInfo[40];
                    arr_time = flightInfo[41];
                    arr_delay = flightInfo[42];
                    arr_del15 = flightInfo[44];
                    cancelled = flightInfo[47];
                    cancellation_code = flightInfo[48];
                    crs_elapsed_time = flightInfo[50];
                    actual_elapsed_time = flightInfo[51];
                    distance = flightInfo[54];
                    carrier_delay = flightInfo[56];
                    weather_delay = flightInfo[57];
                    nas_delay = flightInfo[58];
                    security_delay = flightInfo[59];
                    late_aircraft_delay = flightInfo[60];
                    dest_iata = flightInfo[23];
                    dest_airport_id = flightInfo[20];
                    dest_city_name = flightInfo[24];
                    dest_state_nm = flightInfo[27];
                    origin_iata = flightInfo[14];
                    origin_airport_id = flightInfo[12];
                    origin_city_name = flightInfo[15];
                    origin_state_nm = flightInfo[18];

                    data.add(new String[]{quarter, fl_date, op_unique_carrier, crs_dep_time, dep_time, dep_delay, dep_del15,
                            crs_arr_time, arr_time, arr_delay, arr_del15, cancelled, cancellation_code, crs_elapsed_time, actual_elapsed_time,
                            distance, carrier_delay, weather_delay, nas_delay, security_delay, late_aircraft_delay, dest_iata, dest_airport_id,
                            dest_city_name, dest_state_nm, origin_iata, origin_airport_id, origin_city_name, origin_state_nm});
                }

                writer.writeAll(data);

                // closing writer connection
                writer.close();
            }
        }

        catch (IOException e) {
            System.err.format("Something went wrong during the scraping of the csv file");
            e.printStackTrace();
        }
        System.out.println("Parsing of "+documentName+".csv completed");
        System.out.println("Inserting "+documentName+"_SCRAPED.csv into database");
        //insertDocument(documentName+"_SCRAPED.csv");
    }


    private void insertDocument(String csvFile) {
        MongoDBManager MongoClient = new MongoDBManager();
        MongoClient.openConnection();
    }


    public void periodicScraping() throws Exception {
        //TODO update last year and last month based on mongo values

        //TODO check if the website has new entries

        //if yes update month and year

        //parseDocument();


    }

    private void updateScraperState(int nextMonth, int nextYear){
        lastUpdatedYear = Integer.toString(nextYear);
        lastUpdatedMonth = nextMonth;
    }

    private void getZipFile() throws Exception {
        // let s first build the url based on the parameters of the scraper
        String requestedYear = lastUpdatedYear;
        String requestedMonth = Integer.toString(lastUpdatedMonth);
        String preparedUrl = "https://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_"+requestedYear+"_"+requestedMonth+".zip";

        System.out.println("Requested URL: "+preparedUrl);
        System.out.println("Attempting download");
        new FileOutputStream(System.getProperty("user.dir")+"/scraperDownloads/"+"report_"+requestedMonth+"_"+requestedYear+".zip").getChannel().transferFrom(Channels.newChannel(new URL(preparedUrl).openStream()), 0, Long.MAX_VALUE);

        System.out.println("Download finished");
        System.out.println("New report added: "+"/scraperDownloads/"+"report_"+requestedMonth+"_"+requestedYear+".zip");
    }

    private static void unZip(String filePath){
        String source = filePath;
        String destination = "scraperFiles";
        String password = "";

        try {
            ZipFile zipFile = new ZipFile(source);
            if (zipFile.isEncrypted()) {
                zipFile.setPassword(password);
            }
            zipFile.extractAll(destination);
        } catch (ZipException e) {
            e.printStackTrace();
        }
    }

    private void cleanDownloads(String fileName) throws IOException {
        String trashFile = System.getProperty("user.dir")+"/scraperDownloads/"+fileName;
        Path trashPath = Paths.get(trashFile);
        try {
            Files.delete(trashPath);
        } catch (NoSuchFileException x) {
            System.err.format("%s: no such" + " file or directory%n", trashPath);
        } catch (DirectoryNotEmptyException x) {
            System.err.format("%s not empty%n", trashPath);
        } catch (IOException x) {
            //File permission problems are caught here
            System.err.println(x);
        }
    }

    private void cleanTrash(){
        String trashFile = System.getProperty("user.dir")+"/scraperFiles/"+"readme.html";
        Path trashPath = Paths.get(trashFile);
        try {
            Files.delete(trashPath);
        } catch (NoSuchFileException x) {
            System.err.format("%s: no such" + " file or directory%n", trashPath);
        } catch (DirectoryNotEmptyException x) {
            System.err.format("%s not empty%n", trashPath);
        } catch (IOException x) {
            //File permission problems are caught here
            System.err.println(x);
        }
    }


    private void scrape() {
        String requestedYear = lastUpdatedYear;
        String requestedMonth = Integer.toString(lastUpdatedMonth);

        //Retrieve raw data from the web
        try {
            getZipFile();
            unZip(System.getProperty("user.dir")+"/scraperDownloads/"+"report_"+requestedMonth+"_"+requestedYear+".zip");
        } catch (Exception e) {
            e.printStackTrace();
            System.err.format("Something went wrong during the retrieve of the file from the website");
            return;
        }

        // remove useless files
        try {
            //removes zip file
            cleanDownloads("report_" + requestedMonth + "_" + requestedYear + ".zip");
            //removes readme
            cleanTrash();
        } catch (Exception e){
            System.err.format("Something went wrong when trying to delete trash files");
        }

        String documentName = "On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_"+requestedYear+"_"+requestedMonth;

        // Elaborates the raw data in a MongoDB support db and insert it in the final DB
        elaborateDocument(documentName);

    }

}