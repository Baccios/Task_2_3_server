package com.task2_3.server;

import java.io.*;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.file.*;
import java.util.Calendar;
import net.lingala.zip4j.exception.ZipException;
import net.lingala.zip4j.core.ZipFile;


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

    private void insertDocument(String documentName) {
        //TODO insert document inside of MongoDB
    }

    private void elaborateDocument(String documentName) {
        //TODO elaborate document and insert in the final MongoDB
    }

    public void periodicScraping() throws Exception {
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
            //TODO inserire un semaforo per non eliminare prima di aver finito di estrarre
            cleanDownloads("report_"+requestedMonth+"_"+requestedYear+".zip");
        } catch (Exception e) {
            e.printStackTrace();
            System.err.format("Something went wrong during the retrieve of the file from the website");
            return;
        }

        //Removes readme file
        cleanTrash();
        String documentName = "On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_"+requestedYear+"_"+requestedMonth+".csv";

        // Elaborates the raw data in a MongoDB support db and insert it in the final DB
        insertDocument(documentName);

    }

}