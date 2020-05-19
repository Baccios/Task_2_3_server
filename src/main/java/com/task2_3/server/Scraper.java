package com.task2_3.server;

import java.io.*;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.file.*;
import java.util.Calendar;

import com.opencsv.CSVReader;
import net.lingala.zip4j.exception.ZipException;
import net.lingala.zip4j.core.ZipFile;
import org.bson.types.ObjectId;

import org.bson.Document;



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

        //TODO usare query su mongo
        int startingYear = 2018;

        lastUpdatedMonth = 1;
        lastUpdatedYear = Integer.toString(startingYear);
    }

    public void testScraper() throws Exception {
       lastUpdatedYear= "2019";
       lastUpdatedMonth = 1;
      // getZipFile();
     //  unZip(System.getProperty("user.dir")+"/scraperDownloads/"+"report_1_2019.zip");
      // cleanDownloads("report_1_2019.zip");
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

    double ParseDouble(String strNumber) {
        if (strNumber != null && strNumber.length() > 0) {
            try {
                return Double.parseDouble(strNumber);
            } catch(Exception e) {
                return -1;   // or some value to mark this field is wrong. or make a function validates field first ...
            }
        }
        else return 0;
    }

    int ParseInteger(String strNumber) {
        if (strNumber != null && strNumber.length() > 0) {
            try {
                return Integer.parseInt(strNumber);
            } catch(Exception e) {
                return -1;   // or some value to mark this field is wrong. or make a function validates field first ...
            }
        }
        else return 0;
    }

    private void elaborateDocument(String documentName) {

        //files info
        String filePathToScrape = System.getProperty("user.dir") + "/scraperFiles/" + documentName + ".csv";

        try {

            //list of data to scrape
            int quarter;
            String fl_date;
            String op_unique_carrier;
            int crs_dep_time;
            int dep_time;
            Double dep_delay;
            Double dep_del15;
            int crs_arr_time;
            int arr_time;
            Double arr_delay;
            Double arr_del15;
            int cancelled;
            String cancellation_code;
            int crs_elapsed_time;
            int actual_elapsed_time;
            int distance;
            Double carrier_delay;
            int weather_delay;
            int nas_delay;
            int security_delay;
            int late_aircraft_delay;
            String dest_iata;
            int dest_airport_id;
            String dest_city_name;
            String dest_state_nm;
            String origin_iata;
            int origin_airport_id;
            String origin_city_name;
            String origin_state_nm;

            try ( Reader reader = Files.newBufferedReader(Paths.get(filePathToScrape));
                  CSVReader csvReader = new CSVReader(reader);)
            {
                //write headers with quotes
                String[] flightInfo;
                flightInfo = csvReader.readNext();

                while ((flightInfo = csvReader.readNext()) != null) {

                    quarter = ParseInteger(flightInfo[1]);
                    fl_date = flightInfo[5];
                    op_unique_carrier = flightInfo[8];
                    crs_dep_time = ParseInteger(flightInfo[29]);
                    dep_time = ParseInteger(flightInfo[30]);
                    dep_delay = ParseDouble(flightInfo[31].replaceAll("-", ""));
                    dep_del15 = ParseDouble(flightInfo[33]);
                    crs_arr_time = ParseInteger(flightInfo[40]);
                    arr_time = ParseInteger(flightInfo[41]);
                    arr_delay = ParseDouble(flightInfo[42].replaceAll("-", ""));
                    arr_del15 = ParseDouble(flightInfo[44]);
                    cancelled = (int)(ParseDouble(flightInfo[47]));
                    cancellation_code = flightInfo[48];
                    crs_elapsed_time = (int)(ParseDouble(flightInfo[50]));
                    actual_elapsed_time = (int)(ParseDouble(flightInfo[51]));
                    distance = (int)(ParseDouble(flightInfo[54]));
                    carrier_delay = ParseDouble(flightInfo[56]);
                    weather_delay = (int)(ParseDouble(flightInfo[57]));
                    nas_delay = (int)(ParseDouble(flightInfo[58]));
                    security_delay = (int)(ParseDouble(flightInfo[59]));
                    late_aircraft_delay = (int)(ParseDouble(flightInfo[60]));
                    dest_iata = flightInfo[23];
                    dest_airport_id = ParseInteger(flightInfo[20]);
                    dest_city_name = flightInfo[24];
                    dest_state_nm = flightInfo[27];
                    origin_iata = flightInfo[14];
                    origin_airport_id = ParseInteger(flightInfo[12]);
                    origin_city_name = flightInfo[15];
                    origin_state_nm = flightInfo[18];

                    Document flightDocument = new Document("_id", new ObjectId());
                    flightDocument.append("QUARTER", quarter)
                            .append("FL_DATE",fl_date)
                            .append("OP_UNIQUE_CARRIER",op_unique_carrier)
                            .append("CRS_DEP_TIME",crs_dep_time)
                            .append("DEP_TIME",dep_time)
                            .append("DEP_DELAY",dep_delay)
                            .append("DEP_DEL15",dep_del15)
                            .append("CRS_ARR_TIME",crs_arr_time)
                            .append("ARR_TIME",arr_time)
                            .append("ARR_DELAY",arr_delay)
                            .append("ARR_DEL15",arr_del15)
                            .append("CANCELLED",cancelled)
                            .append("CANCELLATION_CODE",cancellation_code)
                            .append("CRS_ELAPSED_TIME",crs_elapsed_time)
                            .append("ACTUAL_ELAPSED_TIME",actual_elapsed_time)
                            .append("DISTANCE",distance)
                            .append("CARRIER_DELAY",carrier_delay)
                            .append("WEATHER_DELAY",weather_delay)
                            .append("NAS_DELAY",nas_delay)
                            .append("SECURITY_DELAY",security_delay)
                            .append("LATE_AIRCRAFT_DELAY",late_aircraft_delay)
                            .append("DEST_AIRPORT", (new Document("DEST_IATA", dest_iata)
                                                        .append("DEST_AIRPORT_ID",dest_airport_id)
                                                        .append("DEST_CITY_NAME",dest_city_name)
                                                        .append("DEST_STATE_NM",dest_state_nm)))
                            .append("ORIGIN_AIRPORT", (new Document("ORIGIN_IATA", origin_iata)
                                    .append("ORIGIN_AIRPORT_ID",origin_airport_id)
                                    .append("ORIGIN_CITY_NAME",origin_city_name)
                                    .append("ORIGIN_STATE_NM",origin_state_nm)));

                    System.out.println(flightDocument.toJson());

                }

            }
        }

        catch (IOException e) {
            System.err.format("Something went wrong during the scraping of the csv file");
            e.printStackTrace();
        }
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
        FileOutputStream download = new FileOutputStream(System.getProperty("user.dir")+"/scraperDownloads/"+"report_"+requestedMonth+"_"+requestedYear+".zip");
        download.getChannel().transferFrom(Channels.newChannel(new URL(preparedUrl).openStream()), 0, Long.MAX_VALUE);
        download.close();
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