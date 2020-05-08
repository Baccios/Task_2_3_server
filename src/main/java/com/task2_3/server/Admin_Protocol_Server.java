package com.task2_3.server;
import java.io.*;
import java.security.KeyStore;
import javax.net.ssl.*;

/**
 * This class implements a multi-thread Admin-Protocol server
 */
public class Admin_Protocol_Server implements AutoCloseable, Runnable {
    int port;
    SSLServerSocket server;
    SSLSocket socket;
    BufferedReader in;
    PrintWriter out;
    String username;
    String password;

    MongoDBManager mongomanager;

    boolean AdminIsChecked;

    /**
     * Instantiate a SSl Server Socket.
     * @return the socket
     */
    private SSLServerSocket getServerSocket() {
        try {
            SSLContext context;
            KeyManagerFactory kmf;
            KeyStore ks;
            char[] storepass = "password".toCharArray();
            char[] keypass = "password".toCharArray();
            String storeName = "./keychain/keystore.jks";
            context = SSLContext.getInstance("TLS");
            kmf = KeyManagerFactory.getInstance("SunX509");
            FileInputStream fin = new FileInputStream(storeName);
            ks = KeyStore.getInstance("JKS");
            ks.load(fin, storepass);

            kmf.init(ks, keypass);
            context.init(kmf.getKeyManagers(), null, null);
            SSLServerSocketFactory factory = context.getServerSocketFactory();

           // SSLServerSocketFactory factory = (SSLServerSocketFactory)SSLServerSocketFactory.getDefault();

            SSLServerSocket socket = (SSLServerSocket) factory.createServerSocket(port);
            return socket;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Wait for a connection and authentication with the client.
     * @return true if credentials were correct and session was established
     */
    private boolean waitForAuth(SSLSocket sock) {
        // accept a connection
        BufferedReader MyIn;
        PrintWriter MyOut;
        try {
            MyOut = new PrintWriter(
                    new BufferedWriter(
                            new OutputStreamWriter(
                                    sock.getOutputStream()
                            )
                    )
            );

            MyIn = new BufferedReader(
                    new InputStreamReader(
                            sock.getInputStream()
                    )
            );

            String[] credentials = getCredentials();
            String comparison = "Auth " + credentials[0] + " " + credentials[1];
            String inputLine = MyIn.readLine();
            System.out.println("Authentication request received:");
            System.out.println("The text of the request is: \""+inputLine+"\"");

            if(inputLine.equals(comparison)) {
                boolean available = checkAndSetAdmin();
                if(!available) {
                    if(!sendMessage("Auth denied errcode 2", MyOut)) {
                        System.err.println("Error occurred during authentication");
                    }
                    MyIn.close();
                    MyOut.close();
                    System.out.println("Authentication denied: Server was busy with another admin");
                    return false;
                }
                else if(!sendMessage("Auth successful", MyOut)) {
                    System.err.println("Error occurred during authentication");
                    MyIn.close();
                    MyOut.close();
                    return false;
                }
                System.out.println("Authentication successful");
                socket = sock;
                in = MyIn;
                out = MyOut;
                return true;
            }

            else if(inputLine.startsWith("Auth")) {
                if(!sendMessage("Auth denied errcode 1", MyOut)) {
                    System.err.println("Error occurred during authentication");
                }
                System.out.println("Authentication denied: wrong credentials");
                MyIn.close();
                MyOut.close();
                return false;
            }

            /*
           else {
                Ignore the request as specified in the protocol
           }
             */


        } catch (IOException e) {
            System.err.println("Server died: " + e.getMessage());
            e.printStackTrace();
        }

        return false;
    }

    /**
     * Handle a credentials request and send back the acknowledgement message.
     * @param username the new admin username
     * @param psw the new admin password
     */
    private void handleCredentials(String username, String psw) {
        setCredentials(username,psw);

        System.out.println("Managing the credentials request...");

        //TODO: update admin credentials in the database

        if(!sendMessage("Ack credentials")) {
            System.err.println("Error occurred during credentials request processing");
            return;
        }
        System.out.println("Credentials request correctly committed");
    }

    /**
     * Handle a scrape request and send back the acknowledgement message.
     */
    private void handleScrape() {

        System.out.println("Managing scrape request...");

        if(!sendMessage("Ack scrape")) {
            System.err.println("Error occurred during scrape request processing");
        }

        //TODO: force the scraping algorithm

        System.out.println("Scrape request correctly committed");
    }

    /**
     * Handle a year request and send back the acknowledgement message.
     * @param year the new starting year
     */
    private void handleYear(int year) {
        System.out.println("Managing the year request...");

        if(!sendMessage("Ack credentials")) {
            System.err.println("Error occurred during year request processing");
            return;
        }

        //service execution
        mongomanager.deleteUntilYear(year);

        System.out.println("Year request correctly committed");
    }

    /**
     * Handle an update request and send back the acknowledgement message.
     */
    private void handleUpdate() {
        System.out.println("Managing update request...");

        if(!sendMessage("Ack update")) {
            System.err.println("Error occurred during update request processing");
        }

        Neo4jDBManager neo4jmanager = new Neo4jDBManager("bolt://172.16.1.15:7687", "neo4j", "root");
        neo4jmanager.update(mongomanager.getUpdatePacket());
        neo4jmanager.close();

        System.out.println("Update request correctly committed");
    }

    /**
     * Handle a replicas request and send back the acknowledgement message.
     * @param level the new level of replication in the database (values from 1 to 3)
     */
    private void handleReplicas(int level) {
        System.out.println("Managing replicas request...");

        if(!sendMessage("Ack replicas")) {
            System.err.println("Error occurred during replicas request processing");
        }

        //TODO: handle the request on MongoDB

        System.out.println("Replicas request correctly committed");
    }

    /**
     * Handle a limit request and send back the acknowledgement message.
     * @param limit the new storage limit in MongoDB (in Gigabytes)
     */
    private void handleLimit(int limit) {
        if(limit < 1)
            return;

        System.out.println("Managing limit request...");

        if(!sendMessage("Ack limit")) {
            System.err.println("Error occurred during limit request processing");
        }

        double size_mega = (mongomanager.getStorageSize())/(1000000.0);
        double delta = size_mega - limit*1000000;

        while(delta > 0) {
            if(delta > 8000000) {//in a year there are about 6GB of flights, so we can be quite sure
                mongomanager.deleteOldestYear();
            }
            else {
                mongomanager.deleteOldestMonth();
            }
            size_mega = (mongomanager.getStorageSize())/(1000000.0);
            delta = size_mega - limit;
        }


        System.out.println("Limit request correctly committed");
    }

    /**
     * Handle a checkout request and send back the acknowledgement message.
     */
    private void handleCheckout() {
        System.out.println("Managing checkout request...");

        if(!sendMessage("Ack checkout")) {
            System.err.println("Error occurred during checkout request processing");
        }

        releaseAdmin();

        System.out.println("Checkout request correctly committed");
    }

    /**
     * Wait for and handle any kind of incoming request.
     * @return false when there's a checkout request.
     */
    private boolean handleRequest() {
        String inputLine = "";
        try {
            inputLine = in.readLine();
            if(inputLine == null) {
                System.out.println("Client closed its socket. Closing the connection");
                releaseAdmin();
                return false;
            }
            System.out.println("Request received:");
            System.out.println("Request text: \""+inputLine+"\"");
        } catch (IOException e) {
            e.printStackTrace();
        }
        if(!inputLine.startsWith("Request ")) {
            return true;
        }

        String[] input = inputLine.split(" ");
        if(input.length == 2) {
            if (input[1].equals("update"))
                handleUpdate();
            else if(input[1].equals("scrape"))
                handleScrape();
            else if(input[1].equals("checkout")) {
                handleCheckout();
                return false;
            }
        }
        else if(input.length == 3) {
            String[] params = input[2].split(",");
            String regex = "^\\d+$";
            if(input[1].equals("credentials") && params.length == 2) {
                handleCredentials(params[0],params[1]);
            }
            else if(input[1].equals("limit") && params.length == 1 && params[0].matches(regex)) {
                handleLimit(Integer.parseInt(params[0]));
            }
            else if(input[1].equals("year") && params.length == 1 && params[0].matches(regex)) {
                handleYear(Integer.parseInt(params[0]));
            }
            else if(input[1].equals("replicas") && params.length == 1 && params[0].matches(regex)) {
                handleReplicas(Integer.parseInt(params[0]));
            }
        }
        return true;
    }

    /**
     * Close the data socket with a client
     */
    private void closeSession() {
        try {
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Close an Admin_Protocol_Server
     */
    public void close() {
        try {
            server.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void run() {
        SSLSocket sock;
        try {
            System.out.println("New listener ready for connections...");
            sock = (SSLSocket) server.accept();

            newListener(); //place another thread listening to new connections

            if(!waitForAuth(sock)) {
                sock.close();
                return;
            }

            //Since this point the admin is logged in
            mongomanager = new MongoDBManager();

            do {}
            while(handleRequest()); //handle client requests until a checkout

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Generate a new listening thread
     */
    private void newListener() {
        (new Thread(this)).start();
    }

    /**
     * Send a message through the socket.
     * @param msg the message to be sent
     * @return true if the message was correctly sent
     */
    private boolean sendMessage(String msg) {
        out.println(msg.toCharArray());
        out.flush();
        if (out.checkError()) {
            System.err.println(
                    "SSLSocketClient:  java.io.PrintWriter error");
            return false;
        }
        return true;
    }

    /**
     * Send a message through a specified output buffer.
     * @param msg the message to be sent
     * @param out the output stream
     * @return true if the message was correctly sent
     */
    private boolean sendMessage(String msg, PrintWriter out) {
        out.println(msg.toCharArray());
        out.flush();
        if (out.checkError()) {
            System.err.println(
                    "SSLSocketClient:  java.io.PrintWriter error");
            return false;
        }
        return true;
    }


    /**
     * Constructor of the class.
     * @param port the port on which the server will listen
     * @param username the username credential to log in
     * @param password the password credential to log in
     */
    public Admin_Protocol_Server(int port, String username, String password) {
        this.port = port;
        this.server = getServerSocket();
        mongomanager = null;
        //don't keep a neo4j connection always open as only one command interacts with neo4j
        setCredentials(username,password);
        this.AdminIsChecked = false;
        newListener();
    }

    /**
     * Set new credentials in a thread-safe manner.
     * @param user the new username
     * @param psw the new password
     */
    synchronized public void setCredentials(String user, String psw) {
       this.username = user;
       this.password = psw;
    }

    /**
     * Get both credentials in a thread-safe manner.
     * @return username and password in an array.
     */
    synchronized public String[] getCredentials() {
        String u = username;
        String p = password;
        return new String[]{u,p};
    }

    /**
     * Check if anyone is logged in, and if this is the case log in the admin.
     * @return true if login went successfully
     */
    synchronized private boolean checkAndSetAdmin() {
        if(!AdminIsChecked) {
            AdminIsChecked = true;
            return true;
        }
        return false;
    }

    /**
     * Close a session with an admin after the checkout.
     */
    synchronized private void releaseAdmin() {
        mongomanager.close();
        mongomanager = null; //mongo manager instance is kept alive for the duration of the connection
        closeSession();
        AdminIsChecked = false;
    }

}
