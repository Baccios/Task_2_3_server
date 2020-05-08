package com.task2_3.server;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import java.security.SecureRandom;
import java.security.spec.KeySpec;
import java.util.Base64;
import java.io.*;

public class PasswordManager {
    /** Get base64 encoded salt (initialization vector)
     * @return the random generated salt
     */
    public String getNewSalt()  {
        // Don't use Random!
        byte[] salt = new byte[8];
        try {
            SecureRandom random = SecureRandom.getInstance("SHA1PRNG");
            // NIST recommends minimum 4 bytes. We use 8.
            random.nextBytes(salt);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return Base64.getEncoder().encodeToString(salt);
    }

    /** Get an encrypted password using PBKDF2 hash algorithm
     * @param password the password to encrypt
     * @param salt the salt to randomize the output
     * @return the encrypted password
     */
    public String getEncryptedPassword(String password, String salt) {
        String algorithm = "PBKDF2WithHmacSHA1";
        int derivedKeyLength = 160; // for SHA1
        int iterations = 20000; // NIST specifies 10000

        byte[] saltBytes = Base64.getDecoder().decode(salt);
        KeySpec spec = new PBEKeySpec(password.toCharArray(), saltBytes, iterations, derivedKeyLength);
        byte[] encBytes = null;
        try {
            SecretKeyFactory f = SecretKeyFactory.getInstance(algorithm);
            encBytes = f.generateSecret(spec).getEncoded();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return Base64.getEncoder().encodeToString(encBytes);
    }

    /**
     * Retrieve the username and password of the system administrator
     * @return The info about the administrator
     */
    private AdminInfo retrieveCredentials() {
        File pswStore = new File("./keychain/admins");
        if(!pswStore.exists() || pswStore.isDirectory()) {
            System.err.println("File name for admin passwords is not correct");
            return null;
        }
        try(FileInputStream reader = new FileInputStream(pswStore)) {
            ObjectInputStream br = new ObjectInputStream(reader);

            Object ret = br.readObject();
            if(ret == null) {
                System.err.println("Admin password storage is empty!");
                return null;
            }

            return (AdminInfo) ret;

        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Set new credentials for the admin
     * @param username the new username
     * @param password the new password
     * @return true if credentials were correctly updated
     */
    public boolean setCredentials(String username, String password) {
        File pswStore = new File("./keychain/admins");
        if(!pswStore.canWrite()) {
            System.err.println("Error: The application cannot write the new credentials!");
            return false;
        }

        String salt = getNewSalt();
        String encPsw = getEncryptedPassword(password, salt);

        AdminInfo newAdmin = new AdminInfo(username, encPsw, salt);

        try (FileOutputStream fos = new FileOutputStream(pswStore)) {
            ObjectOutputStream pw = new ObjectOutputStream(fos);
            pw.writeObject(newAdmin);

        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }


    /**
     * Authenticate admin credentials
     * @param username the admin username
     * @param password the admin password
     * @return true if credentials are correct
     */
    public boolean authenticate(String username, String password) {
        AdminInfo admin = retrieveCredentials();
        if(admin == null) {
            System.err.println("There is no admin available in the system!");
            return false;
        }

        if(!admin.username.equals(username)) {
            return false;
        }

        String inputHash = getEncryptedPassword( password, admin.salt);

        return inputHash.equals(admin.encPassword);
    }


    private static class AdminInfo implements Serializable{
        public String username;
        public String encPassword;
        public String salt;

        public AdminInfo(String username, String encPassword, String salt) {
            this.username = username;
            this.encPassword = encPassword;
            this.salt = salt;
        }

        public String toString() {
            return username+","+encPassword+","+salt+"\n";
        }

    }


}
