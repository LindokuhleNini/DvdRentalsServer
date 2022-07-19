package dvdrentalsserver;

import static dvdrentalsserver.CustomerDAO.DATABASE_URL;
import static dvdrentalsserver.CustomerDAO.password;
import static dvdrentalsserver.CustomerDAO.username;
import java.util.*;
import java.net.*;
import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.swing.JOptionPane;
import modelclasses.Customer;
import modelclasses.DVD;
import modelclasses.Rental;

/**
 *DvdRentalsClient.java
 * This is our DVD Rentals Client Server Program
 * @author Lindokuhle Nini (218196504)
 * @author Sihle Jijana (216273919)
 * Date: 08 November 2020
 */

public class DvdRentalsServer {

    public static void main(String s[]) throws Exception {

        Socket sa = null;
        Socket sa2 = null;
        ServerSocket ss2 = null;
        System.out.println("server listening ");
        try {
            ss2 = new ServerSocket(7800);
            //ss2.setSoTimeout(0);
        } catch (IOException e) {
            System.out.println("server error");
        }
        while (true) {
            try {
                sa = ss2.accept();
                System.out.println("connetion established");
                ServerThread st = new ServerThread(sa);
                st.start();
            } catch (IOException e) {
                System.out.println("connetion error");
            }
        }
    }
    public static int custNr;
}

class ServerThread extends Thread {

    ObjectOutputStream output = null;
    ObjectInputStream input = null;

    OutputStream s1out = null;
    DataOutputStream dos = null;
    Socket s1 = null;
    public static int dvdNr;
    public static int custNr;
    public static int rentalNr;

    public ServerThread(Socket s) {
        s1 = s;
    }

    @Override
    public void run() {

        String message = "";
        try {
            System.out.println("sending data");
            output = new ObjectOutputStream(s1.getOutputStream());
            input = new ObjectInputStream(s1.getInputStream());
            message = (String) input.readObject();

            if (message.equalsIgnoreCase("addcustomer")) {
                Customer c = new Customer();
                CustomerDAO cdao = new CustomerDAO();
                while ((c = (Customer) input.readObject()) != null) {
                    System.out.println("Recieving data...");
                    c.getCustNumber();
                    c.getName();
                    c.getSurname();
                    c.getPhoneNum();
                    c.getCredit();
                    c.canRent();
                    //Recieve object!
                    System.out.println("Customer object recieved from Client");
                    cdao.saveCustomer(c);
                    //input.close();
                }
            } else if (message.equalsIgnoreCase("adddvd")) {
                //input.close();
                DVD dvd = new DVD();
                while ((dvd = (DVD) input.readObject()) != null) {
                    System.out.println("Recieving data...");
                    DvdDAO dvdDao = new DvdDAO();
                    dvd.getDvdNumber();
                    dvd.getTitle();
                    dvd.getCategory();
                    dvd.getPrice();
                    dvd.isNewRelease();
                    dvd.isAvailable();
                    System.out.println("DVD object recieved from Client");
                    // Recieve object!
                    dvdDao.saveDvd(dvd);
                    //input.close();
                }
            } else if (message.equalsIgnoreCase("displaycustomers")) {

                ArrayList<Vector> vecArr = new ArrayList<>();
                Connection connection = null;
                PreparedStatement statement = null;
                connection = DriverManager.getConnection(DATABASE_URL, username, password);
                statement = connection.prepareStatement("select * from Customer ORDER By FirstName ASC");
                ResultSet rs = statement.executeQuery();

                ResultSetMetaData rd = rs.getMetaData();
                int a = rd.getColumnCount();

                while (rs.next()) {
                    Vector vect = new Vector();

                    for (int i = 1; i <= a; i++) {
                        vect.add(rs.getString("custNumber"));
                        vect.add(rs.getString("firstName"));
                        vect.add(rs.getString("surname"));
                        vect.add(rs.getString("phoneNum"));
                        vect.add(rs.getString("credit"));
                        vect.add(rs.getString("canRent"));

                    }

                    vecArr.add(vect);
                }
                output.writeObject(vecArr);
                output.flush();
            } else if (message.equalsIgnoreCase("displaydvds")) {

                ArrayList<Vector> vecArr = new ArrayList<>();
                Connection connection = null;
                PreparedStatement statement = null;
                connection = DriverManager.getConnection(DATABASE_URL, username, password);
                statement = connection.prepareStatement("select * from Dvd ORDER By Category");
                ResultSet rs = statement.executeQuery();

                ResultSetMetaData rd = rs.getMetaData();
                int a = rd.getColumnCount();

                while (rs.next()) {
                    Vector vect = new Vector();

                    for (int i = 1; i <= a; i++) {
                        vect.add(rs.getString("dvdNumber"));
                        vect.add(rs.getString("title"));
                        vect.add(rs.getString("category"));
                        vect.add(rs.getString("price"));
                        vect.add(rs.getString("newRelease"));
                        vect.add(rs.getString("availableForRent"));

                    }

                    vecArr.add(vect);
                }
                output.writeObject(vecArr);
                output.flush();
            } else if (message.equalsIgnoreCase("showexistingcustomers")) {

                ArrayList<Vector> vecArr = new ArrayList<>();
                Connection connection = null;
                PreparedStatement statement = null;
                connection = DriverManager.getConnection(DATABASE_URL, username, password);
                statement = connection.prepareStatement("select CustNumber, FirstName, Surname from Customer where CanRent=true");
                ResultSet rs = statement.executeQuery();

                ResultSetMetaData rd = rs.getMetaData();
                int a = rd.getColumnCount();

                while (rs.next()) {
                    Vector vect = new Vector();

                    for (int i = 1; i <= 1; i++) {

                        vect.add(rs.getString("custNumber"));
                        vect.add(rs.getString("firstName"));
                        vect.add(rs.getString("surname"));

                    }
                    vecArr.add(vect);
                    //System.out.println(vecArr);
                }
                output.writeObject(vecArr);
                output.flush();
            } else if (message.equalsIgnoreCase("horror")) {

                ArrayList<Vector> vecArr = new ArrayList<>();
                Connection connection = null;
                PreparedStatement statement = null;
                connection = DriverManager.getConnection(DATABASE_URL, username, password);
                statement = connection.prepareStatement("select DvdNumber, Title from Dvd where Category='horror' AND AvailableForRent=true");
                ResultSet rs = statement.executeQuery();

                ResultSetMetaData rd = rs.getMetaData();
                int a = rd.getColumnCount();

                while (rs.next()) {
                    Vector vect = new Vector();

                    for (int i = 1; i <= 1; i++) {

                        vect.add(rs.getInt("dvdNumber"));
                        vect.add(rs.getString("title"));

                    }
                    vecArr.add(vect);
                    //System.out.println(vecArr);
                }
                output.writeObject(vecArr);
                output.flush();
            } else if (message.equalsIgnoreCase("scifi")) {

                ArrayList<Vector> vecArr = new ArrayList<>();
                Connection connection = null;
                PreparedStatement statement = null;
                connection = DriverManager.getConnection(DATABASE_URL, username, password);
                statement = connection.prepareStatement("select DvdNumber, Title from Dvd where Category='Sci-fi' AND AvailableForRent=true");
                ResultSet rs = statement.executeQuery();

                ResultSetMetaData rd = rs.getMetaData();
                int a = rd.getColumnCount();

                while (rs.next()) {
                    Vector vect = new Vector();

                    for (int i = 1; i <= 1; i++) {

                        vect.add(rs.getString("dvdNumber"));
                        vect.add(rs.getString("title"));

                    }
                    vecArr.add(vect);

                }
                output.writeObject(vecArr);
                output.flush();
                //output.close();
            } else if (message.equalsIgnoreCase("drama")) {

                ArrayList<Vector> vecArr = new ArrayList<>();
                Connection connection = null;
                PreparedStatement statement = null;
                connection = DriverManager.getConnection(DATABASE_URL, username, password);
                statement = connection.prepareStatement("select DvdNumber, Title from Dvd where Category='Drama' AND AvailableForRent=true");
                ResultSet rs = statement.executeQuery();

                ResultSetMetaData rd = rs.getMetaData();
                int a = rd.getColumnCount();

                while (rs.next()) {
                    Vector vect = new Vector();

                    for (int i = 1; i <= 1; i++) {

                        vect.add(rs.getString("dvdNumber"));
                        vect.add(rs.getString("title"));

                    }
                    vecArr.add(vect);

                }
                output.writeObject(vecArr);
                output.flush();
                //output.close();
            } else if (message.equalsIgnoreCase("romance")) {

                ArrayList<Vector> vecArr = new ArrayList<>();
                Connection connection = null;
                PreparedStatement statement = null;
                connection = DriverManager.getConnection(DATABASE_URL, username, password);
                statement = connection.prepareStatement("select DvdNumber, Title from Dvd where Category='Romance' AND AvailableForRent=true");
                ResultSet rs = statement.executeQuery();

                ResultSetMetaData rd = rs.getMetaData();
                int a = rd.getColumnCount();

                while (rs.next()) {
                    Vector vect = new Vector();

                    for (int i = 1; i <= 1; i++) {

                        vect.add(rs.getString("dvdNumber"));
                        vect.add(rs.getString("title"));

                    }
                    vecArr.add(vect);

                }
                output.writeObject(vecArr);
                output.flush();
                //output.close();
            } else if (message.equalsIgnoreCase("comedy")) {

                ArrayList<Vector> vecArr = new ArrayList<>();
                Connection connection = null;
                PreparedStatement statement = null;
                connection = DriverManager.getConnection(DATABASE_URL, username, password);
                statement = connection.prepareStatement("select DvdNumber, Title from Dvd where Category='Comedy' AND AvailableForRent=true");
                ResultSet rs = statement.executeQuery();

                ResultSetMetaData rd = rs.getMetaData();
                int a = rd.getColumnCount();

                while (rs.next()) {
                    Vector vect = new Vector();

                    for (int i = 1; i <= 1; i++) {

                        vect.add(rs.getString("dvdNumber"));
                        vect.add(rs.getString("title"));

                    }
                    vecArr.add(vect);

                }
                output.writeObject(vecArr);
                output.flush();
                //output.close();
            } else if (message.equalsIgnoreCase("action")) {

                ArrayList<Vector> vecArr = new ArrayList<>();
                Connection connection = null;
                PreparedStatement statement = null;
                connection = DriverManager.getConnection(DATABASE_URL, username, password);
                statement = connection.prepareStatement("select DvdNumber, Title from Dvd where Category='Action' AND AvailableForRent=true");
                ResultSet rs = statement.executeQuery();

                ResultSetMetaData rd = rs.getMetaData();
                int a = rd.getColumnCount();

                while (rs.next()) {
                    Vector vect = new Vector();

                    for (int i = 1; i <= 1; i++) {

                        vect.add(rs.getString("dvdNumber"));
                        vect.add(rs.getString("title"));

                    }
                    vecArr.add(vect);

                }
                output.writeObject(vecArr);
                output.flush();
                //output.close();
            } else if (message.equalsIgnoreCase("cartoons")) {

                ArrayList<Vector> vecArr = new ArrayList<>();
                Connection connection = null;
                PreparedStatement statement = null;
                connection = DriverManager.getConnection(DATABASE_URL, username, password);
                statement = connection.prepareStatement("select DvdNumber, Title from Dvd where Category='Cartoon' AND AvailableForRent=true");
                ResultSet rs = statement.executeQuery();

                ResultSetMetaData rd = rs.getMetaData();
                int a = rd.getColumnCount();

                while (rs.next()) {
                    Vector vect = new Vector();

                    for (int i = 1; i <= 1; i++) {

                        vect.add(rs.getString("dvdNumber"));
                        vect.add(rs.getString("title"));

                    }
                    vecArr.add(vect);

                }
                output.writeObject(vecArr);
                output.flush();
                //output.close();
            } else if (message.equalsIgnoreCase("customernumber")) {
                //input.close();
                Customer c = new Customer();
                while ((c = (Customer) input.readObject()) != null) {
                    System.out.println("Recieving data...");
                    CustomerDAO custDao = new CustomerDAO();
                    c.getCustNumber();
                    // Recieve object!
                    System.out.println("Customer object recieved from Client");
                    custDao.updateCanRent(c);
                    //input.close();

                }
            } else if (message.equalsIgnoreCase("dvdnumber")) {
                //input.close();
                DVD dvd = new DVD();
                while ((dvd = (DVD) input.readObject()) != null) {
                    System.out.println("Recieving data...");
                    DvdDAO dvdDao = new DvdDAO();
                    RentalsDAO rentalDAO = new RentalsDAO();
                    dvd.getDvdNumber();
                    dvdNr = dvd.getDvdNumber();
                    // Recieve object!
                    System.out.println("Dvd object recieved from Client");
                    dvdDao.updateAvailableForRent(dvd);
                    //input.close();
                }
            } else if (message.equalsIgnoreCase("addrental")) {

                Rental r = new Rental();
                RentalsDAO rdao = new RentalsDAO();
                while ((r = (Rental) input.readObject()) != null) {
                    System.out.println("Recieving data...");
                    r.getRentalNumber();
                    r.getDateRented();
                    r.getDateReturned();
                    r.getDvdNumber();
                    r.getCustNumber();
                    r.getTotalPenaltyCost();
                    //Recieve object!
                    System.out.println("Rental object recieved from Client");
                    rdao.addRental(r);
                    //input.close();
                }
            } else if (message.equalsIgnoreCase("rentnum")) {
                //input.close();
                Rental re = new Rental();
                RentalsDAO rd = new RentalsDAO();
                while ((re = (Rental) input.readObject()) != null) {
                System.out.println("Recieving data...");
                //System.out.println(re);
                re.getRentalNumber();
                rd.updateCustomer(re);
                System.out.println("DVD object recieved from Client");
                // Recieve object!
                //input.close();
                }

            } else if (message.equalsIgnoreCase("updatecustomer")) {

                Connection connection = null;
                PreparedStatement statement = null;
                String sql = "UPDATE Customer SET CanRent = ? WHERE CustNumber = ?";
                int ok;

                try {
                    //connect to database book and query database
                    Class.forName("org.apache.derby.jdbc.ClientDriver");
                } catch (ClassNotFoundException ex) {
                    //Logger.getLogger(RegistrationGUI.class.getName()).log(Level.SEVERE, null, ex);
                }

                try {

                    connection = DriverManager.getConnection(DATABASE_URL, username, password);

                    statement = connection.prepareStatement(sql);
                    statement.setBoolean(1, true);
                    statement.setInt(2, custNr);
                    System.out.println(custNr);

                    ok = statement.executeUpdate();
                    if (ok > 0) {
                        JOptionPane.showMessageDialog(null, "Customer table updated.");
                        System.exit(0);
                    } else {
                        JOptionPane.showMessageDialog(null, "Error: Failed to update table.");
                    }
                } catch (SQLException sqlException) {
                    JOptionPane.showMessageDialog(null, "Error: Failed to update table. " + sqlException);
                } finally {
                    // Method 1
                    try {
                        if (statement != null) {
                            statement.close();
                        }
                    } catch (SQLException exception) {
                        JOptionPane.showMessageDialog(null, exception.getMessage(), "Warning", JOptionPane.ERROR_MESSAGE);
                    }
                    try {
                        if (connection != null) {
                            connection.close();
                        }
                    } catch (SQLException exception) {
                        JOptionPane.showMessageDialog(null, exception.getMessage(), "Warning", JOptionPane.ERROR_MESSAGE);
                    }
                }
            } else if (message.equalsIgnoreCase("displayallrentals")) {

                ArrayList<Vector> vecArr = new ArrayList<>();
                Connection connection = null;
                PreparedStatement statement = null;
                connection = DriverManager.getConnection(DATABASE_URL, username, password);
                statement = connection.prepareStatement("select * from Rental Order By DateRented ASC");
                ResultSet rs = statement.executeQuery();

                ResultSetMetaData rd = rs.getMetaData();
                int a = rd.getColumnCount();

                while (rs.next()) {
                    Vector vect = new Vector();

                    for (int i = 1; i <= a; i++) {
                        vect.add(rs.getInt("rentalNumber"));
                        vect.add(rs.getString("dateRented"));
                        vect.add(rs.getString("dateReturned"));
                        vect.add(rs.getInt("custNumber"));
                        vect.add(rs.getInt("dvdNumber"));
                        vect.add(rs.getDouble("totalPenaltyCost"));

                    }
                    vecArr.add(vect);
                }
                output.writeObject(vecArr);
                output.flush();
            } else if (message.equalsIgnoreCase("unreturnedmovies")) {

                ArrayList<Vector> vecArr = new ArrayList<>();
                Connection connection = null;
                PreparedStatement statement = null;
                connection = DriverManager.getConnection(DATABASE_URL, username, password);
                statement = connection.prepareStatement("select * from Rental where DateReturned = 'NA'");
                ResultSet rs = statement.executeQuery();

                ResultSetMetaData rd = rs.getMetaData();
                int a = rd.getColumnCount();

                while (rs.next()) {
                    Vector vect = new Vector();

                    for (int i = 1; i <= a; i++) {
                        vect.add(rs.getInt("rentalNumber"));
                        vect.add(rs.getString("dateRented"));
                        vect.add(rs.getString("dateReturned"));
                        vect.add(rs.getInt("custNumber"));
                        vect.add(rs.getInt("dvdNumber"));
                        vect.add(rs.getDouble("totalPenaltyCost"));

                    }

                    vecArr.add(vect);
                }
                output.writeObject(vecArr);
                output.flush();
            } else if (message.equalsIgnoreCase("dayrentals")) {

                ArrayList<Vector> vecArr = new ArrayList<>();
                Connection connection = null;
                PreparedStatement statement = null;
                connection = DriverManager.getConnection(DATABASE_URL, username, password);
                statement = connection.prepareStatement("select * from Rental WHERE DateRented = '2020-11-07'");
                ResultSet rs = statement.executeQuery();

                ResultSetMetaData rd = rs.getMetaData();
                int a = rd.getColumnCount();

                while (rs.next()) {
                    Vector vect = new Vector();

                    for (int i = 1; i <= a; i++) {
                        vect.add(rs.getInt("rentalNumber"));
                        vect.add(rs.getString("dateRented"));
                        vect.add(rs.getString("dateReturned"));
                        vect.add(rs.getInt("custNumber"));
                        vect.add(rs.getInt("dvdNumber"));
                        vect.add(rs.getDouble("totalPenaltyCost"));

                    }

                    vecArr.add(vect);
                }
                output.writeObject(vecArr);
                output.flush();
            }

            System.out.println("sent");

            input.close();
            output.close();
            s1.close();

        } catch (SQLException ex) {
            Logger.getLogger(ServerThread.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(ServerThread.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(ServerThread.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
