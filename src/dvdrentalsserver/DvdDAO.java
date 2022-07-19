/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dvdrentalsserver;

import static dvdrentalsserver.CustomerDAO.DATABASE_URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import javax.swing.JOptionPane;
import modelclasses.Customer;
import modelclasses.DVD;

/**
 *DvdRentalsClient.java
 * This is our DVD Rentals Client Server Program
 * @author Lindokuhle Nini (218196504)
 * @author Sihle Jijana (216273919)
 * Date: 08 November 2020
 */
public class DvdDAO {

    static final String DATABASE_URL = "jdbc:derby://localhost:1527/DvdRentalDB";
    static final String username = "Administrator";
    static final String password = "Password";

    public DVD saveDvd(DVD dvd) {

        Connection connection = null;
        PreparedStatement statement = null;
        String sql = "INSERT INTO Dvd VALUES (?, ?, ?, ?, ?, ?)";
        int ok;

        int dvdNumber = Integer.parseInt("" + dvd.getDvdNumber());
        String title = dvd.getTitle();
        String category = dvd.getCategory();
        double price = Double.parseDouble("" + dvd.getPrice());
        boolean newRelease = dvd.isNewRelease();
        boolean availableForRent = dvd.isAvailable();

        try {
            Class.forName("org.apache.derby.jdbc.ClientDriver");
        } catch (ClassNotFoundException ex) {

        }

        try {
            connection = DriverManager.getConnection(DATABASE_URL, username, password);

            statement = connection.prepareStatement(sql);
            statement.setInt(1, dvd.getDvdNumber());
            statement.setString(2, dvd.getTitle());
            statement.setString(3, dvd.getCategory());
            statement.setDouble(4, dvd.getPrice());
            statement.setBoolean(5, dvd.isNewRelease());
            statement.setBoolean(6, dvd.isAvailable());

            ok = statement.executeUpdate();
            if (ok > 0) {
                JOptionPane.showMessageDialog(null, "Success! New dvd added.");
                System.exit(0);
            } else {
                JOptionPane.showMessageDialog(null, "Error: Failed to add new dvd.");
                //txtCricketCode.hasFocus();
            }
        } catch (SQLException sqlException) {
            JOptionPane.showMessageDialog(null, "Error: Failed to add new dvd. " + sqlException);
            // txtCricketCode.hasFocus();
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

        return dvd;
    }
    
    public DVD updateAvailableForRent(DVD dvd) {

        Connection connection = null;
        PreparedStatement statement = null;
        String sql = "UPDATE Dvd SET AvailableForRent = ? WHERE DvdNumber = ?";
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
            statement.setBoolean(1, false);
            statement.setInt(2, dvd.getDvdNumber());
            
            ok = statement.executeUpdate();
            if (ok > 0) {
                JOptionPane.showMessageDialog(null, "Dvd table updated.");
                System.exit(0);
            } else {
                JOptionPane.showMessageDialog(null, "Error: Failed to update dvd table.");
            }
        } catch (SQLException sqlException) {
            JOptionPane.showMessageDialog(null, "Error: Failed to update dvd table. " + sqlException);
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

        return dvd;

}
    
    public DVD returnDvd(DVD dvd) {

        Connection connection = null;
        PreparedStatement statement = null;
        String sql = "UPDATE Dvd SET AvailableForRent = ? WHERE DvdNumber = ?";
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
            statement.setBoolean(1, false);
            statement.setInt(2, dvd.getDvdNumber());
            
            ok = statement.executeUpdate();
            if (ok > 0) {
                JOptionPane.showMessageDialog(null, "Dvd table updated.");
                System.exit(0);
            } else {
                JOptionPane.showMessageDialog(null, "Error: Failed to update dvd table.");
            }
        } catch (SQLException sqlException) {
            JOptionPane.showMessageDialog(null, "Error: Failed to update dvd table. " + sqlException);
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

        return dvd;

}
    
}