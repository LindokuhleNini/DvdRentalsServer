/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dvdrentalsserver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import javax.swing.JOptionPane;
import modelclasses.Rental;

/**
 *DvdRentalsClient.java
 * This is our DVD Rentals Client Server Program
 * @author Lindokuhle Nini (218196504)
 * @author Sihle Jijana (216273919)
 * Date: 08 November 2020
 */
public class RentalsDAO {
    
    static final String DATABASE_URL = "jdbc:derby://localhost:1527/DvdRentalDB";
    static final String username = "Administrator";
    static final String password = "Password";
    int custNr = 0;

    
    public Rental addRental(Rental rental) {

        Connection connection = null;
        PreparedStatement statement = null;
        String sql = "INSERT INTO Rental VALUES (?, ?, ?, ?, ?, ?)";
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
            statement.setInt(1, rental.getRentalNumber());
            statement.setString(2, rental.getDateRented());
            statement.setString(3, rental.getDateReturned());
            statement.setDouble(4, rental.getDvdNumber());
            statement.setDouble(5, rental.getTotalPenaltyCost());
            statement.setDouble(6, rental.getCustNumber());
            
            ok = statement.executeUpdate();
            if (ok > 0) {
                JOptionPane.showMessageDialog(null, "Rental table updated.");
                System.exit(0);
            } else {
                JOptionPane.showMessageDialog(null, "Error: Failed to update rental table.");
            }
        } catch (SQLException sqlException) {
            JOptionPane.showMessageDialog(null, "Error: Failed to update rental table. " + sqlException);
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

        
    return rental;
}
    
    public int rentalNumFromClient(int rental) {

        Connection connection = null;
        PreparedStatement statement = null;
        String sql = "SELECT CustNumber FROM Rental WHERE RentalNumber = ?";
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
            statement.setInt(1, rental);
            
            ResultSet rs = statement.executeQuery();
                 
                 ResultSetMetaData rd = rs.getMetaData();
                 int a = rd.getColumnCount();
                 
            while (rs.next()) {
                     
                int numberOfCustomer = 0;
                     for (int i = 1; i<= 1; i++) {
                         
                         numberOfCustomer = rs.getInt("dvdNumber");
                         
                     }
                     custNr = numberOfCustomer;
                     
                 }
            
            ok = statement.executeUpdate();
            if (ok > 0) {
                JOptionPane.showMessageDialog(null, "Customer table updated.");
                System.exit(0);
            } else {
                JOptionPane.showMessageDialog(null, "Error: Failed to update customer table.");
            }
        } catch (SQLException sqlException) {
            JOptionPane.showMessageDialog(null, "Error: Failed to update customer table. " + sqlException);
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

        
    return rental;
}
    
    public Rental updateCustomer(Rental r) {

        Connection connection = null;
        PreparedStatement statement = null;
        String sql = "UPDATE Customer SET CanRent = ? WHERE Rental.CustomerNumber = ?";
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
            statement.setInt(2, r.getCustNumber());
            
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

        return r;

}
    public Rental updateDvd(Rental r) {

        Connection connection = null;
        PreparedStatement statement = null;
        String sql = "UPDATE Dvd SET AvailableForRent = ? WHERE Rental.CustomerNumber = ?";
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
            statement.setInt(2, r.getCustNumber());
            
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

        return r;
    }

}