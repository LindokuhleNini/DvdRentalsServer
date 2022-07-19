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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.swing.JOptionPane;
import javax.swing.table.DefaultTableModel;
import modelclasses.Customer;

/**
 *DvdRentalsClient.java
 * This is our DVD Rentals Client Server Program
 * @author Lindokuhle Nini (218196504)
 * @author Sihle Jijana (216273919)
 * Date: 08 November 2020
 */
public class CustomerDAO {

    static final String DATABASE_URL = "jdbc:derby://localhost:1527/DvdRentalDB";
    static final String username = "Administrator";
    static final String password = "Password";

    public Customer saveCustomer(Customer c) {

        Connection connection = null;
        PreparedStatement statement = null;
        String sql = "INSERT INTO Customer VALUES (?, ?, ?, ?, ?, ?)";
        int ok;
        //String pstm = ("SELECT * FROM Customer");

        try {
            //connect to database book and query database
            Class.forName("org.apache.derby.jdbc.ClientDriver");
        } catch (ClassNotFoundException ex) {
            //Logger.getLogger(RegistrationGUI.class.getName()).log(Level.SEVERE, null, ex);
        }

        try {

            connection = DriverManager.getConnection(DATABASE_URL, username, password);

            statement = connection.prepareStatement(sql);
            statement.setInt(1, c.getCustNumber());
            statement.setString(2, c.getName());
            statement.setString(3, c.getSurname());
            statement.setString(4, c.getPhoneNum());
            statement.setDouble(5, c.getCredit());
            statement.setBoolean(6, c.canRent());

            ok = statement.executeUpdate();
            if (ok > 0) {
                JOptionPane.showMessageDialog(null, "Success! New customer added.");
                System.exit(0);
            } else {
                JOptionPane.showMessageDialog(null, "Error: Failed to add new customer.");
            }
        } catch (SQLException sqlException) {
            JOptionPane.showMessageDialog(null, "Error: Failed to add new customer. " + sqlException);
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

        return c;
    }

    public List<Customer> displayCustomers(List<Customer> list) {

        Connection connection = null;
        PreparedStatement statement = null;
        int ok;
        list = new ArrayList<>();

        try {

            connection = DriverManager.getConnection(DATABASE_URL, username, password);

            statement = connection.prepareStatement("SELECT * FROM Customer");
            ResultSet Rs = statement.executeQuery();

            ok = 0;

            int columnCount = Rs.getMetaData().getColumnCount();
            while (Rs.next()) {
                List<Customer> row = new ArrayList<>();
                for (int i = 1; i <= columnCount; i++) {

                    row.add((Customer) Rs.getObject(i));

                }
                list.add((Customer) row);
                System.out.println(row);

            }

            JOptionPane.showMessageDialog(null, "Success! Customer detils retrieved.");
            System.exit(0);

        } catch (SQLException sqlException) {
            JOptionPane.showMessageDialog(null, "Error: Failed to retrieve customer details." + sqlException);
        } finally {
            // Method 1
            try {
                if (statement != null) {
                    statement.close();
                }
            } catch (Exception exception) {
                JOptionPane.showMessageDialog(null, exception.getMessage(), "Warning", JOptionPane.ERROR_MESSAGE);
            }
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (Exception exception) {
                JOptionPane.showMessageDialog(null, exception.getMessage(), "Warning", JOptionPane.ERROR_MESSAGE);
            }
        }

        return list;
    }

    public Customer updateCanRent(Customer c) {

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
            statement.setBoolean(1, false);
            statement.setInt(2, c.getCustNumber());

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

        return c;

    }
}
