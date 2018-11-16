package restaurantmanagement;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Thomas Dahl - thom15m6@stud.kea.dk
 */
public class DB_Statement {
    
    private static Statement stmt = null; //Declare a statement
    private static PreparedStatement pStmt = null; //Declare a prepared statement
    private static final Connection CONNECTION = DB_Connector.connect(); //Instantiate a connection
    private static ResultSet rs = null;//Declare a ResultSet
    
    
    /**
     * Generic handling of SQL queries.
     * @param query SQL query to be executed.
     * @throws SQLException 
     */
    private void statement(String query) throws SQLException {
        try{
            stmt = CONNECTION.createStatement();
            stmt.executeUpdate(query);
            
        } catch (SQLException ex){
            System.out.println("Query did not execute");
            throw ex;
        }
    }
    
    /**
     * Generates SQL query to use a given Database.
     * @param dBName Name of the Database
     */
    public void useDB(String dBName) {
        String query = "USE " + dBName;
        try{
            statement(query);
        } catch (SQLException ex){
            ex.printStackTrace();
        }
    }
    
    /**
     * Generates SQL query to create a new table.
     * @param tableName Name of the table.
     * @param columns Names of the columns of the table.
     */
    public void createTable(String tableName, String[] columns) {
        
        String query = "CREATE TABLE " + tableName + " (";
        
        for(String col : columns) {
            query += col;
            query += ",";
        }
        query = query.substring(0, query.length()-1);
        query += ")";
        
        try{
            statement(query);
        } catch (SQLException ex){
            System.out.println("Table '" + tableName + "' already exists");
            ex.printStackTrace();
        }
        
    }
    
    /**
     * Generates SQL query to insert a tuple into given columns in given table.
     * @param tableName
     * @param colNames
     * @param tuple
     */
    public void insertIntoTable(String tableName, String[] colNames, String[] tuple) {
        
        String query = "INSERT INTO " + tableName + " (";
        
        for (String col : colNames) {
            query += col + ",";
        }
        query = query.substring(0, query.length()-1);
        query += ") values (";
        
        for (String value : tuple) {
            query += "'" + value + "'" + ",";
        }
        query = query.substring(0, query.length()-1);
        query += ")";
        
        try{
            statement(query);
        } catch (SQLException ex){
            ex.printStackTrace();
        }
        
    }
    
    /**
     * Calls method "selectColumnsFromTableWhere" with no specified columns or conditions.
     * @param tableName Name of the table to direct the query at.
     * @return 
     */
    public ResultSet selectAllFromTable(String tableName) {
        String[] columns = {"*"};
        return selectColumnsFromTableWhere(columns, tableName, null);
    }
    
    /**
     * Calls method "selectColumnsFromTableWhere" with no specified columns.
     * @param tableName Name of the table to direct the query at.
     * @param condition SQL Query conditions.
     * @return A java.sql.ResultSet corresponding to the given query.
     */
    public ResultSet selectAllFromTableWhere(String tableName, String condition) {
        String[] columns = {"*"};
        return selectColumnsFromTableWhere(columns, tableName, condition);
    }
    
    /**
     * Calls method "selectColumnsFromTableWhere" with no conditions.
     * @param columns Name(s) of the wanted columns.
     * @param tableName Name of the table to direct the query at.
     * @return A java.sql.ResultSet corresponding to the given query.
     */
    public ResultSet selectColumnsFromTable(String[] columns, String tableName) {
        return selectColumnsFromTableWhere(columns, tableName, null);
    }
    
    /**
     * Generates a SELECT SQL query with the given columns, tableName and conditions.
     * @param columns Name(s) of the wanted columns.
     * @param tableName Name of the table to direct the query at.
     * @param condition SQL Query conditions.
     * @return A java.sql.ResultSet corresponding to the given query.
     */
    public ResultSet selectColumnsFromTableWhere(String[] columns, String tableName, String condition) {
        
        String query = "SELECT ";
        
        for(String col : columns) {
            query += col + ", ";
        }
        query = query.substring(0, query.length()-2);
        query += "FROM " + tableName;
        
        if(condition != null) {
            query += "WHERE " + condition;
        }
        
        try{
            
            stmt = CONNECTION.createStatement();
            return stmt.executeQuery(query);
            
        } catch(SQLException ex) {
            System.out.println("Query did not execute");
            ex.printStackTrace();
        }
        
        return null;
        
    }
    
    /**
     * Generates a string-representation of a given java.sql.ResultSet.
     * @param rs The java.sql.ResultSet to be represented as a table-formatted string.
     * @return A table-formatted string representation of the ResultSet.
     * @throws SQLException 
     */
    public String resultSetToString(ResultSet rs) throws SQLException {
        
        //=========== SAVE RESULTSET IN 2-DIMENSIONAL ARRAYLIST =============
        
        ResultSetMetaData rsmd = rs.getMetaData();
        int numCol = rsmd.getColumnCount(); //Amount of columns in rs
        List<List<String>> relation = new ArrayList<>(); //2D ArrayList to hold the values.
        relation.add(new ArrayList<>());
        int activeTuple = 0;
        
        //Add ColumnNames
        for(int i = 1; i <= numCol; i++) {
            relation.get(activeTuple).add(rsmd.getColumnName(i));
        }
        
        //Add Tuples
        while(rs.next()) {
            relation.add(new ArrayList<>());
            activeTuple++;
            String value;
            
            for(int i = 1; i <= numCol; i++) {
                if(rs.getObject(i) != null) {
                    value = rs.getObject(i).toString();
                } else {
                    value = "";
                }
                relation.get(activeTuple).add(value);
            }
            
        }
        
        //================== COLUMN SIZING INFORMATION  ====================
        
        int[] colWidths = new int[numCol]; //Array to hold to max colWidth values.
        activeTuple = 0;
        for(List<String> tuple : relation) {
            for(int i = 0; i < numCol; i++) {
                int valLength = relation.get(activeTuple).get(i).length();
                colWidths[i] = Math.max(valLength, colWidths[i]);
            }
            activeTuple++;
        }
        
        //=================== CONSTRUCTING THE STRING  =====================
        
        //Horizontal lines
        String hLine = "+";
        for(int i = 0; i < numCol; i++) {
            int numDashes = colWidths[i] + 4;
            hLine += repeat("-",numDashes) + "+";
        }
        String table = hLine + "\n";
        
        //Add Column Names
        for(int i = 0; i < numCol; i++) {
            table += "|  ";
            String value = relation.get(0).get(i);
            int numSpaces = colWidths[i] - value.length();
            int spacesBefore = numSpaces / 2;
            int spacesAfter = numSpaces - spacesBefore;
            table += repeat(" ", spacesBefore);
            table += value;
            table += repeat(" ", spacesAfter);
            table += "  ";
        }
        table += "|\n" + hLine + "\n";
        
        //Add the tuples
        for(activeTuple = 1; activeTuple < relation.size(); activeTuple++) {
            for(int i = 0; i < numCol; i++) {
                table += "|  ";
                String value = relation.get(activeTuple).get(i);
                int numSpaces = colWidths[i] - value.length();
                table += repeat(" ", numSpaces);
                table += value + "  ";
            }
            table += "|\n";
        }
        table += hLine;
        
        return table;
    }
    
    /**
     * Repeats a given string a multiple of times.
     * @param string String to be repeated.
     * @param times Number of times the string should repeat.
     * @return a concatenation of the given string 'times' times.
     */
    private String repeat(String string, int times) {
        String newString = "";
        for(int i = 0; i < times; i++) {
            newString += string;
        }
        
        return newString;
    }

}