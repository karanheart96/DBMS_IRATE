import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * This class has DDL functionality for the Movie Rating database.
 * It contains functions to create and drop the tables, functions, and procedures.
 *
 */
public class irate_DDL {
  private static Statement s = null;

  /**
   * Creates an instance of the class that has the sql statement variable for irate
   * @param s the sql statement
   */
  public irate_DDL(Statement s){

    this.s = s;
  }


  /**
   * Create stored functions in irate.
   */
  public void createFunctions() {
    int success = 0;
    try {
      String func_isStar = "CREATE FUNCTION isStar(" +
        "Rating INTEGER" +
        ") RETURNS BOOLEAN" +
        " PARAMETER STYLE JAVA" +
        " LANGUAGE JAVA" +
        " DETERMINISTIC" +
        " NO SQL" +
        " EXTERNAL NAME" +
        " 'DBMS_storedfunc.isStar' ";
      s.executeUpdate(func_isStar);
      success++;
    }catch (SQLException e) {
      System.err.println("Did not create function isStar"+e.getMessage());
    }

    try {
      String func_isEndorvalid = "CREATE FUNCTION isEndorvalid(" +
        "CUST_ID INTEGER" +
        "REVIEW_ID INTEGER" +
        ") RETURNS BOOLEAN" +
        " PARAMETER STYLE JAVA" +
        " LANGUAGE JAVA" +
        " DETERMINISTIC" +
        " NO SQL" +
        " EXTERNAL NAME" +
        " 'DBMS_storedfunc.isEndorvalid' ";
      s.executeUpdate(func_isEndorvalid);
      success++;
    }catch (SQLException e) {
      System.err.println("Did not create function isEndor" + e.getMessage());
    }

    try {
      String func_isRevdate = "CREATE FUNCTION isRevdate(" +
        "Review_date DATE" +
        ") RETURNS BOOLEAN" +
        " PARAMETER STYLE JAVA" +
        " LANGUAGE JAVA" +
        " DETERMINISTIC" +
        " NO SQL" +
        " EXTERNAL NAME" +
        " 'DBMS_storedfunc.isRevdate'";
      s.executeUpdate(func_isRevdate);
      success++;
    }catch (SQLException e) {
      System.err.println("Did not create function isRevdate" + e.getMessage());
    }

    try {
      String func_isEndordate = "CREATE FUNCTION isEndordate(" +
        "CUST_ID INTEGER"+
        "Movie_ID INTEGER" +
        "Endorse_date DATE" +
        ") RETURNS BOOLEAN" +
        "PARAMETER STYLE JAVA"+
        "LANGUAGE JAVA" +
        "DETERMINISTIC"+
        "NO SQL"+
        "EXTERNAL NAME"+
        "'DBMS_storedfunc.isEndordate'";
      s.executeUpdate(func_isEndordate);
      success++;
    }catch (SQLException e) {
      System.err.println("Did not create function isEndordate"+e.getMessage());
    }
  }

  /**
   * Creates the tables for the irate database.
   * Created tables: customer, movie, attendance,review, and endorsement.
   */
  public void createTables() {
    int success = 0;
    try {
      s.executeUpdate("create table customer(" +
          "CUST_ID INTEGER NOT NULL UNIQUE GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) ," +
          "Name VARCHAR(32) NOT NULL," +
          "Email VARCHAR(32) NOT NULL," +
          "Address VARCHAR(64) NOT NULL," +
          "register_date DATE NOT NULL," +
          "PRIMARY KEY(CUST_ID)" +
        ")"
        );
      success++;

    }catch (SQLException e) {
      System.err.println("Unable to create customer table"+e.getMessage());
    }

    try {
      s.executeUpdate("create table movie(" +
        "Title VARCHAR(32) NOT NULL," +
        "Movie_ID INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1)," +
        "PRIMARY KEY(Movie_ID)" +
        ")"
      );
      success++;
    }catch (SQLException e) {
      System.err.println("Unable to create movie table"+e.getMessage());
    }

    try {
      s.executeUpdate("create table attendance(" +
        "Movie_ID INTEGER NOT NULL references movie(Movie_ID) ON DELETE CASCADE," +
        "Atten_date Date NOT NULL," +
        "CUST_ID INTEGER NOT NULL references customer(CUST_ID)" +
        ")"
      );
      success++;

    }catch (SQLException e) {
      System.err.println("Unable to create attendance table"+e.getMessage());
    }

    try {
      s.executeUpdate("create table review(" +
        "CUST_ID INTEGER NOT NULL references customer(CUST_ID) ON DELETE CASCADE," +
        "Movie_ID INTEGER NOT NULL references movie(Movie_ID) ON DELETE CASCADE," +
        "Review_date DATE NOT NULL," +
        "Rating INTEGER NOT NULL," +
        "Review VARCHAR(1000) NOT NULL," +
        "Review_ID INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1)," +
        "PRIMARY KEY(Review_ID)," +
        "CHECK (isStar(Rating))," +
        "CHECK (isRevdate(Review_date,CUST_ID,Movie_ID))"+
        ")"
      );
      success++;

    }catch (SQLException e) {
      System.err.println("Unable to create review table"+e.getMessage());
    }

    try {
      s.executeUpdate("create table endorsement(" +
        "Review_ID INTEGER NOT NULL references review(Review_ID) ON DELETE CASCADE," +
        "CUST_ID INTEGER NOT NULL references customer(CUST_ID) ON DELETE CASCADE," +
        "Endorse_date DATE NOT NULL," +
        "Movie_ID INTEGER NOT NULL references movie(Movie_ID) ON DELETE CASCADE,"+
        "CHECK (isEndorvalid(CUST_ID,Review_ID))," +
        "CHECK (isEndordate(CUST_ID,Movie_ID,Endorse_date))"+
        ")"
      );
      success++;

    }catch (SQLException e) {
      System.err.println("Unable to create endorsement table"+e.getMessage());
    }
  }

  /**
   * Drops all tables in irate
   */
  public void dropTables(String dbTables[]){
    // Drops tables if they already exist
    int dropped = 0;
    for(String table : dbTables) {
      try {
        s.execute("drop table " + table);
        dropped++;
      } catch (SQLException e) {
        System.out.println("did not drop " + table);
      }
    }
    if(dropped == dbTables.length){
      System.out.println("Successfully dropped all tables.");
    }
  }

  /**
   * Drops all functions in irate
   */
  public void dropFunctions(String dbFunctions[]){
    // Drop functions if they already exist
    int dropped = 0;
    for(String fn : dbFunctions) {
      try {
        s.execute("drop function " + fn);
        dropped++;
      } catch (SQLException e) {
        System.out.println("did not drop " + fn);
      }
    }
    if(dropped == dbFunctions.length) {
      System.out.println("Successfully dropped all functions.");
    }
  }

  /**
   * Drops all triggers in irate
   */
  public void dropTriggers(String dbTriggers[]){
    //Drops triggers if they already exists.
    for(String tri : dbTriggers) {
      try {
        s.execute("drop trigger " + tri);
        System.out.println("dropped trigger " + tri);
      } catch (SQLException e) {
        System.out.println("did not drop trigger " + tri);
      }
    }
  }


  /**
   * Drops all procedures in irate (currently unused, no procedures)
   */
  public void dropProcedures(String dbProcedures[]) {
    // Drops procedures if they already exist
    for(String proc : dbProcedures) {
      try {
        s.execute("drop procedure " + proc);
        System.out.println("dropped procedure " + proc);
      } catch (SQLException e) {
        System.out.println("did not drop procedure " + proc);
      }
    }
  }

  /**
   * Creates triggers for Movie Rating (currently no triggers ,feel free to include your own if you feel so)
   */
  public void createTriggers() {

  }
}
