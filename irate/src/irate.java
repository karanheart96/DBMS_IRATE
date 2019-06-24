import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Properties;

/**
 * irate: A Database for Movie reviews and Ticket sales
 * @author Karan Machado
 *
 * The RDBMS maintains information about movies and reviews by the customers,
 * tracks sales of tickets,reviews for a movie and the endorsements for reviews.
 * It enables a theater business to track sales of tickets, and allows customers to enter a review for a movie.
 *
 */
public class irate {
  // connection variables
  private static Connection conn = null;
  private static Statement s = null;

  /** Name of the database */
  private static String dbName = "irate";

  /** Contains the names of the tables in irate */
  private static String dbTables[]= {
    "customer", "movie", "attendance",
    "review", "endorsement"
  };

  /** Contains names of stored functions in irate */
  private static String dbFunctions[]={
    "isStar", "isEndorvalid", "isRevdate",
    "isEndordate"
  };

  /* Contains names of triggers in irate (unused)*/
  private static String dbTriggers[] = {
    //none
  };

  /* Contains names of procedures in irate (unused)*/
  private static String dbProcedures[] = {
    // none
  };

  /** Initializes the database and creates the tables */
  public static void main(String[] args)
  {
    // start connection
    irate_Connection irate = new irate_Connection();
    irate.startConnection("user1", "password", dbName);
    conn = irate.getConnection();
    s = irate.getStatement();


    irate_DDL ddl = new irate_DDL(s);
    irate_DML dml = new irate_DML(conn, s);
    irate_DQL dql = new irate_DQL(conn, s);
    irate_API api = new irate_API(dml, dql);

    //Feel free to use api functions for inserting your own values into the database or for testing purposes.


    System.out.println("Initializing database...");
    System.out.println("\nDropping tables and functions and triggers:");
    ddl.dropTables(dbTables);
    ddl.dropFunctions(dbFunctions);
    // trigger drop not needed, because dropping tables drops the triggers as well
    // ddl.dropTriggers(s, dbTriggers);


    System.out.println("\nCreating functions:");
    ddl.createFunctions();


    System.out.println("\nCreating tables:");
    ddl.createTables();

    System.out.println("\nCreating triggers:");
    ddl.createTriggers();

    // truncate, then insert data into tables
    System.out.println("\nTruncating tables:");
    dml.truncateTables(dbTables);
    System.out.println("\nInserting values:");
    dml.insertAll();

    //For testing purposes.
    System.out.println("Viewing all customers");
    dql.selectAllCustomer();
    System.out.println("Viewing all movies");
    dql.selectAllMovie();
    System.out.println("Viewing all reviews");
    dql.selectAllReview();
    System.out.println("Viewing all endorsement");
    dql.selectAllEndorsement();
    System.out.println("Viewing all attendance");
    dql.selectAllAttendance();

    // close connection
    irate.closeConnection(dbName);

  }

}