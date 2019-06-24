import java.io.FileNotFoundException;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Date;

/**
 * Inserts and drops data from the irate database.
 */
public class irate_DML {
  // database connection variables
  private static Connection conn = null;
  private static Statement s = null;
  private static String customer = "data/Customer.txt";
  private static String moview = "data/Movie.txt";
  private static String attendance = "data/Attendance.txt";
  private static String review = "data/Review.txt";
  private static String endorse = "data/Endorsement.txt";
  private static String insertIntoCustomer = "insert into customer(Name,Email,Address,register_date) values(?,?,?,?)";
  private static String insertIntoMovie = "insert into movie(Title) values(?)";
  private static String insertIntoattendance = "insert into attendance(Movie_ID,Atten_date,CUST_ID) values(?,?,?)";
  private static String insertIntoReview = "insert into review(CUST_ID,Movie_ID,Review_date,Rating,Review) values(?,?,?,?)";
  private static String insertIntoEndorsement = "insert into endorsement(Review_ID,CUST_ID,Endorse_date) values(?,?,?)";
  /**
   * Create the DML class with the specified database connection and statement
   * @param conn the connection to database
   * @param s the sql statement
   */
  public irate_DML(Connection conn, Statement s) {
    this.conn = conn;
    this.s = s;
  }

  /**
   * Calls all insert into table functions that insert data from respective files into the database.
   */
  public static void insertAll() {
    int success = 0;
    try {
      insertCustomerFile();
      success++;
    }catch (SQLException e) {
      System.err.printf("Unable to insert into customer\n");
      System.err.println(e.getMessage());
    }
    try {
      insertMovieFile();
      success++;
    }catch (SQLException e) {
      System.err.printf("Unable to insert into movie\n");
      System.err.println(e.getMessage());
    }
    try {
      insertAttendanceFile();
      success++;
    }catch (SQLException e) {
      System.err.printf("Unable to insert into attendance\n");
      System.err.println(e.getMessage());
    }
    try {
      insertReviewFile();
      success++;
    }catch (SQLException e) {
      System.err.printf("Unable to insert into review\n");
      System.err.println(e.getMessage());
    }
    try {
      insertEndorsementFile();
      success++;
    }catch (SQLException e) {
      System.err.printf("Unable to insert into endorsement\n");
      System.err.println(e.getMessage());
    }
  }

  /**
   * Inserts the given information into the customer table. Returns the customer id on success, or -1 if unable to insert the customer.
   * @param Name
   * @param email
   * @param address
   * @param date1
   * @return customer id or -1 if unable to insert
   */
  public int insertCustomer(String Name,String email,String address,Date date1) {
    int cust_id = -1;
    try {
      PreparedStatement insertRow_customers = conn.prepareStatement(insertIntoCustomer, PreparedStatement.RETURN_GENERATED_KEYS);
      insertRow_customers.setString(1,Name);
      insertRow_customers.setString(2,email);
      insertRow_customers.setString(3,address);
      insertRow_customers.setDate(4,date1);

      insertRow_customers.executeUpdate();

      ResultSet rs = insertRow_customers.getGeneratedKeys();
      if(rs.next()) {
        cust_id = rs.getInt(1);
      }
      insertRow_customers.close();

    }catch (SQLException e) {
      System.err.println("Unable to insert" + Name + e.getMessage());

    }
    return cust_id;
  }

  /**
   * Inserts the given information into the movie table. Returns the movie id on success, or -1 if unable to insert the movie.
   * @param Title
   * @return movie_id or -1 if unable to insert.
   */
  public int insertmovie(String Title) {
    int movie_id = -1;
    try{
      PreparedStatement insertRow_movie = conn.prepareStatement(insertIntoMovie,PreparedStatement.RETURN_GENERATED_KEYS);
      insertRow_movie.setString(1,Title);

      insertRow_movie.executeUpdate();

      ResultSet rs = insertRow_movie.getGeneratedKeys();
      if (rs.next()) {
        movie_id = rs.getInt(1);
      }
      insertRow_movie.close();
      rs.close();

    }catch (SQLException e) {
      System.err.println("Unable to insert movie"+e.getMessage());
    }
    return movie_id;
  }

  /**
   * Inserts the given information into the attendance table.
   * @param movie_id
   * @param atten_date
   * @param cust_id
   * @return 0 on success or -1 on error.
   */
  public int insertattendance(int movie_id,Date atten_date,int cust_id) {
    try {
      PreparedStatement insertRow_attendance = conn.prepareStatement(insertIntoattendance);
      insertRow_attendance.setInt(1,movie_id);
      insertRow_attendance.setDate(2,atten_date);
      insertRow_attendance.setInt(3,cust_id);

      insertRow_attendance.executeUpdate();
      insertRow_attendance.close();
    }catch (SQLException e) {
      System.err.println("Unable to insert %d"+movie_id);
      return -1;
    }
    return 0;
  }

  /**
   * Inserts the given information into the review table.
   * @param cust_id
   * @param movie_id
   * @param review
   * @param Rating
   * @param Review
   * @return review id or -1 if unable to insert.
   */
  public int insertreview(int cust_id,int movie_id,String review,int Rating,String Review) {
    int review_id = -1;
    try {
      PreparedStatement insertRow_review = conn.prepareStatement(insertIntoReview,PreparedStatement.RETURN_GENERATED_KEYS);
      insertRow_review.setInt(1,cust_id);
      insertRow_review.setInt(2,movie_id);
      insertRow_review.setString(3,review);
      insertRow_review.setInt(4,Rating);
      insertRow_review.setString(5,Review);

      insertRow_review.executeUpdate();
      ResultSet rs = insertRow_review.getGeneratedKeys();
      if (rs.next()) {
        review_id = rs.getInt(1);
      }
      insertRow_review.close();
      rs.close();
    }catch (SQLException e) {
      System.err.println("Unable to insert rating"+e.getMessage());
    }
    return review_id;
  }

  /**
   * Inserts into the endorsement table.
   * @param review_id
   * @param cust_id
   * @param endorse_date
   * @param movie_id
   * @return 0 on success or -1 on error.
   */
  public int insertendorse(int review_id,int cust_id,Date endorse_date,int movie_id) {
    try {
      PreparedStatement insertRow_endorse = conn.prepareStatement(insertIntoEndorsement);
      insertRow_endorse.setInt(1,review_id);
      insertRow_endorse.setInt(2,cust_id);
      insertRow_endorse.setDate(3,endorse_date);
      insertRow_endorse.setInt(4,movie_id);

      insertRow_endorse.executeUpdate();
      insertRow_endorse.close();

    }catch (SQLException e) {
      System.err.println("Unable to insert endorse"+e.getMessage());
      return -1;
    }
    return 0;
  }


  /**
   * Inserts information into the customer table from a text file.
   * @throws SQLException
   */
  public static void insertCustomerFile() throws SQLException {
    PreparedStatement insertRow_customer = conn.prepareStatement(insertIntoCustomer);
    try (
      BufferedReader br = new BufferedReader(new FileReader(new File(customer)));
    ){
      String line;
      while((line = br.readLine()) != null) {
        String[] data = line.split("\t");
        String Name = data[0];
        String email = data[1];
        String address = data[2];
        String register_date = data[3];
        Date date1 = Date.valueOf(register_date);

        insertRow_customer.setString(1,Name);
        insertRow_customer.setString(2,email);
        insertRow_customer.setString(3,address);
        insertRow_customer.setDate(4,date1);

        insertRow_customer.executeUpdate();

        if (insertRow_customer.getUpdateCount() != 1) {
          System.err.printf("Unable to insert Customer table");
        }
      }
    }catch (FileNotFoundException e) {
      System.err.printf("Unable to open the file: %s\n", customer);
    }catch (IOException e) {
      System.err.println("Error reading line\n");
    }
  }

  /**
   * Inserts information into the movie table from a text file.
   * @throws SQLException
   */
  public static void insertMovieFile() throws SQLException {
    PreparedStatement insertRow_Moview = conn.prepareStatement(insertIntoMovie);
    try (
      BufferedReader br = new BufferedReader(new FileReader(new File(moview)));
      ) {
      String line;
      while ((line = br.readLine()) != null) {
        String[] data = line.split("\t");
        String Title = data[0];

        insertRow_Moview.setString(1,Title);

        insertRow_Moview.executeUpdate();

        if(insertRow_Moview.getUpdateCount() != 1) {
          System.err.printf("Unable to insert Movie table");
        }
      }
    }catch (FileNotFoundException e) {
      System.err.printf("Unable to open the file: %s\n",moview);
    }catch (IOException e) {
      System.err.printf("Error reading line.\n");
    }
  }

  /**
   * Inserts information into the attendance table from a text file.
   * @throws SQLException
   */
  public static void insertAttendanceFile()  throws SQLException {
    PreparedStatement insertRow_Attendance = conn.prepareStatement(insertIntoattendance);
    try (
      BufferedReader br = new BufferedReader(new FileReader(new File(attendance)));
    ) {
      String line;
      while((line = br.readLine()) != null) {
        String[] data = line.split("\t");
        String moview_id = data[0];
        String atten_date = data[1];
        String review_id = data[2];
        int m_id = Integer.parseInt(moview_id);
        int r_id = Integer.parseInt(review_id);
        Date date1 = Date.valueOf(atten_date);

        insertRow_Attendance.setInt(1,m_id);
        insertRow_Attendance.setDate(2,date1);
        insertRow_Attendance.setInt(3,r_id);

        insertRow_Attendance.executeUpdate();

        if(insertRow_Attendance.getUpdateCount() != 1) {
          System.err.println("Unable to insert into attendance table");
        }
      }
    }catch (FileNotFoundException e) {
      System.err.printf("Unable to open the file: %s\n",attendance);
    }catch (IOException e) {
      System.err.printf("Error reading line.\n");
    }
  }

  /**
   * Inserts information into the review table from a text file.
   * @throws SQLException
   */
  public static void insertReviewFile() throws SQLException {
    PreparedStatement insertRow_Review = conn.prepareStatement(insertIntoReview);
    try (
      BufferedReader br = new BufferedReader(new FileReader(new File(review)))
      ) {
      String line;
      while((line = br.readLine()) != null) {
        String[] data = line.split("\t");
        String cust_id = data[0];
        String moview_id = data[1];
        String review_date = data[2];
        String rating = data[3];
        String review = data[4];
        int c_id = Integer.parseInt(cust_id);
        int m_id = Integer.parseInt(moview_id);
        Date date1 = Date.valueOf(review_date);

        insertRow_Review.setInt(1,c_id);
        insertRow_Review.setInt(2,m_id);
        insertRow_Review.setDate(3,date1);
        insertRow_Review.setString(4,rating);
        insertRow_Review.setString(5,review);

        insertRow_Review.executeUpdate();

        if(insertRow_Review.getUpdateCount() != 1) {
          System.err.println("Unable to insert into review table");
        }
      }
    }catch (FileNotFoundException e) {
      System.err.printf("Unable to open the file: %s\n",review);
    }catch (IOException e) {
      System.err.printf("Error reading line\n");
    }
  }

  /**
   * Inserts information into the endorsement table from a text file.
   * @throws SQLException
   */
  public static void insertEndorsementFile() throws SQLException {
    PreparedStatement insertRow_endorse = conn.prepareStatement(insertIntoEndorsement);
    try (
      BufferedReader br = new BufferedReader(new FileReader(new File(endorse)))
      ) {
      String line;
      while((line = br.readLine()) != null) {
        String[] data = line.split("\t");
        String review_id = data[0];
        String cust_id = data[1];
        String endorse_date = data[2];
        int r_id = Integer.parseInt(review_id);
        int c_id =Integer.parseInt(cust_id);
        Date date1 = Date.valueOf(endorse_date);

        insertRow_endorse.setInt(1,r_id);
        insertRow_endorse.setInt(2,c_id);
        insertRow_endorse.setDate(3,date1);

        insertRow_endorse.executeUpdate();

        if(insertRow_endorse.getUpdateCount() != 1) {
          System.err.println("Unable to insert into endorsement table");
        }
      }
    }catch (FileNotFoundException e) {
      System.err.printf("Unable to open the file: %s\n",endorse);
    }catch (IOException e) {
      System.err.printf("Error reading line\n");
    }
  }

  /**
   * Truncate tables, clearing them of existing data
   *
   * @param dbTables an array of table names
   */
  public static void truncateTables(String dbTables[]) {
    int deleted = 0;
    for(String table : dbTables) {
      try {
        s.executeUpdate("delete from " + table);
        deleted++;
      }
      catch (SQLException e) {
        System.out.println("Did not truncate table " + table);
      }
    }
    if(deleted == dbTables.length){
      System.out.println("Successfully truncated all tables.");
    }
  }

}
