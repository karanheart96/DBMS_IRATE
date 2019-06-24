import java.sql.Date;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.Connection;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class irate_DQL {
  // connection variables
  Connection conn = null;
  Statement s = null;

  // holds results from queries
  private ResultSet rs = null;

  /**
   * Creates an instance of the class with the sql statement for OrderManager database.
   * @param s the sql statement
   */
  public irate_DQL(Connection conn, Statement s) {
    this.conn = conn;
    this.s = s;
  }

  /**
   * Finds the review id for a particular customer.
   * @param CUST_ID The unique customer id of a customer.
   * @return Returns the review id if present,0 otherwise
   */
  public int revid(int CUST_ID) {
    int rev = 0;
    try {
      ResultSet rs = s.executeQuery("select * from review where CUST_ID = "+CUST_ID);
      while (rs.next()) {
        rev = rs.getInt("REVIEW_ID");
      }

    }catch (SQLException e)
    {
      System.err.println(e.getMessage());
    }
    return rev;

  }

  /**
   * Finds the most recent attended date of a customer for a movie.
   * @param CUST_ID The unique customer id for a customer.
   * @param MOVIE_ID The unique movie id for a movie.
   * @return Returns the most recent attendance date if present,"1970-06-06" otherwise.
   */
  public java.sql.Date atten(int CUST_ID, int MOVIE_ID) {
    String str = "Select * from attendance where CUST_ID = " + CUST_ID + "and MOVIE_ID = " + MOVIE_ID +"order by attendance_date DESC";
    java.util.Date now = new java.util.Date();
    java.sql.Date d1 = new java.sql.Date(now.getTime());
    int flag = 0;
    java.util.Date date = new java.util.Date();
    SimpleDateFormat formatter1 = new SimpleDateFormat("yyyy-MM-dd");
    java.util.Date date1 = null;
    try {
      date1 = formatter1.parse("1970-06-06");
    } catch (ParseException e) {
      e.printStackTrace();
    }
    try {
      ResultSet rs = s.executeQuery(str);
      while(rs.next()) {
        d1 = rs.getDate("attendance_date");
        flag = 1;
        break;
      }

    }catch (SQLException e) {
      flag = 0;
      System.err.println(e.getMessage());
    }
    if(flag == 1)
      return d1;
    else
      return (Date) date1;
  }

  /**
   * Finds the most recent reviewed date for a movie by a customer.
   * @param CUST_ID The unique customer id for a customer.
   * @param MOVIE_ID The unique movie id for a movie.
   * @return Returns the most recent reviewed date.
   */
  public java.sql.Date revatten(int CUST_ID,int MOVIE_ID) {
    String str = "Select * from endorsement where CUST_ID = " + CUST_ID+ "and MOVIE_ID =" +MOVIE_ID +"order by endorse_date DESC";
    java.util.Date now = new java.util.Date();
    java.sql.Date d1 = new java.sql.Date(now.getTime());
    int flag = 0;
    java.util.Date date = new java.util.Date();
    SimpleDateFormat formatter1 = new SimpleDateFormat("yyyy-MM-dd");
    java.util.Date date1 = null;
    try {
      date1 = formatter1.parse("1970-06-06");
    } catch (ParseException e) {
      e.printStackTrace();
    }
    try {
      ResultSet rs = s.executeQuery(str);
      while(rs.next()) {
        d1 = rs.getDate("endorse_date");
        flag = 1;
        break;
      }

    }catch (SQLException e) {
      flag = 0;
      System.err.println(e.getMessage());
    }
    if(flag == 1)
      return d1;
    else
      return (Date) date1;
  }

  /**
   * Selects and displays all the customers.
   */
  public void selectAllCustomer() {
    try {
      ResultSet rs = s.executeQuery("select * from customer");
      while(rs.next()) {
        System.out.println(rs.getString("CUST_ID"));
        System.out.println(rs.getString("Name"));
        System.out.println(rs.getString("Email"));
        System.out.println(rs.getString("Address"));
        System.out.println(rs.getDate("register_date"));
      }
      rs.close();
    }catch (SQLException e) {
      System.err.println(e.getMessage());
    }
  }

  /**
   * Selects and displays customer information for a particular customer.
   * @param cust_id The unique customer id for a customer.
   */
  public void selectCustomerID(int cust_id) {
    try{
      rs = s.executeQuery("Select * from customer where CUST_ID = "+cust_id);
      while(rs.next()) {
        System.out.println(rs.getString("CUST_ID"));
        System.out.println(rs.getString("Name"));
        System.out.println(rs.getString("Email"));
        System.out.println(rs.getString("Address"));
        System.out.println(rs.getDate("register_date"));
      }
      rs.close();
    }catch (SQLException e) {
      System.err.printf("Customer ID: %d was not found",cust_id);
    }

  }

  /**
   * Selects and displays all the movies.
   */
  public void selectAllMovie() {
    try {
     ResultSet rs = s.executeQuery("Select * from movie");
      while(rs.next()) {
        System.out.println(rs.getString("Title"));
        System.out.println(rs.getInt("Movie_ID"));
      }
      rs.close();
    }catch (SQLException e) {
      System.err.println(e.getMessage());
    }
  }

  /**
   * Selects and displays movie information for a particular movie.
   * @param movie_id The unique movie id for a movie.
   */
  public void selectMovieID(int movie_id) {
    try{
      ResultSet rs = s.executeQuery("Select * from movie where Movie_ID = "+ movie_id);
      System.out.println(rs.getString("Title"));
      System.out.println(rs.getInt("Movie_ID"));
    }catch (SQLException e) {
      System.err.printf("Movie ID : %d was not found",movie_id);
    }

  }

  /**
   * Selects and displays all the reviews.
   */
  public void selectAllReview() {
    try{
      rs = s.executeQuery("Select * from review");
      while(rs.next()) {
        System.out.println(rs.getInt("CUST_ID"));
        System.out.println(rs.getInt("Movie_ID"));
        System.out.println(rs.getDate("Review_date"));
        System.out.println(rs.getInt("Rating"));
        System.out.println(rs.getString("Review"));
        System.out.println(rs.getInt("Review_ID"));
      }
      rs.close();
    }catch (SQLException e) {
      System.err.println(e.getMessage());
    }
  }

  /**
   * Selects and displays review information for a particular review.
   * @param cust_id The unique customer id for a review.
   */
  public void selectReviewID(int cust_id) {
    try {
      rs = s.executeQuery("Select * from review where CUST_ID = "+cust_id);
      while (rs.next()) {
        System.out.println(rs.getInt("CUST_ID"));
        System.out.println(rs.getInt("Movie_ID"));
        System.out.println(rs.getDate("Review_date"));
        System.out.println(rs.getInt("Rating"));
        System.out.println(rs.getString("Review"));
        System.out.println(rs.getInt("Review_ID"));
      }
      rs.close();
    }catch (SQLException e) {
      System.err.println(e.getMessage());
    }

  }

  /**
   * Selects and displays all the endorsements.
   */
  public void selectAllEndorsement() {
    try {
      rs = s.executeQuery("Select * from endorsement");
      while (rs.next()) {
        System.out.println(rs.getInt("Review_ID"));
        System.out.println(rs.getInt("CUST_ID"));
        System.out.println(rs.getDate("Endorse_date"));
      }
      rs.close();
    }catch (SQLException e) {
      System.err.println(e.getMessage());
    }
  }

  /**
   * Selects and displays endorsement information for a review.
   * @param cust_id The unique customer id for a customer.
   */
  public void selectEndorseID(int cust_id) {
    try {
      rs = s.executeQuery("Select * from endorsement where CUST_ID = "+cust_id);
      while (rs.next()) {
        System.out.println(rs.getInt("Review_ID"));
        System.out.println(rs.getInt("CUST_ID"));
        System.out.println(rs.getDate("Endorse_date"));
      }
      rs.close();
    }catch (SQLException e) {
      System.err.println(e.getMessage());
    }

  }

  /**
   * Selects and displays all the attendance.
   */
  public void selectAllAttendance() {
    try {
      rs = s.executeQuery("Select * from attendance");
      while(rs.next()) {
        System.out.println(rs.getInt("Movie_ID"));
        System.out.println(rs.getDate("Atten_date"));
        System.out.println(rs.getInt("CUST_ID"));
      }
      rs.close();
    }catch (SQLException e) {
      System.err.println(e.getMessage());
    }
  }

  /**
   * Selects and displays attendance information for a particular movie.
   * @param movie_id The unique movie id for a movie.
   */
  public void selectAttendaceID(int movie_id) {
    try {
      rs = s.executeQuery("Select * from attendance where Movie_ID = " + movie_id);
      while (rs.next()) {
        System.out.println(rs.getInt("Movie_ID"));
        System.out.println(rs.getDate("Atten_date"));
        System.out.println(rs.getInt("CUST_ID"));
      }
    }catch (SQLException e) {
      System.err.println(e.getMessage());
    }

  }

}
