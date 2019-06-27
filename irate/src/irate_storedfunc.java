import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;

public class irate_storedfunc {
  // connection variables
  private static Connection conn = null;
  private static Statement s = null;

  /** Name of the database */
  private static String dbName = "om";

  /**
   * Checks whether the rating is valid or not.
   * @param Rating The rating of the movie.
   * @return true if rating is between 1 and 5 , false otherwise.
   */
  public static boolean isStar(int Rating) {
    if(Rating > 0 && Rating <= 5) {
      return true;
    }
    else {
      return false;
    }
  }

  /**
   * Checks whether endorsement date is within a day of recent review of the movie.
   * @param cust_id The unique customer id of the customer
   * @param movie_id The unique movie id for the movie
   * @param date1 The date of endorsement
   * @return true if date is within a day,false otherwise.
   */
  public static boolean isEndordate(int cust_id,int movie_id,java.sql.Date date1) {
    SimpleDateFormat formatter1 = new SimpleDateFormat("yyyy-MM-dd");
    java.util.Date date11 = null;
    try {
      date11 = formatter1.parse("1970-06-06");
    } catch (ParseException e) {
      e.printStackTrace();
    }
    long r_date = date1.getTime();
    irate_Connection om = new irate_Connection();
    om.startConnection("user1", "password", dbName);
    conn = om.getConnection();
    s = om.getStatement();
    irate_DQL dql = new irate_DQL(conn, s);
    java.sql.Date date2 = dql.atten(cust_id, movie_id);
    if(date2 == date11)
      return false;
    long a_date = date2.getTime();
    long diffinmill = r_date - a_date;
    long diff = TimeUnit.DAYS.convert(diffinmill,TimeUnit.MILLISECONDS);
    if(diff >= 1)
      return true;
    else
      return false;

  }

  /**
   * Checks whether review date is within 7 days of recent attendance of the movie.
   * @param date1 The date when the review was made.
   * @param CUST_ID Unique customer id of the customer
   * @param MOVIE_ID Unique movie id for the movie
   * @return Returns true if date is within 7 days,false otherwise.
   */
  public static boolean isRevdate(java.sql.Date date1,int CUST_ID,int MOVIE_ID) {
    SimpleDateFormat formatter1 = new SimpleDateFormat("yyyy-MM-dd");
    java.util.Date date11 = null;
    try {
      date11 = formatter1.parse("1970-06-06");
    } catch (ParseException e) {
      e.printStackTrace();
    }
    long r_date = date1.getTime();
    irate_Connection om = new irate_Connection();
    om.startConnection("user1", "password", dbName);
    conn = om.getConnection();
    s = om.getStatement();
    irate_DQL dql = new irate_DQL(conn, s);
    java.sql.Date date2 = dql.atten(CUST_ID, MOVIE_ID);
    if(date2 == date11)
      return false;
    long a_date = date2.getTime();
    long diffinmill = r_date - a_date;
    long diff = TimeUnit.DAYS.convert(diffinmill,TimeUnit.MILLISECONDS);
    if(diff > 7  || diff <= 0)
      return false;
    else
      return true;
  }

  /**
   * Checks whether the customer's endorsement is valid or not.
   * @param CUST_ID The unique customer id of the customer.
   * @param REVIEW_ID The unique review id for the review.
   * @return Returns true if customer's endorsement is valid,false otherwise.
   */
  public static boolean isEndorValid(int CUST_ID,int REVIEW_ID) {
    irate_Connection om = new irate_Connection();
    om.startConnection("user1", "password", dbName);
    conn = om.getConnection();
    s = om.getStatement();
    irate_DQL dql = new irate_DQL(conn, s);
    int REVIEW_ID_2 = dql.revid(CUST_ID);
    if(REVIEW_ID == REVIEW_ID_2)
      return false;
    else
      return true;
  }

}
