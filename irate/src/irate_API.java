import java.sql.*;

public class irate_API {

  // holds an instance of the DML class for insert statements
  private irate_DML dml = null;

  // holds an instance of the DQL class for select statements
  private irate_DQL dql = null;

  /**
   * Creates an instance of the API class with instances of the DML and DQL classes
   */
  public irate_API(irate_DML dml, irate_DQL dql){
    this.dml = dml;
    this.dql = dql;
  }

  /**
   * Creates a customer with the given attributes.
   * @param s sql statement
   * @param conn connection to database
   * @param Name name of the customer.
   * @param email email id of the customer.
   * @param address address of the customer.
   * @param register_date Registry date of the customer.
   * @return customer id on success or -1 if unsuccessful.
   */
  public int createCustomer(Statement s,Connection conn,String Name,String email,String address,Date register_date) {
    int cust_id = dml.insertCustomer(Name,email,address,register_date);
    if(cust_id != -1) {
      System.out.printf("Successfully added %s\n",Name);
    }
    return cust_id;
  }

  /**
   * Creates a movie with the given attributes.
   * @param Title The title of the movie.
   */
  public void createMovie(String Title) {
    int movie_id = dml.insertmovie(Title);
    if(movie_id != -1) {
      System.out.printf("Successfully added %s\n",Title);
    }
  }

  /**
   * Creates a review with the given attributes.
   * @param cust_id Customer id who made the review
   * @param movie_id Movie id for the movie.
   * @param review_date The date on which the review was made.
   * @param rating Rating for the movie.
   * @param review Review for the movie.
   */
  public void createReview(int cust_id,int movie_id,Date review_date,int rating,String review) {
   int review_id =  dml.insertreview(cust_id,movie_id,review,rating,review);
   if(review_id != -1) {
     System.out.printf("Successfully added %s\n",review);
   }
  }

  /**
   * Creates an endorsement with the given attributes.
   * @param review_id Review id which was endorsed.
   * @param cust_id Customer id who made the endorsement
   * @param endorse_date The date on which the endorsement was made.
   * @param movie_id Movie id for which the review was endorsed.
   */
  public void createendorsement(int review_id,int cust_id,Date endorse_date,int movie_id) {
   int end =  dml.insertendorse(review_id,cust_id,endorse_date,movie_id);
    if(end != -1) {
      System.out.printf("Successfully added ");
    }
  }

  /**
   * Creates an attendance with the given attributes.
   * @param movie_id Movie id which ws attended by the customer.
   * @param atten_date The date on which the customer attended the movie.
   * @param cust_id Customer id who attended the movie.
   */
  public void createattendance(int movie_id,Date atten_date,int cust_id) {
    int attend = dml.insertattendance(movie_id,atten_date,cust_id);
    if(attend != -1) {
      System.out.printf("Successfully added ");
    }
  }

}
