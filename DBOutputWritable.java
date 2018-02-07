import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DBOutputWritable implements DBWritable{

	private int user_id;
	private int movie_id;
	private String rating;
	
	public DBOutputWritable(int user_id, int movie_id, double rating) {
		this.user_id = user_id;
		this.movie_id = movie_id;
		this.rating = Double.toString(rating);
	}

	public void readFields(ResultSet arg0) throws SQLException {

		this.user_id = arg0.getInt(1);
		this.movie_id = arg0.getInt(2);
		this.rating = arg0.getString(3);
		
	}

	public void write(PreparedStatement arg0) throws SQLException {

		arg0.setInt(1, user_id);
		arg0.setInt(2, movie_id);
		arg0.setString(3, rating);
		
	}

}
