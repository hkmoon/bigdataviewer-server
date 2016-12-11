package bdv.db;

import bdv.model.DataSet;
import bdv.model.Share;
import bdv.model.User;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Optional;

/**
 * DBConnection holds the connection of H2 database for {@link bdv.server.BigDataServer} in advanced mode
 *
 * Author: HongKee Moon (moon@mpi-cbg.de), Scientific Computing Facility
 * Organization: MPI-CBG Dresden
 * Date: November 2016
 */
public class DBConnection
{
	Optional< Connection > conn = Optional.empty();

	public DBConnection()
	{
	}

	/**
	 * Connect to the default database
	 * @return
	 */
	public boolean connect()
	{
		try
		{
			Class.forName( "org.h2.Driver" );
		}
		catch ( ClassNotFoundException e )
		{
			System.err.println( e.getMessage() );
		}

		if ( !conn.isPresent() )
		{
			try
			{
				conn = Optional.of( DriverManager.getConnection( "jdbc:h2:./etc/BigDataServer", "sa", "" ) );
			}
			catch ( SQLException e )
			{
				System.err.println( e.getMessage() );
			}
		}

		boolean ret = false;

		if ( !conn.isPresent() )
			return ret;

		try
		{
			ret = !conn.get().isClosed();
		}
		catch ( SQLException e )
		{
			System.err.println( e.getMessage() );
			//			 e.printStackTrace();
		}

		return ret;
	}

	/**
	 * Close the current database connection
	 */
	public void close()
	{
		if ( conn.isPresent() )
		{
			try
			{
				Class.forName( "org.h2.Driver" );
			}
			catch ( ClassNotFoundException e )
			{
				System.err.println( e.getMessage() );
			}

			try
			{
				conn.get().close();
			}
			catch ( SQLException e )
			{
				System.err.println( e.getMessage() );
			}
		}
	}

	/**
	 * Return the Closed property of the database connection
	 * @return false if the connection is closed
	 */
	public boolean isClosed()
	{
		boolean retValue = true;
		if ( conn.isPresent() )
			try
			{
				retValue = conn.get().isClosed();
			}
			catch ( SQLException e )
			{
				System.err.println( e.getMessage() );
				e.printStackTrace();
			}
		else
			retValue = true;

		return retValue;
	}

	/**
	 * Add an User to the database
	 * @param id is the user unique id
	 * @param name is the user name
	 * @param password is the user's password
	 */
	public void addUser( String id, String name, String password )
	{
		addUser( id, name, password, false );
	}

	/**
	 * Add an User to the database with the manager flag
	 * @param id is the user unique id
	 * @param name is the user name
	 * @param password is the user's password
	 * @param isManager true for having the manager role
	 */
	public void addUser( String id, String name, String password, boolean isManager )
	{
		if ( conn.isPresent() )
		{
			try
			{
				Statement stat = conn.get().createStatement();

				String sql = String.format( "MERGE INTO USER ( ID, NAME, PASSWORD, MANAGER, UPDATED_TIME ) VALUES('%s', '%s', "
								+ "HASH('SHA256', STRINGTOUTF8('%s'), 1024), %s, CURRENT_TIMESTAMP() )",
						id, name, password, isManager );

				stat.execute( sql );

				stat.close();
			}
			catch ( SQLException e )
			{
				System.err.println( e.getMessage() );
				//				e.printStackTrace();
			}
		}
	}

	/**
	 * Remove the user with the given password
	 * @param id is the user unique id
	 * @param password is the user's password
	 * @return true if the user is removed successfully
	 */
	public boolean removeUser( String id, String password )
	{
		boolean ret = false;

		if ( conn.isPresent() )
		{
			try
			{
				Statement stat = conn.get().createStatement();

				String sql = String.format( "SELECT ID, NAME, MANAGER, UPDATED_TIME from USER where ID = '%s' and PASSWORD = HASH('SHA256', STRINGTOUTF8('%s'), 1024)", id, password );

				ResultSet rs = stat.executeQuery( sql );
				User user = null;

				if ( !rs.next() )
				{
					//if rs.next() returns false
					//then there are no rows.
					System.err.println( "No User found with ID=" + id );
				}
				else
				{
					// Get data from the current row and use it
					user = new User( rs.getString( "ID" ),
							rs.getString( "NAME" ),
							rs.getBoolean( "MANAGER" ),
							rs.getTimestamp( "UPDATED_TIME" ) );
				}

				if ( null != user )
				{
					if ( user.isManager() )
					{
						System.err.println( "Manager cannot be removed." );
					}
					else
					{
						int n = stat.executeUpdate( String.format( "DELETE from USER where ID = '%s' and PASSWORD = HASH('SHA256', STRINGTOUTF8('%s'), 1024)", id, password ) );
						ret = n == 1;
					}
				}

				stat.close();
			}
			catch ( SQLException e )
			{
				System.err.println( e.getMessage() );
				//				e.printStackTrace();
			}
		}

		return ret;
	}

	/**
	 * Get the user with the given id and password
	 * @param id is the unique user id
	 * @param password is the user's password
	 * @return {@link User} instance
	 */
	public User getUser( String id, String password )
	{
		User user = null;

		if ( conn.isPresent() )
		{
			try
			{
				Statement stat = conn.get().createStatement();

				String sql = String.format( "SELECT ID, NAME, MANAGER, UPDATED_TIME from USER where ID = '%s' and PASSWORD = HASH('SHA256', STRINGTOUTF8('%s'), 1024)", id, password );

				ResultSet rs = stat.executeQuery( sql );

				if ( !rs.next() )
				{
					//if rs.next() returns false
					//then there are no rows.
					System.err.println( "No user found with ID=" + id );
				}
				else
				{
					// Get data from the current row and use it
					user = new User( rs.getString( "ID" ),
							rs.getString( "NAME" ),
							rs.getBoolean( "MANAGER" ),
							rs.getTimestamp( "UPDATED_TIME" ) );
				}

				stat.close();
			}
			catch ( SQLException e )
			{
				System.err.println( e.getMessage() );
				//				e.printStackTrace();
			}
		}

		return user;
	}

	/**
	 * Add new DataSet to the database
	 * @param user {@link User} holding the user information
	 * @param name is the {@link DataSet} name
	 * @param description is the description of {@link DataSet}
	 * @param xmlPath is the xml location path of {@link DataSet}
	 * @param isPublic true if the {@link DataSet} is public specified by owner
	 */
	public void addDataSet( User user, String name, String description, String xmlPath, boolean isPublic )
	{
		if ( null == user )
		{
			System.err.println( "User is null" );
			return;
		}

		if ( conn.isPresent() )
		{
			try
			{
				Statement stat = conn.get().createStatement();

				String sql = String.format( "INSERT INTO DATASET ( NAME, XMLPATH, DESCRIPTION, OWNER_ID, PUBLIC, UPDATED_TIME ) VALUES('%s', '%s', "
								+ "'%s', '%s', %s, CURRENT_TIMESTAMP() )",
						name, xmlPath, description, user.getId(), isPublic );

				stat.execute( sql );

				stat.close();
			}
			catch ( SQLException e )
			{
				System.err.println( e.getMessage() );
				//				e.printStackTrace();
			}
		}
	}

	/**
	 * Get the {@link DataSet}
	 * @param user {@link User} is the owner of {@link DataSet}
	 * @param name {@link DataSet} name
	 * @return the found {@link DataSet}
	 */
	public DataSet getDataSet( User user, String name )
	{
		if ( null == user )
		{
			System.err.println( "User is null" );
			return null;
		}

		DataSet dataSet = null;
		if ( conn.isPresent() )
		{
			try
			{
				Statement stat = conn.get().createStatement();

				String sql = String.format( "SELECT ID, NAME, XMLPATH, DESCRIPTION, OWNER_ID, PUBLIC, UPDATED_TIME from DATASET where NAME = '%s' and OWNER_ID = '%s'", name, user.getId() );

				ResultSet rs = stat.executeQuery( sql );

				if ( !rs.next() )
				{
					//if rs.next() returns false
					//then there are no rows.
					System.err.println( "No DataSet found with Name=" + name );
				}
				else
				{
					// Get data from the current row and use it
					dataSet = new DataSet( rs.getInt( "ID" ),
							rs.getString( "NAME" ),
							rs.getString( "XMLPATH" ),
							rs.getString( "DESCRIPTION" ),
							rs.getString( "OWNER_ID" ),
							rs.getBoolean( "PUBLIC" ),
							rs.getTimestamp( "UPDATED_TIME" ) );
				}

				if ( null != dataSet )
				{
					sql = String.format( "SELECT TITLE from TAG join TAG_DATASET on TAG.ID = TAG_DATASET.TAG_ID where TAG_DATASET.DATASET_ID = %d", dataSet.getIndex() );

					rs = stat.executeQuery( sql );

					if ( !rs.next() )
					{
						//System.err.println( "No Tag found" );
					}
					else
					{
						do
						{
							dataSet.addTag( rs.getString( "TITLE" ) );
						} while ( rs.next() );
					}
				}

				stat.close();
			}
			catch ( SQLException e )
			{
				System.err.println( e.getMessage() );
				//				e.printStackTrace();
			}
		}

		return dataSet;
	}

	/**
	 * Update the DataSet database with the given {@link DataSet}
	 * @param user {@link User} instance who can access the specified {@link DataSet}
	 * @param dataSet {@link DataSet} containing the updating information
	 */
	public void updateDataSet( User user, DataSet dataSet )
	{
		if ( null == user )
		{
			System.err.println( "User is null" );
			return;
		}

		if ( user.isManager() || user.getId().equals( dataSet.getOwner() ) )
		{
			if ( conn.isPresent() )
			{
				try
				{
					Statement stat = conn.get().createStatement();

					String sql = String.format( "MERGE INTO DATASET ( ID, NAME, XMLPATH, DESCRIPTION, PUBLIC, UPDATED_TIME ) VALUES("
									+ "%d, '%s', '%s', '%s', %s, CURRENT_TIMESTAMP() "
									+ ")",
							dataSet.getIndex(), dataSet.getName(), dataSet.getXmlPath(), dataSet.getDescription(), dataSet.isPublic() );

					stat.execute( sql );

					stat.close();
				}
				catch ( SQLException e )
				{
					System.err.println( e.getMessage() );
					//				e.printStackTrace();
				}
			}
		}
	}

	/**
	 * Remove the {@link DataSet} belongs to the {@link User}
	 * @param user {@link User} has ownership of the {@link DataSet}
	 * @param dataSet {@link DataSet} to be removed
	 */
	public void removeDataSet( User user, DataSet dataSet )
	{
		if ( null == user )
		{
			System.err.println( "User is null" );
			return;
		}

		if ( user.isManager() || user.getId().equals( dataSet.getOwner() ) )
		{
			if ( conn.isPresent() )
			{
				try
				{
					Statement stat = conn.get().createStatement();

					// Delete the dataset from TAG_DATASET table
					String sql = String.format( "DELETE FROM TAG_DATASET WHERE DATASET_ID = %d",
							dataSet.getIndex() );

					stat.executeUpdate( sql );

					// Delete the dataset from USER_DATASET table
					sql = String.format( "DELETE FROM USER_DATASET WHERE DATASET_ID = %d",
							dataSet.getIndex() );

					stat.executeUpdate( sql );

					sql = String.format( "DELETE FROM DATASET WHERE ID = %d",
							dataSet.getIndex() );

					// Delete the dataset in DATASET table
					stat.executeUpdate( sql );

					stat.close();
				}
				catch ( SQLException e )
				{
					System.err.println( e.getMessage() );
					//				e.printStackTrace();
				}
			}
		}
	}

	/**
	 * Add a share information to enable other user to give access privilege to the specified {@link DataSet}
	 * @param user the owner of the {@link DataSet}
	 * @param dataSet the {@link DataSet} to be shared
	 * @param addingUserId the {@link User} to be shared with
	 * @param read true for read
	 * @param update true for update
	 * @param delete true for delete
	 */
	public void addDataSetShare( User user, DataSet dataSet, String addingUserId, boolean read, boolean update, boolean delete )
	{
		if ( null == user )
		{
			System.err.println( "User is null" );
			return;
		}

		if ( null == dataSet )
		{
			System.err.println( "DataSet is null" );
			return;
		}

		if ( user.isManager() || user.getId().equals( dataSet.getOwner() ) )
		{
			if ( conn.isPresent() )
			{
				try
				{
					Statement stat = conn.get().createStatement();

					String sql = String.format( "MERGE INTO USER_DATASET ( USER_ID, DATASET_ID, READ, UPDATE, DELETE, UPDATED_TIME ) VALUES("
									+ "'%s', %d, %s, %s, %s, CURRENT_TIMESTAMP() "
									+ ")",
							addingUserId, dataSet.getIndex(), read, update, delete );

					stat.execute( sql );

					stat.close();
				}
				catch ( SQLException e )
				{
					System.err.println( e.getMessage() );
					//				e.printStackTrace();
				}
			}
		}
	}

	/**
	 * Update the share information with the different access privilege
	 * @param user the owner of the {@link DataSet}
	 * @param dataSet the {@link DataSet} to be shared
	 * @param addingUserId the {@link User} to be shared with
	 * @param read true for read
	 * @param update true for update
	 * @param delete true for delete
	 */
	public void updateDataSetShare( User user, DataSet dataSet, String addingUserId, boolean read, boolean update, boolean delete )
	{
		addDataSetShare( user, dataSet, addingUserId, read, update, delete );
	}

	/**
	 * Remove the share from the specific user
	 * @param user the owner of the {{@link DataSet}}
	 * @param dataSet the target {@link DataSet}
	 * @param removingUserId the {@link User} to be removed from the share
	 */
	public void removeDataSetShare( User user, DataSet dataSet, String removingUserId )
	{
		if ( null == user )
		{
			System.err.println( "User is null" );
			return;
		}

		if ( null == dataSet )
		{
			System.err.println( "DataSet is null" );
			return;
		}

		if ( user.isManager() || user.getId().equals( dataSet.getOwner() ) )
		{
			if ( conn.isPresent() )
			{
				try
				{
					Statement stat = conn.get().createStatement();

					// Delete the dataset from USER_DATASET table
					String sql = String.format( "DELETE FROM USER_DATASET WHERE DATASET_ID = %d and USER_ID = '%s'",
							dataSet.getIndex(), removingUserId );

					stat.executeUpdate( sql );

					stat.close();
				}
				catch ( SQLException e )
				{
					System.err.println( e.getMessage() );
					//				e.printStackTrace();
				}
			}
		}
	}

	/**
	 * This returns all the accessible DataSet collection for the specified {@link User} associated with {@link Share} property
	 * @param user the target {@link User}
	 * @return {@link HashMap} containing all the possible {@link DataSet} and {@link Share} property
	 */
	public HashMap< DataSet, Share > getDataSetCollection( User user )
	{
		if ( null == user )
		{
			System.err.println( "User is null" );
			return null;
		}

		HashMap< DataSet, Share > map = new HashMap<>();

		if ( conn.isPresent() )
		{
			try
			{
				Statement stat = conn.get().createStatement();

				String sql = String.format( "SELECT ID, NAME, XMLPATH, DESCRIPTION, OWNER_ID, PUBLIC, UPDATED_TIME from DATASET where OWNER_ID = '%s'", user.getId() );

				ResultSet rs = stat.executeQuery( sql );

				if ( !rs.next() )
				{
					//if rs.next() returns false
					//then there are no rows.
					System.err.println( "No DataSet found for User ID=" + user.getId() );
				}
				else
				{
					// Get data from the current row and use it
					do
					{
						final DataSet dataSet = new DataSet( rs.getInt( "ID" ),
								rs.getString( "NAME" ),
								rs.getString( "XMLPATH" ),
								rs.getString( "DESCRIPTION" ),
								rs.getString( "OWNER_ID" ),
								rs.getBoolean( "PUBLIC" ),
								rs.getTimestamp( "UPDATED_TIME" ) );

						map.put( dataSet, new Share( true, true, true ) );
					} while ( rs.next() );
				}

				sql = String.format( "SELECT DATASET.ID, DATASET.NAME, XMLPATH, DESCRIPTION, OWNER_ID, PUBLIC, DATASET.UPDATED_TIME, READ, UPDATE, DELETE from DATASET JOIN USER_DATASET ON DATASET.ID = USER_DATASET.DATASET_ID where USER_DATASET.USER_ID = '%s'", user.getId() );

				rs = stat.executeQuery( sql );

				if ( !rs.next() )
				{
					//if rs.next() returns false
					//then there are no rows.
					System.err.println( "No DataSet shared with User ID=" + user.getId() );
				}
				else
				{
					// Get data from the current row and use it
					do
					{
						final DataSet dataSet = new DataSet( rs.getInt( "ID" ),
								rs.getString( "NAME" ),
								rs.getString( "XMLPATH" ),
								rs.getString( "DESCRIPTION" ),
								rs.getString( "OWNER_ID" ),
								rs.getBoolean( "PUBLIC" ),
								rs.getTimestamp( "UPDATED_TIME" ) );

						map.put( dataSet, new Share( rs.getBoolean( "READ" ), rs.getBoolean( "UPDATE" ), rs.getBoolean( "DELETE" ) ) );
					} while ( rs.next() );
				}

				stat.close();
			}
			catch ( SQLException e )
			{
				System.err.println( e.getMessage() );
				//				e.printStackTrace();
			}
		}

		return map;
	}

	/**
	 * Returns all the DataSet publicly accessible associated with the tag string
	 * @param tag the tag string included in {@link DataSet}
	 * @return {@link ArrayList} of {@link DataSet} associated with the tag
	 */
	public ArrayList< DataSet > getPublicDataSetCollection( String tag )
	{
		if ( null == tag )
		{
			System.err.println( "Tag is null" );
			return null;
		}

		ArrayList< DataSet > list = new ArrayList<>();

		if ( conn.isPresent() )
		{
			try
			{
				Statement stat = conn.get().createStatement();

				String sql = String.format( "SELECT ID, NAME, XMLPATH, DESCRIPTION, OWNER_ID, PUBLIC, UPDATED_TIME from DATASET where PUBLIC = TRUE" );

				ResultSet rs = stat.executeQuery( sql );

				if ( !rs.next() )
				{
					//if rs.next() returns false
					//then there are no rows.
					System.err.println( "No DataSet found with Tag=" + tag );
				}
				else
				{
					// Get data from the current row and use it
					do
					{
						final DataSet dataSet = new DataSet( rs.getInt( "ID" ),
								rs.getString( "NAME" ),
								rs.getString( "XMLPATH" ),
								rs.getString( "DESCRIPTION" ),
								rs.getString( "OWNER_ID" ),
								rs.getBoolean( "PUBLIC" ),
								rs.getTimestamp( "UPDATED_TIME" ) );

						list.add( dataSet );
					} while ( rs.next() );
				}

				stat.close();
			}
			catch ( SQLException e )
			{
				System.err.println( e.getMessage() );
				//				e.printStackTrace();
			}
		}

		return list;
	}

	/**
	 * Add tag string for the {@link DataSet}
	 * @param dataSet {@link DataSet} for the tag
	 * @param tagTitle the tag title to be added to {@link DataSet}
	 */
	public void addDataSetTag( DataSet dataSet, String tagTitle )
	{
		if ( null == dataSet )
		{
			System.err.println( "DataSet is null" );
			return;
		}

		if ( conn.isPresent() )
		{
			try
			{
				Statement stat = conn.get().createStatement();

				String sql = String.format( "SELECT ID from TAG where TITLE = '%s'", tagTitle );

				ResultSet rs = stat.executeQuery( sql );
				if ( !rs.next() )
				{
					sql = String.format( "INSERT INTO TAG ( TITLE ) VALUES("
							+ "'%s')", tagTitle );

					stat.execute( sql );
				}

				sql = String.format( "SELECT ID from TAG where TITLE = '%s'", tagTitle );

				rs = stat.executeQuery( sql );
				if ( rs.next() )
				{
					sql = String.format( "MERGE INTO TAG_DATASET ( TAG_ID, DATASET_ID ) VALUES ( %d, %d )",
							rs.getInt( "ID" ), dataSet.getIndex() );

					stat.executeUpdate( sql );
				}

				stat.close();
			}
			catch ( SQLException e )
			{
				System.err.println( e.getMessage() );
				//				e.printStackTrace();
			}
		}
	}

	/**
	 * Remove the tag from the {@link DataSet}
	 * @param dataSet the target {@link DataSet}
	 * @param tagTitle the tag title to be removed from the {@link DataSet}
	 */
	public void removeDataSetTag( DataSet dataSet, String tagTitle )
	{
		if ( null == dataSet )
		{
			System.err.println( "DataSet is null" );
			return;
		}

		if ( conn.isPresent() )
		{
			try
			{
				Statement stat = conn.get().createStatement();

				String sql = String.format( "SELECT ID from TAG where TITLE = '%s'", tagTitle );

				ResultSet rs = stat.executeQuery( sql );

				if ( rs.next() )
				{
					sql = String.format( "DELETE FROM TAG_DATASET where TAG_ID = %d and DATASET_ID = %d",
							rs.getInt( "ID" ), dataSet.getIndex() );

					stat.executeUpdate( sql );
				}

				stat.close();
			}
			catch ( SQLException e )
			{
				System.err.println( e.getMessage() );
				//				e.printStackTrace();
			}
		}
	}

	/**
	 * Initialize the H2 database at the first time
	 * All the queries use "IF NOT EXISTS" so that it keeps the original database structure for safety
	 */
	public void initializeDatabase()
	{
		if ( conn.isPresent() )
		{
			try
			{
				Statement stat = conn.get().createStatement();

				// Create USER table
				stat.execute( "CREATE TABLE IF NOT EXISTS USER"
						+ "("
						+ "  ID VARCHAR(32) PRIMARY KEY NOT NULL,"
						+ "  NAME VARCHAR(255) NOT NULL,"
						+ "  PASSWORD CHAR(64) NOT NULL,"
						+ "  UPDATED_TIME TIMESTAMP NOT NULL,"
						+ "  MANAGER BOOLEAN NOT NULL DEFAULT FALSE"
						+ ");"
				);

				// Create DATASET table
				stat.execute( "CREATE TABLE IF NOT EXISTS DATASET"
						+ "("
						+ "  ID INTEGER AUTO_INCREMENT PRIMARY KEY NOT NULL,"
						+ "  NAME VARCHAR(255) NOT NULL,"
						+ "  XMLPATH VARCHAR(4096) NOT NULL,"
						+ "  DESCRIPTION VARCHAR(4096),"
						+ "  OWNER_ID VARCHAR(32) NOT NULL,"
						+ "  UPDATED_TIME TIMESTAMP NOT NULL,"
						+ "  PUBLIC BOOLEAN NOT NULL DEFAULT FALSE,"
						+ ""
						+ "  CONSTRAINT DATASET_USER_ID_FK FOREIGN KEY (OWNER_ID) REFERENCES USER (ID)"
						+ ");" );

				stat.execute( "CREATE UNIQUE INDEX IF NOT EXISTS DATASET_NAME_OWNER_ID_u_INDEX ON DATASET (NAME, OWNER_ID);" );

				// Create USER/DATASET mapping table
				stat.execute( "CREATE TABLE IF NOT EXISTS USER_DATASET\n"
						+ "("
						+ "  USER_ID VARCHAR(32) NOT NULL,"
						+ "  DATASET_ID INTEGER NOT NULL,"
						+ ""
						+ "  READ BOOLEAN DEFAULT FALSE NOT NULL,"
						+ "  UPDATE BOOLEAN DEFAULT FALSE NOT NULL,"
						+ "  DELETE BOOLEAN DEFAULT FALSE NOT NULL,"
						+ ""
						+ "  UPDATED_TIME TIMESTAMP NOT NULL,"
						+ ""
						+ "  CONSTRAINT USER_DATASET_USER_ID_DATASET_ID_PK PRIMARY KEY (USER_ID, DATASET_ID),"
						+ "  CONSTRAINT USER_DATASET_USER_ID_FK FOREIGN KEY (USER_ID) REFERENCES USER (ID),"
						+ "  CONSTRAINT USER_DATASET_DATASET_ID_FK FOREIGN KEY (DATASET_ID) REFERENCES DATASET (ID)"
						+ ");" );

				// Using Toxi solution from http://vtidter.blogspot.de/2014/02/database-schema-for-tags.html
				// Create TAG table
				stat.execute( "CREATE TABLE IF NOT EXISTS TAG"
						+ "("
						+ "  ID INTEGER AUTO_INCREMENT PRIMARY KEY NOT NULL,"
						+ "  TITLE VARCHAR(255) NOT NULL"
						+ ");" );

				// Create unique index for TAG table
				stat.execute( "CREATE UNIQUE INDEX IF NOT EXISTS TAG_TITLE_uindex ON TAG (TITLE);" );

				// Create TAG/DATASET mapping table
				stat.execute( "CREATE TABLE IF NOT EXISTS TAG_DATASET"
						+ "("
						+ "  TAG_ID INTEGER NOT NULL,"
						+ "  DATASET_ID INTEGER NOT NULL,"
						+ ""
						+ "  CONSTRAINT TAG_DATASET_TAG_ID_DATASET_ID_PK PRIMARY KEY (TAG_ID, DATASET_ID),"
						+ "  CONSTRAINT TAG_DATASET_TAG_ID_FK FOREIGN KEY (TAG_ID) REFERENCES TAG (ID),"
						+ "  CONSTRAINT TAG_DATASET_DATASET_ID_FK FOREIGN KEY (DATASET_ID) REFERENCES DATASET (ID)"
						+ ");" );

				stat.close();
			}
			catch ( SQLException e )
			{
				e.printStackTrace();
			}
		}
	}
}
