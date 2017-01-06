package bdv.db;

import bdv.model.DataSet;
import bdv.model.User;

import org.eclipse.jetty.security.DefaultIdentityService;
import org.eclipse.jetty.security.IdentityService;
import org.eclipse.jetty.security.MappedLoginService;
import org.eclipse.jetty.server.UserIdentity;
import org.eclipse.jetty.util.security.Credential;
import org.eclipse.jetty.util.security.Password;
import org.h2.jdbcx.JdbcConnectionPool;

import javax.security.auth.Subject;

import java.security.Principal;
import java.sql.*;
import java.util.ArrayList;
import java.util.Optional;

import static bdv.db.UserController.conn;

/**
 * DBConnection holds the connection of H2 database for {@link bdv.server.BigDataServer} in advanced mode
 *
 * Author: HongKee Moon (moon@mpi-cbg.de), Scientific Computing Facility
 * Organization: MPI-CBG Dresden
 * Date: November 2016
 */
public class DBConnection
{
	private Optional< JdbcConnectionPool > cp = Optional.empty();
	private IdentityService _identityService = new DefaultIdentityService();

	public DBConnection()
	{
	}

	/**
	 * Connect to the default database
	 * @return
	 */
	private Connection getConnection()
	{
		try
		{
			Class.forName( "org.h2.Driver" );
		}
		catch ( ClassNotFoundException e )
		{
			System.err.println( e.getMessage() );
		}

		if ( !cp.isPresent() )
		{
			cp = Optional.of(
					JdbcConnectionPool.create( "jdbc:h2:./etc/BigDataServer", "sa", "" )
			);
		}

		Connection conn = null;
		try
		{
			conn = cp.get().getConnection();
		}
		catch ( SQLException e )
		{
			System.err.println( e.getMessage() );
		}

		//		System.out.println( cp.get().getActiveConnections() );

		return conn;
	}

	/**
	 * Close the current database connection
	 */
	private void close( Connection conn )
	{
		try
		{
			conn.close();
		}
		catch ( SQLException e )
		{
			System.err.println( e.getMessage() );
		}
	}

	/**
	 * Close the current database connection poll
	 */
	public void closeConnectionPool()
	{
		if ( cp.isPresent() )
		{
			try
			{
				Class.forName( "org.h2.Driver" );
			}
			catch ( ClassNotFoundException e )
			{
				System.err.println( e.getMessage() );
			}

			cp.get().dispose();
		}
	}

	/**
	 * Add an User to the database
	 * @param id is the user unique id
	 * @param name is the user name
	 * @param password is the user's password
	 */
	public boolean addUser( String id, String name, String password )
	{
		return addUser( id, name, password, false );
	}

	/**
	 * Add an User to the database with the manager flag
	 * @param id is the user unique id
	 * @param name is the user name
	 * @param password is the user's password
	 * @param isManager true for having the manager role
	 */
	public boolean addUser( String id, String name, String password, boolean isManager )
	{
		boolean ret = false;

		Connection conn = getConnection();
		{
			try
			{
				Statement stat = conn.createStatement();

				String sql = String.format( "MERGE INTO USER ( ID, NAME, PASSWORD, MANAGER, UPDATED_TIME ) VALUES('%s', '%s', "
								+ "'%s', %s, CURRENT_TIMESTAMP() )",
						id, name, Password.MD5.digest( password ), isManager );

				ret = stat.execute( sql );

				stat.close();
			}
			catch ( SQLException e )
			{
				System.err.println( e.getMessage() );
				//				e.printStackTrace();
			}
		}

		close( conn );

		return ret;
	}

	/**
	 * Update the User's manager status that is only used by MANAGER
	 * @param id is the user unique id
	 * @param isManager true for having the manager role
	 */
	public boolean updateUser( String id, boolean isManager )
	{
		boolean ret = false;

		Connection conn = getConnection();
		{
			try
			{
				Statement stat = conn.createStatement();

				String sql = String.format( "MERGE INTO USER ( ID, MANAGER, UPDATED_TIME ) VALUES('%s', "
								+ "%s, CURRENT_TIMESTAMP() )",
						id, isManager );

				ret = stat.execute( sql );

				stat.close();
			}
			catch ( SQLException e )
			{
				System.err.println( e.getMessage() );
				//				e.printStackTrace();
			}
		}

		close( conn );

		return ret;
	}

	/**
	 * Update the User's name
	 * @param id is the user unique id
	 * @param newName will replace the old name
	 */
	public boolean updateUserName( String id, String newName )
	{
		boolean ret = false;

		Connection conn = getConnection();
		{
			try
			{
				Statement stat = conn.createStatement();

				String sql = String.format( "MERGE INTO USER ( ID, Name, UPDATED_TIME ) VALUES('%s', "
								+ "'%s', CURRENT_TIMESTAMP() )",
						id, newName );

				ret = stat.execute( sql );

				stat.close();
			}
			catch ( SQLException e )
			{
				System.err.println( e.getMessage() );
				//				e.printStackTrace();
			}
		}

		close( conn );

		return ret;
	}

	public boolean updateUserPassword( String id, String password )
	{
		boolean ret = false;

		Connection conn = getConnection();
		{
			try
			{
				Statement stat = conn.createStatement();

				String sql = String.format( "MERGE INTO USER ( ID, PASSWORD, UPDATED_TIME ) VALUES('%s', '%s', CURRENT_TIMESTAMP() )",
						id, Password.MD5.digest( password ) );

				ret = stat.execute( sql );

				stat.close();
			}
			catch ( SQLException e )
			{
				System.err.println( e.getMessage() );
				//				e.printStackTrace();
			}
		}

		close( conn );

		return ret;
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

		Connection conn = getConnection();
		{
			try
			{
				Statement stat = conn.createStatement();

				String sql = String.format( "SELECT ID, NAME, MANAGER, UPDATED_TIME from USER where ID = '%s' and PASSWORD = '%s'", id, Password.MD5.digest( password ) );

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
						int n = stat.executeUpdate( String.format( "DELETE from USER where ID = '%s' and PASSWORD = '%s'", id, Password.MD5.digest( password ) ) );
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

		close( conn );

		return ret;
	}

	/**
	 * Remove the user only by MANAGER
	 * @param id is the user unique id
	 * @return true if the user is removed successfully
	 */
	public boolean removeUser( String id )
	{
		boolean ret = false;

		Connection conn = getConnection();
		{
			try
			{
				Statement stat = conn.createStatement();

				String sql = String.format( "SELECT ID, NAME, MANAGER, UPDATED_TIME from USER where ID = '%s'", id );

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
						int n = stat.executeUpdate( String.format( "DELETE from USER where ID = '%s'", id ) );
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

		close( conn );

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

		Connection conn = getConnection();
		{
			try
			{
				Statement stat = conn.createStatement();

				String sql = String.format( "SELECT ID, NAME, MANAGER, UPDATED_TIME from USER where ID = '%s' and PASSWORD = '%s'",
						id, Password.MD5.digest( password ) );

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

		close( conn );

		return user;
	}

	/**
	 * This returns all the users
	 * @return {@link ArrayList} containing all users {@link User}
	 */
	public ArrayList< User > getAllUsers()
	{
		ArrayList< User > users = null;

		Connection conn = getConnection();
		{
			try
			{
				Statement stat = conn.createStatement();

				// Retrieve all the users
				String sql = String.format( "SELECT ID, NAME, MANAGER, UPDATED_TIME from USER" );

				ResultSet rs = stat.executeQuery( sql );

				users = getUsers( rs );

				stat.close();
			}
			catch ( SQLException e )
			{
				System.err.println( e.getMessage() );
				//				e.printStackTrace();
			}
		}

		close( conn );

		return users;
	}

	private ArrayList< User > getUsers( ResultSet rs ) throws SQLException
	{
		ArrayList< User > users = null;

		if ( rs.next() )
		{
			users = new ArrayList<>();

			// Get data from the current row and use it
			do
			{
				final User dataSet = new User( rs.getString( "ID" ),
						rs.getString( "NAME" ),
						rs.getBoolean( "MANAGER" ),
						rs.getTimestamp( "UPDATED_TIME" ) );

				users.add( dataSet );

			} while ( rs.next() );
		}

		return users;
	}

	public UserIdentity getUserIdentity( String id )
	{
		UserIdentity userIdentity = null;

		Connection conn = getConnection();
		{
			try
			{
				Statement stat = conn.createStatement();

				String sql = String.format( "SELECT ID, PASSWORD, MANAGER from USER where ID = '%s'", id );

				ResultSet rs = stat.executeQuery( sql );

				if ( !rs.next() )
				{
					//if rs.next() returns false
					//then there are no rows.
					System.err.println( "No user found with ID=" + id );
				}
				else
				{
					Credential credential = Credential.getCredential( rs.getString( "PASSWORD" ) );

					Principal userPrincipal = new MappedLoginService.KnownUser( id, credential );
					Subject subject = new Subject();
					subject.getPrincipals().add( userPrincipal );
					subject.getPrivateCredentials().add( credential );

					String[] roleArray = rs.getBoolean( "MANAGER" ) ? new String[] { "admin", "user" } : new String[] {
							"user" };
					for ( String role : roleArray )
					{
						subject.getPrincipals().add( new MappedLoginService.RolePrincipal( role ) );
					}

					subject.setReadOnly();
					userIdentity = _identityService.newUserIdentity( subject, userPrincipal, roleArray );
				}

				stat.close();
			}
			catch ( SQLException e )
			{
				System.err.println( e.getMessage() );
				//				e.printStackTrace();
			}
		}

		close( conn );

		return userIdentity;
	}

	/**
	 * Add new DataSet to the database
	 * @param user {@link User} holding the user information
	 * @param name is the {@link DataSet} name
	 * @param description is the description of {@link DataSet}
	 * @param xmlPath is the xml location path of {@link DataSet}
	 * @param isPublic true if the {@link DataSet} is public specified by owner
	 */
	public long addDataSet( User user, String name, String description, String xmlPath, boolean isPublic )
	{
		return addDataSet( user.getId(), name, description, xmlPath, isPublic );
	}

	/**
	 * Add new DataSet to the database
	 * @param userId is user string
	 * @param name is the {@link DataSet} name
	 * @param description is the description of {@link DataSet}
	 * @param xmlPath is the xml location path of {@link DataSet}
	 * @param isPublic true if the {@link DataSet} is public specified by owner
	 */
	public long addDataSet( String userId, String name, String description, String xmlPath, boolean isPublic )
	{
		long ret = -1;

		if ( null == userId || userId.isEmpty() )
		{
			System.err.println( "User is null or empty" );
			return ret;
		}

		Connection conn = getConnection();
		{
			try
			{
				Statement stat = conn.createStatement();

				String sql = String.format( "INSERT INTO DATASET ( NAME, XMLPATH, DESCRIPTION, OWNER_ID, PUBLIC, UPDATED_TIME ) VALUES('%s', '%s', "
								+ "'%s', '%s', %s, CURRENT_TIMESTAMP() )",
						name, xmlPath, description, userId, isPublic );

				stat.executeUpdate( sql );

				ResultSet rs = stat.getGeneratedKeys();
				if( rs.next() )
					ret = rs.getLong( 1 );

				stat.close();
			}
			catch ( SQLException e )
			{
				System.err.println( e.getMessage() );
				//				e.printStackTrace();
			}
		}

		close( conn );

		return ret;
	}

	public DataSet getOwnDataSet( String userId, Long dataSetId )
	{
		if ( null == userId || userId.isEmpty() )
		{
			System.err.println( "User is null or empty" );
			return null;
		}

		DataSet dataSet = null;
		Connection conn = getConnection();
		{
			try
			{
				Statement stat = conn.createStatement();

				String sql = String.format( "SELECT ID, NAME, XMLPATH, DESCRIPTION, OWNER_ID, PUBLIC, UPDATED_TIME from DATASET where ID = %d and OWNER_ID = '%s'", dataSetId, userId );

				ResultSet rs = stat.executeQuery( sql );

				if ( rs.next() )
				{
					// Get data from the current row and use it
					dataSet = new DataSet( rs.getLong( "ID" ),
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

		close( conn );

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
			updateDataSet( dataSet );
		}
	}

	/**
	 * Update the DataSet database with the given {@link DataSet}
	 * @param userId user who can access the specified {@link DataSet}
	 * @param dataSet {@link DataSet} containing the updating information
	 */
	public void updateDataSet( String userId, DataSet dataSet )
	{
		if ( null == userId || userId.isEmpty() )
		{
			System.err.println( "User is null or empty" );
			return;
		}

		if ( dataSet.getOwner().equals( userId ) )
		{
			updateDataSet( dataSet );
		}
	}

	public void updateDataSet( DataSet dataSet )
	{
		Connection conn = getConnection();
		{
			try
			{
				Statement stat = conn.createStatement();

				String sql = String.format( "MERGE INTO DATASET ( ID, NAME, XMLPATH, DESCRIPTION, PUBLIC, UPDATED_TIME ) VALUES("
								+ "%d, '%s', '%s', '%s', %s, CURRENT_TIMESTAMP() )",
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

		close( conn );
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
			removeDataSet( dataSet );
		}
	}

	public void removeDataSet( String userId, Long dataSetId )
	{
		if ( null == userId || userId.isEmpty() )
		{
			System.err.println( "User is null or empty" );
			return;
		}

		DataSet dataSet = getOwnDataSet( userId, dataSetId );

		if ( null != dataSet && dataSet.getOwner().equals( userId ) )
		{
			removeDataSet( dataSet );
		}
	}

	public void removeDataSet( DataSet dataSet )
	{
		removeDataSet( dataSet.getIndex() );
	}

	public void removeDataSet( Long dataSetId )
	{
		Connection conn = getConnection();
		{
			try
			{
				Statement stat = conn.createStatement();

				// Delete the dataset from TAG_DATASET table
				String sql = String.format( "DELETE FROM TAG_DATASET WHERE DATASET_ID = %d",
						dataSetId );

				stat.executeUpdate( sql );

				// Delete the dataset from USER_DATASET table
				sql = String.format( "DELETE FROM USER_DATASET_PERMISSION WHERE DATASET_ID = %d",
						dataSetId );

				stat.executeUpdate( sql );

				sql = String.format( "DELETE FROM DATASET WHERE ID = %d",
						dataSetId );

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

		close( conn );
	}

	/**
	 * Add a share information to enable other user to give access privilege to the specified {@link DataSet}
	 * @param user the owner of the {@link DataSet}
	 * @param dataSet the {@link DataSet} to be shared
	 * @param addingUserId the {@link User} to be shared with
	 */
	public void addReadableDataSetShare( User user, DataSet dataSet, String addingUserId )
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
			addReadableDataSetShare( dataSet.getIndex(), addingUserId );
		}
	}

	public void addReadableDataSetShare( Long dataSetId, String addingUserId )
	{
		if ( null == dataSetId )
		{
			System.err.println( "DataSet is null" );
			return;
		}

		if ( null == addingUserId || addingUserId.isEmpty() )
		{
			System.err.println( "User Id is null or empty" );
			return;
		}

		Connection conn = getConnection();
		{
			try
			{
				Statement stat = conn.createStatement();

				String sql = String.format( "MERGE INTO USER_DATASET_PERMISSION ( USER_ID, DATASET_ID, READ, WRITE, UPDATED_TIME ) VALUES("
								+ "'%s', %d, %s, %s, CURRENT_TIMESTAMP() "
								+ ")",
						addingUserId, dataSetId, true, false );

				stat.execute( sql );

				stat.close();
			}
			catch ( SQLException e )
			{
				System.err.println( e.getMessage() );
				//				e.printStackTrace();
			}
		}

		close( conn );
	}

	/**
	 * Update the share information with the different access privilege
	 * @param user the owner of the {@link DataSet}
	 * @param dataSet the {@link DataSet} to be shared
	 * @param addingUserId the {@link User} to be shared with
	 */
	public void updateReadableDataSetShare( User user, DataSet dataSet, String addingUserId )
	{
		addReadableDataSetShare( user, dataSet, addingUserId );
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
			removeReadableDataSetShare( dataSet.getIndex(), removingUserId );
		}
	}

	public void removeReadableDataSetShare( Long dataSetId, String removingUserId )
	{
		if ( null == dataSetId )
		{
			System.err.println( "DataSet is null" );
			return;
		}

		if ( null == removingUserId || removingUserId.isEmpty() )
		{
			System.err.println( "User Id is null or empty" );
			return;
		}

		Connection conn = getConnection();
		{
			try
			{
				Statement stat = conn.createStatement();

				// Delete the dataset from USER_DATASET table
				String sql = String.format( "DELETE FROM USER_DATASET_PERMISSION WHERE DATASET_ID = %d and USER_ID = '%s'",
						dataSetId, removingUserId );

				stat.executeUpdate( sql );

				stat.close();
			}
			catch ( SQLException e )
			{
				System.err.println( e.getMessage() );
				//				e.printStackTrace();
			}
		}

		close( conn );
	}

	/**
	 * This returns all the accessible DataSet collection for the specified user
	 * @param userId User id
	 * @return {@link ArrayList} containing all the possible {@link DataSet}
	 */
	public ArrayList[] getReadableDataSetCollection( String userId )
	{
		if ( null == userId || userId.isEmpty() )
		{
			System.err.println( "User is null or empty" );
			return null;
		}

		ArrayList< DataSet > myDataSets = null;
		ArrayList< DataSet > sharedDataSets = null;

		Connection conn = getConnection();
		{
			try
			{
				Statement stat = conn.createStatement();

				// Retrieve my dataset collection
				String sql = String.format( "SELECT ID, NAME, XMLPATH, DESCRIPTION, OWNER_ID, PUBLIC, UPDATED_TIME from DATASET where OWNER_ID = '%s'", userId );

				ResultSet rs = stat.executeQuery( sql );

				myDataSets = getDataSetArray( rs );

				// Collect the all the shared users
				for ( DataSet ds : myDataSets )
				{
					sql = String.format( "SELECT USER_ID from DATASET JOIN USER_DATASET_PERMISSION ON DATASET.ID = USER_DATASET_PERMISSION.DATASET_ID WHERE USER_DATASET_PERMISSION.DATASET_ID = %d", ds.getIndex() );
					rs = stat.executeQuery( sql );

					while ( rs.next() )
					{
						ds.addSharedUser( rs.getString( "USER_ID" ) );
					}
				}

				// Retrieve all the dataset shared with me
				sql = String.format( "SELECT DATASET.ID, DATASET.NAME, XMLPATH, DESCRIPTION, OWNER_ID, PUBLIC, DATASET.UPDATED_TIME, READ, WRITE from DATASET JOIN USER_DATASET_PERMISSION ON DATASET.ID = USER_DATASET_PERMISSION.DATASET_ID where USER_DATASET_PERMISSION.USER_ID = '%s'", userId );

				rs = stat.executeQuery( sql );

				sharedDataSets = getDataSetArray( rs );

				rs.close();

				stat.close();
			}
			catch ( SQLException e )
			{
				System.err.println( e.getMessage() );
				//				e.printStackTrace();
			}
		}

		close( conn );

		return new ArrayList[] { myDataSets, sharedDataSets };
	}

	private ArrayList< DataSet > getDataSetArray( ResultSet rs ) throws SQLException
	{
		final ArrayList< DataSet > dataSets = new ArrayList<>();

		Connection conn = getConnection();

		while ( rs.next() )
		{
			// Get data from the current row and use it
			final DataSet dataSet = new DataSet( rs.getLong( "ID" ),
					rs.getString( "NAME" ),
					rs.getString( "XMLPATH" ),
					rs.getString( "DESCRIPTION" ),
					rs.getString( "OWNER_ID" ),
					rs.getBoolean( "PUBLIC" ),
					rs.getTimestamp( "UPDATED_TIME" ) );

			String sql = String.format( "SELECT TITLE from TAG join TAG_DATASET on TAG.ID = TAG_DATASET.TAG_ID where TAG_DATASET.DATASET_ID = %d", dataSet.getIndex() );

			Statement tagStat = conn.createStatement();
			ResultSet tagRs = tagStat.executeQuery( sql );

			while ( tagRs.next() )
			{
				dataSet.addTag( tagRs.getString( "TITLE" ) );
			}

			tagRs.close();
			tagStat.close();

			dataSets.add( dataSet );
		}

		conn.close();

		return dataSets;
	}

	/**
	 * Returns all the DataSet publicly accessible associated with the tag string
	 * @return {@link ArrayList} of {@link DataSet} which is public
	 */
	public ArrayList< DataSet > getPublicDataSetCollection()
	{
		return getDataSetCollection( true );
	}

	/**
	 * Returns all the DataSet privately accessible
	 * @return {@link ArrayList} of {@link DataSet} which is private
	 */
	public ArrayList< DataSet > getPrivateDataSetCollection()
	{
		return getDataSetCollection( false );
	}

	private ArrayList< DataSet > getDataSetCollection( final boolean isPublic )
	{
		ArrayList< DataSet > list = new ArrayList<>();

		Connection conn = getConnection();
		{
			try
			{
				Statement stat = conn.createStatement();

				String sql = String.format( "SELECT ID, NAME, XMLPATH, DESCRIPTION, OWNER_ID, PUBLIC, UPDATED_TIME from DATASET where PUBLIC = " + isPublic );

				ResultSet rs = stat.executeQuery( sql );

				while ( rs.next() )
				{
					// Get data from the current row and use it
					final DataSet dataSet = new DataSet( rs.getLong( "ID" ),
							rs.getString( "NAME" ),
							rs.getString( "XMLPATH" ),
							rs.getString( "DESCRIPTION" ),
							rs.getString( "OWNER_ID" ),
							rs.getBoolean( "PUBLIC" ),
							rs.getTimestamp( "UPDATED_TIME" ) );

					Statement nestedStat = conn.createStatement();

					sql = String.format( "SELECT USER_ID from DATASET JOIN USER_DATASET_PERMISSION ON DATASET.ID = USER_DATASET_PERMISSION.DATASET_ID WHERE USER_DATASET_PERMISSION.DATASET_ID = %d", dataSet.getIndex() );

					ResultSet nestedRs = nestedStat.executeQuery( sql );

					while ( nestedRs.next() )
					{
						dataSet.addSharedUser( nestedRs.getString( "USER_ID" ) );
					}

					sql = String.format( "SELECT TITLE from TAG join TAG_DATASET on TAG.ID = TAG_DATASET.TAG_ID where TAG_DATASET.DATASET_ID = %d", dataSet.getIndex() );
					ResultSet tagRs = nestedStat.executeQuery( sql );

					while ( tagRs.next() )
					{
						dataSet.addTag( tagRs.getString( "TITLE" ) );
					}

					nestedStat.close();

					list.add( dataSet );
				}

				stat.close();
			}
			catch ( SQLException e )
			{
				System.err.println( e.getMessage() );
				//				e.printStackTrace();
			}
		}

		close( conn );

		return list;
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

		ArrayList< DataSet > list = null;

		Connection conn = getConnection();
		{
			try
			{
				Statement stat = conn.createStatement();

				String sql = String.format( "SELECT ID, NAME, XMLPATH, DESCRIPTION, OWNER_ID, PUBLIC, DATASET.UPDATED_TIME from DATASET "
						+ "JOIN TAG_DATASET ON DATASET.ID = TAG_DATASET.DATASET_ID where TAG_DATASET.TITLE = '%s'", tag );

				ResultSet rs = stat.executeQuery( sql );

				list = new ArrayList<>();

				while ( rs.next() )
				{
					final DataSet dataSet = new DataSet( rs.getLong( "ID" ),
							rs.getString( "NAME" ),
							rs.getString( "XMLPATH" ),
							rs.getString( "DESCRIPTION" ),
							rs.getString( "OWNER_ID" ),
							rs.getBoolean( "PUBLIC" ),
							rs.getTimestamp( "UPDATED_TIME" ) );

					list.add( dataSet );
				}

				stat.close();
			}
			catch ( SQLException e )
			{
				System.err.println( e.getMessage() );
				//				e.printStackTrace();
			}
		}

		close( conn );

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

		addDataSetTag( dataSet.getIndex(), tagTitle );
	}

	public void addDataSetTag( Long dataSetId, String tagTitle )
	{
		Connection conn = getConnection();
		{
			try
			{
				Statement stat = conn.createStatement();

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
							rs.getLong( "ID" ), dataSetId );

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

		close( conn );
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

		removeDataSetTag( dataSet.getIndex(), tagTitle );
	}

	public void removeDataSetTag( Long dataSetId, String tagTitle )
	{
		Connection conn = getConnection();
		{
			try
			{
				Statement stat = conn.createStatement();

				String sql = String.format( "SELECT ID from TAG where TITLE = '%s'", tagTitle );

				ResultSet rs = stat.executeQuery( sql );

				if ( rs.next() )
				{
					sql = String.format( "DELETE FROM TAG_DATASET where TAG_ID = %d and DATASET_ID = %d",
							rs.getLong( "ID" ), dataSetId );

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

		close( conn );
	}

	/**
	 * Initialize the H2 database at the first time
	 * All the queries use "IF NOT EXISTS" so that it keeps the original database structure for safety
	 */
	public void initializeDatabase()
	{
		Connection conn = getConnection();
		{
			try
			{
				Statement stat = conn.createStatement();

				// Create USER table
				stat.execute( "CREATE TABLE IF NOT EXISTS USER"
						+ "("
						+ "  ID VARCHAR PRIMARY KEY NOT NULL,"
						+ "  NAME VARCHAR NOT NULL,"
						+ "  PASSWORD CHAR(36) NOT NULL,"
						+ "  UPDATED_TIME TIMESTAMP NOT NULL,"
						+ "  MANAGER BOOLEAN NOT NULL DEFAULT FALSE"
						+ ");"
				);

				// Create DATASET table
				stat.execute( "CREATE TABLE IF NOT EXISTS DATASET"
						+ "("
						+ "  ID IDENTITY,"
						+ "  NAME VARCHAR NOT NULL,"
						+ "  XMLPATH VARCHAR NOT NULL,"
						+ "  DESCRIPTION VARCHAR,"
						+ "  OWNER_ID VARCHAR NOT NULL,"
						+ "  UPDATED_TIME TIMESTAMP NOT NULL,"
						+ "  PUBLIC BOOLEAN NOT NULL DEFAULT FALSE,"
						+ ""
						+ "  CONSTRAINT DATASET_USER_ID_FK FOREIGN KEY (OWNER_ID) REFERENCES USER (ID)"
						+ ");" );

				// Create USER/DATASET mapping table
				stat.execute( "CREATE TABLE IF NOT EXISTS USER_DATASET_PERMISSION"
						+ "("
						+ "  USER_ID VARCHAR NOT NULL,"
						+ "  DATASET_ID LONG NOT NULL,"
						+ ""
						+ "  READ BOOLEAN DEFAULT FALSE NOT NULL,"
						+ "  WRITE BOOLEAN DEFAULT FALSE NOT NULL,"
						+ ""
						+ "  UPDATED_TIME TIMESTAMP NOT NULL,"
						+ ""
						+ "  CONSTRAINT USER_DATASET_PERMISSION_USER_ID_DATASET_ID_PK PRIMARY KEY (USER_ID, DATASET_ID),"
						+ "  CONSTRAINT USER_DATASET_PERMISSION_USER_ID_FK FOREIGN KEY (USER_ID) REFERENCES USER (ID),"
						+ "  CONSTRAINT USER_DATASET_PERMISSION_DATASET_ID_FK FOREIGN KEY (DATASET_ID) REFERENCES DATASET (ID)"
						+ ");" );

				// Using Toxi solution from http://vtidter.blogspot.de/2014/02/database-schema-for-tags.html
				// Create TAG table
				stat.execute( "CREATE TABLE IF NOT EXISTS TAG"
						+ "("
						+ "  ID IDENTITY,"
						+ "  TITLE VARCHAR NOT NULL"
						+ ");" );

				// Create unique index for TAG table
				stat.execute( "CREATE UNIQUE INDEX IF NOT EXISTS TAG_TITLE_uindex ON TAG (TITLE);" );

				// Create TAG/DATASET mapping tables
				stat.execute( "CREATE TABLE IF NOT EXISTS TAG_DATASET"
						+ "("
						+ "  TAG_ID LONG NOT NULL,"
						+ "  DATASET_ID LONG NOT NULL,"
						+ ""
						+ "  CONSTRAINT TAG_DATASET_TAG_ID_DATASET_ID_PK PRIMARY KEY (TAG_ID, DATASET_ID),"
						+ "  CONSTRAINT TAG_DATASET_TAG_ID_FK FOREIGN KEY (TAG_ID) REFERENCES TAG (ID),"
						+ "  CONSTRAINT TAG_DATASET_DATASET_ID_FK FOREIGN KEY (DATASET_ID) REFERENCES DATASET (ID)"
						+ ");" );

				// Create ANNOTATION table
				stat.execute( "CREATE TABLE IF NOT EXISTS ANNOTATION"
						+ "("
						+ "    ID IDENTITY,"
						+ "    TITLE VARCHAR"
						+ ");" );

				// Create ANNOTATION/USER/DATASET mapping table
				stat.execute( "CREATE TABLE IF NOT EXISTS ANNOTATION_USER_DATASET"
						+ "("
						+ "    ANNOTATION_ID LONG,"
						+ "    USER_ID VARCHAR,"
						+ "    DATASET_ID LONG,"
						+ "    VALUE VARCHAR,"
						+ "    UPDATED_TIME TIMESTAMP NOT NULL,"
						+ "    CONSTRAINT ANNOTATION_USER_DATASET_ANNOTATION_ID_fk FOREIGN KEY (ANNOTATION_ID) REFERENCES ANNOTATION (ID),"
						+ "    CONSTRAINT ANNOTATION_USER_DATASET_USER_ID_fk FOREIGN KEY (USER_ID) REFERENCES USER (ID),"
						+ "    CONSTRAINT ANNOTATION_USER_DATASET_DATASET_ID_fk FOREIGN KEY (DATASET_ID) REFERENCES DATASET (ID)"
						+ ");" );

				stat.close();
			}
			catch ( SQLException e )
			{
				e.printStackTrace();
			}
		}

		close( conn );
	}
}
