package bdv.db;

import bdv.model.DataSet;

import java.util.List;

/**
 * Author: HongKee Moon (moon@mpi-cbg.de), Scientific Computing Facility
 * Organization: MPI-CBG Dresden
 * Date: December 2016
 */
public class UserController
{
	static DBConnection conn = new DBConnection();

	protected UserController()
	{
	}

	/**
	 * Get the user's DataSet collection
	 * @param userId the user id
	 * @return all the dataset collection belongs to the specified user
	 */
	public static List< DataSet >[] getDataSets( String userId )
	{
		return conn.getReadableDataSetCollection( userId );
	}

	public static DataSet getDataSet( String userId, long dsId )
	{
		return conn.getOwnDataSet( userId, dsId );
	}

	/**
	 * Get the public DataSet collection
	 * @return all the public dataset collection
	 */
	public static List< DataSet > getPublicDataSets()
	{
		return conn.getPublicDataSetCollection();
	}

	/**
	 * Get the public DataSet collection with the specified tag
	 * @param tag specified tag
	 * @return all the public dataset collection
	 */
	public static List< DataSet > getPublicDataSets( String tag )
	{
		return conn.getPublicDataSetCollection( tag );
	}

	/**
	 * Add a DataSet
	 * @param userId
	 * @param dataSet
	 */
	public static void addDataSet( String userId, DataSet dataSet )
	{
		if ( null == userId || userId.isEmpty() )
		{
			System.err.println( "User id is null or empty" );
			return;
		}

		if ( null == dataSet )
		{
			System.err.println( "DataSet is null" );
			return;
		}

		if ( dataSet.getXmlPath().isEmpty() )
		{
			System.err.println( "DataSet URL is empty" );
			return;
		}

		long id = conn.addDataSet( userId, dataSet.getName(), dataSet.getDescription(), dataSet.getXmlPath(), dataSet.isPublic() );
		dataSet.setIndex( id );

		if ( dataSet.getCategory() != null && !dataSet.getCategory().isEmpty() )
		{
			String[] tags = dataSet.getCategory().split( "," );

			for ( String tag : tags )
			{
				conn.addDataSetTag( id, tag.trim() );
				dataSet.getTags().add( tag );
			}
		}
	}

	/**
	 * Update the DataSet
	 * @param userId
	 * @param dataSet
	 */
	public static void updateDataSet( String userId, DataSet dataSet )
	{
		if ( null == userId || userId.isEmpty() )
		{
			System.err.println( "User id is null or empty" );
			return;
		}

		if ( null == dataSet )
		{
			System.err.println( "DataSet is null" );
			return;
		}

		conn.updateDataSet( userId, dataSet );
	}

	/**
	 * Remove the DataSet
	 * @param userId
	 * @param dataSetId
	 */
	public static void removeDataSet( String userId, Long dataSetId )
	{
		if ( null == userId || userId.isEmpty() )
		{
			System.err.println( "User id is null or empty" );
			return;
		}

		conn.removeDataSet( userId, dataSetId );
	}

	/**
	 * Add a readable SharedUser
	 * @param owner
	 * @param dataSetId
	 * @param sharedUserId
	 */
	public static void addReadableSharedUser( String owner, Long dataSetId, String sharedUserId )
	{
		if ( null == owner || owner.isEmpty() )
		{
			System.err.println( "Owner is null or empty" );
			return;
		}

		if ( null == sharedUserId || sharedUserId.isEmpty() )
		{
			System.err.println( "Sharing user is null or empty" );
			return;
		}

		if ( owner.equals( sharedUserId ) )
		{
			System.err.println( "Owner cannot add him/herself" );
			return;
		}
		conn.addReadableDataSetShare( dataSetId, sharedUserId );
	}

	/**
	 * Remove the readable SharedUser
	 * @param owner
	 * @param dataSetId
	 * @param sharedUserId
	 */
	public static void removeReadableSharedUser( String owner, Long dataSetId, String sharedUserId )
	{
		if ( null == owner || owner.isEmpty() )
		{
			System.err.println( "Owner is null or empty" );
			return;
		}

		if ( null == sharedUserId || sharedUserId.isEmpty() )
		{
			System.err.println( "Sharing user is null or empty" );
			return;
		}

		if ( owner.equals( sharedUserId ) )
		{
			System.err.println( "Owner cannot remove him/herself" );
			return;
		}
		conn.removeReadableDataSetShare( dataSetId, sharedUserId );
	}

	/**
	 * Add a Tag
	 * @param dataSetId
	 * @param tag
	 */
	public static void addTag( Long dataSetId, String tag )
	{
		if ( null == tag || tag.isEmpty() )
		{
			System.err.println( "Tag is null or empty" );
			return;
		}
		conn.addDataSetTag( dataSetId, tag );
	}

	/**
	 * Remove the Tag
	 * @param dataSetId
	 * @param tag
	 */
	public static void removeTag( Long dataSetId, String tag )
	{
		if ( null == tag || tag.isEmpty() )
		{
			System.err.println( "Tag is null or empty" );
			return;
		}
		conn.removeDataSetTag( dataSetId, tag );
	}

	/**
	 * User password change
	 * @param userId
	 * @param password
	 */
	public static void updateUserPassword( String userId, String password )
	{
		if ( null == userId || userId.isEmpty() )
		{
			System.err.println( "User id is null or empty" );
			return;
		}

		if ( null == password || password.isEmpty() )
		{
			System.err.println( "Password is null or empty" );
			return;
		}
		conn.updateUserPassword( userId, password );
	}
}
