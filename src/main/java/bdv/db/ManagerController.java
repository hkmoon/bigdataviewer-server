package bdv.db;

import bdv.model.DataSet;
import bdv.model.User;

import java.util.List;

/**
 * Author: HongKee Moon (moon@mpi-cbg.de), Scientific Computing Facility
 * Organization: MPI-CBG Dresden
 * Date: December 2016
 */
public class ManagerController extends UserController
{
	/**
	 * User manager status change
	 * @param userId
	 * @param isManager
	 */
	public static void updateUserManager( String userId, boolean isManager )
	{
		conn.updateUser( userId, isManager );
	}

	/**
	 * User name change
	 * @param userId
	 * @param newName
	 */
	public static void updateUserManager( String userId, String newName )
	{
		conn.updateUserName( userId, newName );
	}

	/**
	 * Get all the users in the database
	 * @return
	 */
	public static List< User > getAllUsers()
	{
		return conn.getAllUsers();
	}

	/**
	 * Add a new user
	 * @param userId
	 * @param userName
	 * @param password
	 * @param isManager
	 */
	public static boolean addUser( String userId, String userName, String password, boolean isManager )
	{
		return conn.addUser( userId, userName, password, isManager );
	}

	/**
	 * Remove the user
	 * @param userId
	 */
	public static boolean removeUser( String userId )
	{
		return conn.removeUser( userId );
	}

	public static List< DataSet > getPublicDataSets()
	{
		return conn.getPublicDataSetCollection();
	}

	public static List< DataSet > getPrivateDataSets()
	{
		return conn.getPrivateDataSetCollection();
	}

	public static void updateDataSet( DataSet ds )
	{
		conn.updateDataSet( ds );
	}

	public static void removeDataSet( Long dsId )
	{
		conn.removeDataSet( dsId );
	}
}
