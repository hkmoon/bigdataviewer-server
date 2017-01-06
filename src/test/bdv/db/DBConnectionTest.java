package bdv.db;

import bdv.model.DataSet;
import bdv.model.User;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

/**
 * Author: HongKee Moon (moon@mpi-cbg.de), Scientific Computing Facility
 * Organization: MPI-CBG Dresden
 * Date: December 2016
 */
public class DBConnectionTest
{
	DBConnection conn;

	@Before
	public void setUp() throws Exception
	{
		conn = new DBConnection();
		conn.initializeDatabase();
	}

	@After
	public void tearDown() throws Exception
	{
		conn = null;
	}

	@Test
	public void testNormalUser() throws Exception
	{
		final String testUser = "testUser";
		final String testUserPassword = "testUserPassword";

		// 1. create new user
		conn.addUser( testUser, "Test user", testUserPassword, false );

		User user = conn.getUser( testUser, testUserPassword );

		assertEquals( testUser, user.getId() );
		assertEquals( false, user.isManager() );
		//		System.out.println( moon.getUpdatedTime() );

		// 2. Checking the abnormal user with the wrong password
		User wrongUser = conn.getUser( testUser, "wrongPassword" );
		assertNull( wrongUser );

		// 3. Delete the user
		boolean bRet = conn.removeUser( testUser, testUserPassword );
		assertEquals( true, bRet );
	}

	@Test
	public void testManagerUser() throws Exception
	{
		final String testManager = "testManager";
		final String testManagerPassword = "testManagerPassword";
		// 1. create new user
		conn.addUser( testManager, "Manager for Test", testManagerPassword, true );

		User manager = conn.getUser( testManager, testManagerPassword );

		assertEquals( testManager, manager.getId() );
		assertEquals( true, manager.isManager() );

		// 2. Try to delete the manager
		boolean bRet = conn.removeUser( testManager, testManagerPassword );
		assertEquals( false, bRet );
	}

	@Test
	public void testUserAndDataset() throws Exception
	{
		final String testUser = "normalTestUser1";
		final String testUserPassword = "normalTestUser1Password";

		// 1. create a normal user
		conn.addUser( testUser, "Normal User", testUserPassword );

		// 2. create a dataset
		final String testDataset = "Test Dataset 1";
		final String testDatasetDescription = "Test Dataset description";

		User normalUser = conn.getUser( testUser, testUserPassword );
		assertNotNull( normalUser );

		conn.addDataSet( normalUser, testDataset, testDatasetDescription, "/local/xml/Test Data.xml", true );

		List< DataSet >[] lists = conn.getReadableDataSetCollection( normalUser.getId() );

		DataSet ds = lists[ 0 ].get( 0 );

		final long dsInt = ds.getIndex();

		assertNotNull( ds );

		assertEquals( true, ds.isPublic() );
		assertEquals( testUser, ds.getOwner() );
		assertEquals( testDataset, ds.getName() );
		assertEquals( testDatasetDescription, ds.getDescription() );

		// 3. modify the dataset
		final String newDescription = "new Description";

		ds.setDescription( newDescription );
		ds.setPublic( false );
		conn.updateDataSet( normalUser, ds );

		ds = conn.getOwnDataSet( normalUser.getId(), dsInt );
		assertNotNull( ds );

		assertEquals( newDescription, ds.getDescription() );
		assertEquals( false, ds.isPublic() );

		// 4. delete the dataset
		conn.removeDataSet( normalUser, ds );

		ds = conn.getOwnDataSet( normalUser.getId(), dsInt );
		assertNull( ds );

		// 5. delete the normal user
		conn.removeUser( testUser, testUserPassword );
		normalUser = conn.getUser( testUser, testUserPassword );

		assertNull( normalUser );
	}

	@Test
	public void testNormalUserSharingDataset() throws Exception
	{
		// 1. create two normal users
		final String normalUser1 = "normalSharingUser1";
		final String normalUser2 = "normalSharingUser2";
		conn.addUser( normalUser1, normalUser1, normalUser1 );
		conn.addUser( normalUser2, normalUser2, normalUser2 );

		// 2. create a dataset made by the first normal user
		final String normalDataset1 = "Normal dataset 1";
		conn.addDataSet( normalUser1, normalDataset1, "Description", "/local/xml/Test Data.xml", false );

		// 4. create the share with the second user
		List< DataSet >[] lists = conn.getReadableDataSetCollection( normalUser1 );

		DataSet ds = lists[ 0 ].get( 0 );

		final long dsInt = ds.getIndex();

		conn.addReadableDataSetShare( dsInt, normalUser2 );

		List< DataSet >[] result = conn.getReadableDataSetCollection( normalUser2 );
		for ( DataSet dataSet : result[ 1 ] )
		{
			assertEquals( dsInt, dataSet.getIndex() );
		}

		conn.removeReadableDataSetShare( dsInt, normalUser2 );
		result = conn.getReadableDataSetCollection( normalUser2 );
		assertNull( result[ 0 ] );

		// 5. clean up
		conn.removeDataSet( normalUser1, dsInt );
		conn.removeUser( normalUser1, normalUser1 );
		conn.removeUser( normalUser2, normalUser2 );
	}

	@Test
	public void testTagAndDataset() throws Exception
	{
		// 1. create a user
		final String normalTagUser = "normalTagUser";
		conn.addUser( normalTagUser, normalTagUser, normalTagUser );

		// 2. create a dataset
		final String normalTagDataSet = "normalTagDataSet";
		User user = conn.getUser( normalTagUser, normalTagUser );
		conn.addDataSet( user, normalTagDataSet, "description", "/xml/path", false );

		// 3. create a tag to the dataset
		List< DataSet >[] lists = conn.getReadableDataSetCollection( normalTagUser );
		DataSet ds = lists[ 0 ].get( 0 );

		final long dsIndex = ds.getIndex();

		conn.addDataSetTag( ds, "test" );

		// 4. add more tags to the dataset
		conn.addDataSetTag( ds, "test 2" );

		// 5. get the dataset with those tags
		ds = conn.getOwnDataSet( normalTagUser, dsIndex );
		assertEquals( 2, ds.getTags().size() );

		// 6. delete the tag from the dataset
		conn.removeDataSetTag( ds, "test 2" );
		ds = conn.getOwnDataSet( normalTagUser, dsIndex );

		// 7. delete all the tags from the dataset
		conn.removeDataSetTag( ds, "test" );
		ds = conn.getOwnDataSet( normalTagUser, dsIndex );

		// 8. get the dataset with any tag
		assertEquals( 0, ds.getTags().size() );

		// 9. delete the dataset
		conn.removeDataSet( normalTagUser, dsIndex );

		// 10. delete the user
		conn.removeUser( normalTagUser, normalTagUser );
	}
}