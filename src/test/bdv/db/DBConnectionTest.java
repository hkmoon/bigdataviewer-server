package bdv.db;

import bdv.model.DataSet;
import bdv.model.Share;
import bdv.model.User;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;

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
		connect();
		conn.initializeDatabase();
	}

	@After
	public void tearDown() throws Exception
	{
		close();
		conn = null;
	}

	void connect() throws Exception
	{
		assertEquals( true, conn.connect() );
	}

	void close() throws Exception
	{
		conn.close();
		assertEquals( true, conn.isClosed() );
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

		DataSet ds = conn.getDataSet( normalUser, testDataset );
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

		ds = conn.getDataSet( normalUser, testDataset );
		assertNotNull( ds );

		assertEquals( newDescription, ds.getDescription() );
		assertEquals( false, ds.isPublic() );

		// 4. delete the dataset
		conn.removeDataSet( normalUser, ds );

		ds = conn.getDataSet( normalUser, testDataset );
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
		User normalUserFirst = conn.getUser( normalUser1, normalUser1 );

		// 2. create a dataset made by the first normal user
		final String normalDataset1 = "Normal dataset 1";
		conn.addDataSet( normalUserFirst, normalDataset1, "Description", "/local/xml/Test Data.xml", false );

		// 4. create the share with the second user
		DataSet ds = conn.getDataSet( normalUserFirst, normalDataset1 );
		conn.addDataSetShare( normalUserFirst, ds, normalUser2, true, true, false );

		User normalUserSecond = conn.getUser( normalUser2, normalUser2 );
		HashMap< DataSet, Share > result = conn.getDataSetCollection( normalUserSecond );
		for ( DataSet dataSet : result.keySet() )
		{
			assertEquals( true, result.get( dataSet ).isRead() );
			assertEquals( true, result.get( dataSet ).isUpdate() );
			assertEquals( false, result.get( dataSet ).isDelete() );
		}

		conn.updateDataSetShare( normalUserFirst, ds, normalUser2, false, false, false );
		result = conn.getDataSetCollection( normalUserSecond );
		for ( DataSet dataSet : result.keySet() )
		{
			assertEquals( false, result.get( dataSet ).isRead() );
			assertEquals( false, result.get( dataSet ).isUpdate() );
			assertEquals( false, result.get( dataSet ).isDelete() );
		}

		conn.removeDataSetShare( normalUserFirst, ds, normalUser2 );
		result = conn.getDataSetCollection( normalUserSecond );
		assertEquals( 0, result.size() );

		// 5. clean up
		conn.removeDataSet( normalUserFirst, ds );
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
		DataSet ds = conn.getDataSet( user, normalTagDataSet );
		conn.addDataSetTag( ds, "test" );

		// 4. add more tags to the dataset
		conn.addDataSetTag( ds, "test 2" );

		// 5. get the dataset with those tags
		ds = conn.getDataSet( user, normalTagDataSet );
		assertEquals( 2, ds.getTags().size() );

		// 6. delete the tag from the dataset
		conn.removeDataSetTag( ds, "test 2" );
		ds = conn.getDataSet( user, normalTagDataSet );

		// 7. delete all the tags from the dataset
		conn.removeDataSetTag( ds, "test" );
		ds = conn.getDataSet( user, normalTagDataSet );

		// 8. get the dataset with any tag
		assertEquals( 0, ds.getTags().size() );

		// 9. delete the dataset
		conn.removeDataSet( user, ds );

		// 10. delete the user
		conn.removeUser( normalTagUser, normalTagUser );
	}
}