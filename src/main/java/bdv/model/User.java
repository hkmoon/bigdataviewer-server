package bdv.model;

import java.sql.Timestamp;

/**
 * Author: HongKee Moon (moon@mpi-cbg.de), Scientific Computing Facility
 * Organization: MPI-CBG Dresden
 * Date: December 2016
 */
public class User
{
	private final String id;
	private final String name;

	private boolean manager = false;

	private java.sql.Timestamp updatedTime;

	public User( String id, String name, boolean isManager, Timestamp timestamp )
	{
		this.id = id;
		this.name = name;
		this.manager = isManager;
		this.updatedTime = timestamp;
	}

	public String getId()
	{
		return id;
	}

	public String getName()
	{
		return name;
	}

	public boolean isManager()
	{
		return manager;
	}

	public void setManager( boolean manager )
	{
		this.manager = manager;
	}

	public Timestamp getUpdatedTime()
	{
		return updatedTime;
	}

	public void setUpdatedTime( Timestamp updatedTime )
	{
		this.updatedTime = updatedTime;
	}
}
