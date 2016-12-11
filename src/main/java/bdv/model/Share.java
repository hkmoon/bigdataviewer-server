package bdv.model;

/**
 * Author: HongKee Moon (moon@mpi-cbg.de), Scientific Computing Facility
 * Organization: MPI-CBG Dresden
 * Date: December 2016
 */
public class Share
{
	boolean read;
	boolean update;
	boolean delete;

	public Share( boolean read, boolean update, boolean delete )
	{
		this.read = read;
		this.update = update;
		this.delete = delete;
	}

	public boolean isRead()
	{
		return read;
	}

	public void setRead( boolean read )
	{
		this.read = read;
	}

	public boolean isUpdate()
	{
		return update;
	}

	public void setUpdate( boolean update )
	{
		this.update = update;
	}

	public boolean isDelete()
	{
		return delete;
	}

	public void setDelete( boolean delete )
	{
		this.delete = delete;
	}
}
