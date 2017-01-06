package bdv.server;

import bdv.model.DataSet;
import mpicbg.spim.data.SpimDataException;

import java.io.IOException;

/**
 * Author: HongKee Moon (moon@mpi-cbg.de), Scientific Computing Facility
 * Organization: MPI-CBG Dresden
 * Date: December 2016
 */
public class PublicCellHandler extends CellHandler
{
	public static String baseUrl;

	public PublicCellHandler( String context, DataSet dataSet, String thumbnailsDirectory ) throws SpimDataException, IOException
	{
		super( baseUrl + context, dataSet, thumbnailsDirectory );
	}
}
