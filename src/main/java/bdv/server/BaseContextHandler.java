package bdv.server;

import bdv.db.UserController;
import bdv.model.DataSet;
import mpicbg.spim.data.SpimDataException;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.util.log.Log;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;

/**
 * Author: HongKee Moon (moon@mpi-cbg.de), Scientific Computing Facility
 * Organization: MPI-CBG Dresden
 * Date: January 2017
 */
public class BaseContextHandler extends ContextHandler
{
	private static final org.eclipse.jetty.util.log.Logger LOG = Log.getLogger( BaseContextHandler.class );

	protected final Server server;

	private final ContextHandlerCollection publicDatasetHandlers;

	private final ContextHandlerCollection privateDatasetHandlers;

	private final String thumbnailsDirectoryName;

	BaseContextHandler(
			final Server server,
			final ContextHandlerCollection publicDatasetHandlers,
			final ContextHandlerCollection privateDatasetHandlers,
			final String thumbnailsDirectoryName
	)
	{
		this.server = server;
		this.publicDatasetHandlers = publicDatasetHandlers;
		this.privateDatasetHandlers = privateDatasetHandlers;
		this.thumbnailsDirectoryName = thumbnailsDirectoryName;
	}

	void processTag( final String op, final Request baseRequest,
			HttpServletRequest request, HttpServletResponse response )
	{
		final long dataSetId = Long.parseLong( request.getParameter( "dataset" ) );
		final String tagString = request.getParameter( "tag" );

		if ( op.equals( "addTag" ) )
		{
			//			System.out.println("Add a Tag");
			UserController.addTag( dataSetId, tagString );
			updateHandlerTag( dataSetId, true, tagString, baseRequest, response );
		}
		else if ( op.equals( "removeTag" ) )
		{
			//			System.out.println("Remove the Tag");
			UserController.removeTag( dataSetId, tagString );
			updateHandlerTag( dataSetId, false, tagString, baseRequest, response );
		}
	}

	private void updateHandlerTag( long dataSetId, boolean isAdd, String tagString, Request baseRequest, HttpServletResponse response )
	{
		for ( final Handler handler : server.getChildHandlersByClass( CellHandler.class ) )
		{
			final CellHandler contextHandler = ( CellHandler ) handler;
			if ( contextHandler.getDataSet().getIndex() == dataSetId )
			{
				final DataSet dataSet = contextHandler.getDataSet();

				if ( isAdd )
					dataSet.getTags().add( tagString );
				else
					dataSet.getTags().remove( tagString );

				break;
			}
		}

		response.setContentType( "text/html" );
		response.setStatus( HttpServletResponse.SC_OK );
		baseRequest.setHandled( true );
	}

	final CellHandler getCellHandler( DataSet ds, boolean isPublic, String context ) throws IOException
	{
		CellHandler ctx = null;

		try
		{
			if ( isPublic )
				ctx = new PublicCellHandler( context + "/", ds, thumbnailsDirectoryName );
			else
				ctx = new PrivateCellHandler( context + "/", ds, thumbnailsDirectoryName );
		}
		catch ( final SpimDataException e )
		{
			LOG.warn( "Failed to create a CellHandler", e );
			e.printStackTrace();
		}

		if ( isPublic )
			publicDatasetHandlers.addHandler( ctx );
		else
			privateDatasetHandlers.addHandler( ctx );

		return ctx;
	}

	final boolean removeCellHandler( long index )
	{
		boolean ret = false;

		for ( final Handler handler : server.getChildHandlersByClass( CellHandler.class ) )
		{
			final CellHandler contextHandler = ( CellHandler ) handler;
			if ( contextHandler.getDataSet().getIndex() == index )
			{
				try
				{
					contextHandler.stop();
				}
				catch ( final Exception e )
				{
					LOG.warn( "Failed to remove the CellHandler", e );
					e.printStackTrace();
				}

				contextHandler.destroy();

				if ( contextHandler.getDataSet().isPublic() )
					publicDatasetHandlers.removeHandler( contextHandler );
				else
					privateDatasetHandlers.removeHandler( contextHandler );

				ret = true;
				break;
			}
		}

		return ret;
	}
}
