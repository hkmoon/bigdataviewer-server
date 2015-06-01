package bdv.server;

import bdv.model.DataSet;
import com.google.gson.stream.JsonWriter;
import mpicbg.spim.data.SpimDataException;

import org.antlr.stringtemplate.StringTemplate;
import org.antlr.stringtemplate.StringTemplateGroup;
import org.apache.commons.collections.Buffer;
import org.apache.commons.collections.BufferUtils;
import org.apache.commons.collections.buffer.CircularFifoBuffer;
import org.eclipse.jetty.server.ConnectorStatistics;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.server.handler.StatisticsHandler;
import org.eclipse.jetty.util.log.Log;
import org.eclipse.jetty.util.resource.Resource;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author HongKee Moon <moon@mpi-cbg.de>
 * @author Tobias Pietzsch <tobias.pietzsch@gmail.com>
 */
public class ManagerHandler extends ContextHandler
{
	private static final org.eclipse.jetty.util.log.Logger LOG = Log.getLogger( ManagerHandler.class );

	private final String baseURL;

	private final Server server;

	private final ContextHandlerCollection handlers;

	private final ConnectorStatistics connectorStats;

	private int noDataSets = 0;

	private long sizeDataSets = 0;

	private final String thumbnailsDirectoryName;

	private long totalSentBytes = 0;

	// Buffer holds 1-hour period bandwidth information
	private Buffer fifo = BufferUtils.synchronizedBuffer( new CircularFifoBuffer( 12 * 60 ) );

	public ManagerHandler(
			final String baseURL,
			final Server server,
			final ConnectorStatistics connectorStats,
			final StatisticsHandler statHandler,
			final ContextHandlerCollection handlers,
			final String thumbnailsDirectoryName )
			throws IOException, URISyntaxException
	{
		this.baseURL = baseURL;
		this.server = server;
		this.handlers = handlers;
		this.connectorStats = connectorStats;
		this.thumbnailsDirectoryName = thumbnailsDirectoryName;
		setContextPath( "/" + Constants.MANAGER_CONTEXT_NAME );

		ResourceHandler resHandler = new ResourceHandler();
		resHandler.setBaseResource( Resource.newClassPathResource( "webapp" ) );
		setHandler( resHandler );
		setWelcomeFiles( new String[] { "index.html" } );

		// Setup the statCollector for collecting statistics in every 5 seconds
		// Adding the data formed as byte/second
		final ScheduledExecutorService statCollector = Executors.newSingleThreadScheduledExecutor();
		statCollector.scheduleAtFixedRate( new Runnable()
		{
			@Override public void run()
			{
				totalSentBytes += statHandler.getResponsesBytesTotal();
				fifo.add( new Long( statHandler.getResponsesBytesTotal() / 5 ) );
				statHandler.statsReset();
			}
		}, 0, 5, TimeUnit.SECONDS );

	}

	@Override
	public void doHandle( final String target, final Request baseRequest, final HttpServletRequest request, final HttpServletResponse response ) throws IOException, ServletException
	{
		final String op = request.getParameter( "op" );

		if ( null != op )
		{
			if ( op.equals( "deploy" ) )
			{
				final String ds = request.getParameter( "ds" );
				final String category = request.getParameter( "category" );
				final String description = request.getParameter( "description" );
				final String index = request.getParameter( "index" );
				final String file = request.getParameter( "file" );
				deploy( ds, category, description, index, file, baseRequest, response );
			}
			else if ( op.equals( "undeploy" ) )
			{
				final String ds = request.getParameter( "ds" );
				undeploy( ds, baseRequest, response );
			}
			else if ( op.equals( "getTrafficData" ) )
			{
				// Provide json type of one hour traffic information
				final String tf = request.getParameter( "tf" );
				final int timeFrame = Integer.parseInt( tf );
				getTraffic( timeFrame, baseRequest, response );
			}
			else if ( op.equals( "getDatasets" ) )
			{
				// Provide json type of datasets
				getDatasets( baseRequest, response );
			}
			else if ( op.equals( "getServerInfo" ) )
			{
				// Provide html type of server information
				getServerInfo( baseRequest, response );
			}
			else if ( op.equals( "activate" ) )
			{
				// Provide html type of server information
				final String datasetName = request.getParameter( "name" );
				final String activated = request.getParameter( "active" );
				activateDataset( datasetName, activated, baseRequest, response );
			}
			else if ( op.equals( "updateDS" ) )
			{
				// UpdateDS uses x-editable
				// Please, refer http://vitalets.github.io/x-editable/docs.html
				final String field = request.getParameter( "name" );
				final String datasetName = request.getParameter( "pk" );
				final String value = request.getParameter( "value" );
				updateDataSet( datasetName, field, value, baseRequest, response );
			}
		}
		else
		{
			super.doHandle( target, baseRequest, request, response );
		}
	}

	public String getByteSizeString( final long size )
	{
		if ( size <= 0 )
			return "0";
		final String[] units = new String[] { "B", "kB", "MB", "GB", "TB" };
		final int digitGroups = ( int ) ( Math.log10( size ) / Math.log10( 1024 ) );
		return new DecimalFormat( "#,##0.#" ).format( size / Math.pow( 1024, digitGroups ) ) + " " + units[ digitGroups ];
	}

	/**
	 * Compute Dataset Statistics including the total size of datasets and the number of datasets
	 * When the new dataset is inserted or deleted, this function should be called in order to keep the statistics
	 * consistent.
	 */
	public void computeDatasetStat()
	{
		noDataSets = 0;
		sizeDataSets = 0;

		for ( final Handler handler : server.getChildHandlersByClass( CellHandler.class ) )
		{
			final CellHandler contextHandler = ( CellHandler ) handler;
			noDataSets++;
			sizeDataSets += contextHandler.getDataSetSize();
		}
	}

	private void deploy( final String datasetName, final String category, final String description, final String index, final String fileLocation, final Request baseRequest, final HttpServletResponse response ) throws IOException
	{
		LOG.info( "Add new context: " + datasetName );
		final String context = "/" + datasetName;

		boolean alreadyExists = false;
		for ( final Handler handler : server.getChildHandlersByClass( CellHandler.class ) )
		{
			final CellHandler contextHandler = ( CellHandler ) handler;
			if ( context.equals( contextHandler.getContextPath() ) )
			{
				LOG.info( "Context " + datasetName + " already exists." );
				alreadyExists = true;
				break;
			}
		}

		if ( !alreadyExists )
		{
			CellHandler ctx = null;
			final DataSet dataSet = new DataSet( datasetName, fileLocation, category, description, index );

			try
			{
				ctx = new CellHandler( baseURL + context + "/", dataSet, thumbnailsDirectoryName );
			}
			catch ( final SpimDataException e )
			{
				LOG.warn( "Failed to create a CellHandler", e );
				e.printStackTrace();
			}
			ctx.setContextPath( context );
			handlers.addHandler( ctx );

			try
			{
				ctx.start();
			}
			catch ( Exception e )
			{
				LOG.warn( "Failed to start CellHandler", e );
				e.printStackTrace();
			}
		}

		response.setContentType( "text/html" );
		response.setStatus( HttpServletResponse.SC_OK );
		baseRequest.setHandled( true );

		final PrintWriter ow = response.getWriter();
		if ( alreadyExists )
			ow.write( "Error: " + datasetName + " already exists. The dataset cannot be deployed." );
		else
			ow.write( "Success: " + datasetName + " is deployed." );
		ow.close();
	}

	private void undeploy( final String datasetName, final Request baseRequest, final HttpServletResponse response ) throws IOException
	{
		LOG.info( "Remove the context: " + datasetName );
		boolean ret = false;

		final String context = "/" + datasetName;
		for ( final Handler handler : server.getChildHandlersByClass( CellHandler.class ) )
		{
			final CellHandler contextHandler = ( CellHandler ) handler;
			if ( context.equals( contextHandler.getContextPath() ) )
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
				handlers.removeHandler( contextHandler );
				ret = true;
				break;
			}
		}

		response.setContentType( "text/html" );
		response.setStatus( HttpServletResponse.SC_OK );
		baseRequest.setHandled( true );

		final PrintWriter ow = response.getWriter();

		if ( ret )
		{
			ow.write( "Success: " + datasetName + " is undeployed." );
		}
		else
		{
			ow.write( "Error: " + datasetName + " cannot be undeployed." );
		}
		ow.close();
	}

	private void getTraffic( final int tf, final Request baseRequest, final HttpServletResponse response ) throws IOException
	{
		response.setContentType( "application/json" );
		response.setStatus( HttpServletResponse.SC_OK );
		baseRequest.setHandled( true );

		final PrintWriter ow = response.getWriter();
		getJsonTrafficData( tf, ow );
		ow.close();
	}

	private void getJsonTrafficData( final int tf, final PrintWriter out ) throws IOException
	{
		final JsonWriter writer = new JsonWriter( out );

		writer.setIndent( "\t" );

		writer.beginArray();

		Long[] dest = new Long[ tf ];

		Long[] src = ( Long[] ) fifo.toArray( new Long[ 1 ] );

		if ( dest.length > src.length )
		{
			System.arraycopy( src, 0, dest, dest.length - src.length, src.length );
		}
		else
		{
			System.arraycopy( src, src.length - dest.length, dest, 0, dest.length );
		}

		for ( int i = 0; i < dest.length; i++ )
		{
			if ( null == dest[ i ] )
				writer.value( 0 );
			else
				writer.value( dest[ i ] );
		}

		writer.endArray();

		writer.flush();

		writer.close();
	}

	private void getDatasets( final Request baseRequest, final HttpServletResponse response ) throws IOException
	{
		response.setContentType( "application/json" );
		response.setStatus( HttpServletResponse.SC_OK );
		baseRequest.setHandled( true );

		final PrintWriter ow = response.getWriter();
		getJsonDatasets( ow );
		ow.close();
	}

	private void getJsonDatasets( final PrintWriter out ) throws IOException
	{
		final JsonWriter writer = new JsonWriter( out );

		writer.setIndent( "\t" );

		writer.beginObject();

		writer.name( "data" );

		writer.beginArray();

		for ( final Handler handler : server.getChildHandlersByClass( CellHandler.class ) )
		{
			final CellHandler contextHandler = ( CellHandler ) handler;
			writer.beginObject();
			writer.name( "active" ).value( contextHandler.isActive() );
			writer.name( "category" ).value( contextHandler.getDataSet().getCategory() );
			writer.name( "name" ).value( contextHandler.getDataSet().getName() );
			writer.name( "index" ).value( contextHandler.getDataSet().getIndex() );
			writer.name( "description" ).value( contextHandler.getDataSet().getDescription() );
			writer.name( "path" ).value( contextHandler.getDataSet().getXmlPath() );
			writer.endObject();
		}

		writer.endArray();

		writer.endObject();

		writer.flush();

		writer.close();
	}

	private void getServerInfo( Request baseRequest, HttpServletResponse response ) throws IOException
	{
		// Calculate the size and the number of the datasets
		computeDatasetStat();

		response.setContentType( "text/html" );
		response.setStatus( HttpServletResponse.SC_OK );
		baseRequest.setHandled( true );

		final PrintWriter ow = response.getWriter();

		final StringTemplateGroup templates = new StringTemplateGroup( "serverInfo" );
		final StringTemplate t = templates.getInstanceOf( "templates/serverInfo" );

		t.setAttribute( "bytesSent", getByteSizeString( totalSentBytes ) );
		t.setAttribute( "msgPerSec", connectorStats.getMessagesOutPerSecond() );
		t.setAttribute( "openConnections", connectorStats.getConnectionsOpen() );
		t.setAttribute( "maxOpenConnections", connectorStats.getConnectionsOpenMax() );
		t.setAttribute( "noDataSets", noDataSets );
		t.setAttribute( "sizeDataSets", getByteSizeString( sizeDataSets ) );

		ow.write( t.toString() );
		ow.close();
	}

	private void activateDataset( String datasetName, String activated, Request baseRequest, HttpServletResponse response ) throws IOException
	{
		response.setContentType( "text/html" );
		response.setStatus( HttpServletResponse.SC_OK );
		baseRequest.setHandled( true );

		final String context = "/" + datasetName;
		for ( final Handler handler : server.getChildHandlersByClass( CellHandler.class ) )
		{
			final CellHandler contextHandler = ( CellHandler ) handler;
			if ( context.equals( contextHandler.getContextPath() ) )
			{
				try
				{
					contextHandler.setActive( activated.equals( "true" ) );
				}
				catch ( SpimDataException e )
				{
					LOG.warn( e );
				}
				break;
			}
		}

		final PrintWriter ow = response.getWriter();
		ow.write( datasetName + " active:" + activated );
		ow.close();
	}

	private void updateDataSet( String datasetName, String field, String value, Request baseRequest, HttpServletResponse response ) throws IOException
	{
		response.setContentType( "text/html" );
		response.setStatus( HttpServletResponse.SC_OK );
		baseRequest.setHandled( true );

		final String context = "/" + datasetName;
		for ( final Handler handler : server.getChildHandlersByClass( CellHandler.class ) )
		{
			final CellHandler contextHandler = ( CellHandler ) handler;
			if ( context.equals( contextHandler.getContextPath() ) )
			{
				final DataSet dataSet = contextHandler.getDataSet();

				if ( field.equals( "category" ) )
					dataSet.setCategory( value );
				else if ( field.equals( "index" ) )
					dataSet.setIndex( value );
				else if ( field.equals( "description" ) )
					dataSet.setDescription( value );

				break;
			}
		}

		// Save the datasets in the given list file
		final ArrayList< DataSet > list = new ArrayList<>();

		for ( final Handler handler : server.getChildHandlersByClass( CellHandler.class ) )
		{
			CellHandler contextHandler = null;
			if ( handler instanceof CellHandler )
			{
				contextHandler = ( CellHandler ) handler;

				if ( contextHandler.isActive() )
				{
					list.add( contextHandler.getDataSet() );

				}
			}
		}

		try
		{
			DataSet.storeDataSet( list );
		}
		catch ( IOException ioexception )
		{
			response.setStatus( HttpServletResponse.SC_BAD_REQUEST );
			final PrintWriter ow = response.getWriter();
			ow.write( "The dataset list is not stored. " + ioexception.getMessage() );
			ow.close();
		}
	}
}
