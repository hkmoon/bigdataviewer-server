package bdv.server;

import bdv.db.ManagerController;
import bdv.db.UserController;
import bdv.model.DataSet;
import bdv.model.User;
import com.google.gson.stream.JsonWriter;
import mpicbg.spim.data.SpimDataException;

import org.apache.commons.collections.Buffer;
import org.apache.commons.collections.BufferUtils;
import org.apache.commons.collections.buffer.CircularFifoBuffer;
import org.eclipse.jetty.server.ConnectorStatistics;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.server.handler.StatisticsHandler;
import org.eclipse.jetty.util.log.Log;
import org.eclipse.jetty.util.resource.Resource;
import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STGroup;
import org.stringtemplate.v4.STRawGroupDir;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author HongKee Moon <moon@mpi-cbg.de>
 * @author Tobias Pietzsch <tobias.pietzsch@gmail.com>
 */
public class ManagerHandler extends BaseContextHandler
{
	private static final org.eclipse.jetty.util.log.Logger LOG = Log.getLogger( ManagerHandler.class );

	private final ConnectorStatistics connectorStats;

	private int noDataSets = 0;

	private long sizeDataSets = 0;

	private long totalSentBytes = 0;

	// Buffer holds 1-hour period bandwidth information
	private Buffer fifo = BufferUtils.synchronizedBuffer( new CircularFifoBuffer( 12 * 60 ) );

	public ManagerHandler(
			final Server server,
			final ConnectorStatistics connectorStats,
			final StatisticsHandler statHandler,
			final ContextHandlerCollection publicDatasetHandlers,
			final ContextHandlerCollection privateDatasetHandlers,
			final String thumbnailsDirectoryName )
			throws IOException, URISyntaxException
	{
		super( server, publicDatasetHandlers, privateDatasetHandlers, thumbnailsDirectoryName );

		this.connectorStats = connectorStats;
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
			//			System.out.println( request.getParameterMap() );

			if ( op.equals( "addDS" ) )
			{
				final String owner = request.getParameter( "owner" );
				final String name = request.getParameter( "name" );
				final String tags = request.getParameter( "tags" );
				final String description = request.getParameter( "description" );
				final String file = request.getParameter( "file" );
				final boolean isPublic = Boolean.parseBoolean( request.getParameter( "public" ) );

				//				System.out.println( "owner: " + owner );
				//				System.out.println( "name: " + name );
				//				System.out.println( "tags: " + tags );
				//				System.out.println( "description: " + description );
				//				System.out.println( "file: " + file );
				//				System.out.println( "isPublic: " + isPublic );

				deploy( owner, name, tags, description, file, isPublic, baseRequest, response );
			}
			else if ( op.equals( "removeDS" ) )
			{
				final long dsId = Long.parseLong( request.getParameter( "dataset" ) );
				undeploy( dsId, baseRequest, response );
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
			else if ( op.equals( "getUsers" ) )
			{
				getUsers( baseRequest, response );
			}
			else if ( op.equals( "updateDS" ) )
			{
				// UpdateDS uses x-editable
				// Please, refer http://vitalets.github.io/x-editable/docs.html
				final String field = request.getParameter( "name" );
				final Long datasetId = Long.parseLong( request.getParameter( "pk" ) );
				final String value = request.getParameter( "value" );

				updateDataSet( datasetId, field, value, baseRequest, response );
			}
			else if ( op.equals( "addTag" ) || op.equals( "removeTag" ) )
			{
				processTag( op, baseRequest, request, response );
			}
			else if ( op.equals( "addNewUser" ) )
			{
				// Add new user into the database
				final String userId = request.getParameter( "userId" );
				final String userName = request.getParameter( "userName" );
				final String passwd = request.getParameter( "password" );
				final boolean isManager = Boolean.parseBoolean( request.getParameter( "isManager" ) );

				boolean ret = ManagerController.addUser( userId, userName, passwd, isManager );

				response.setContentType( "text/html" );
				response.setStatus( HttpServletResponse.SC_OK );
				baseRequest.setHandled( true );

				final PrintWriter ow = response.getWriter();

				if ( ret )
					ow.write( "Success: " + userId + " created." );
				else
					ow.write( "Error: " + userId + " cannot be created." );

				ow.close();
			}
			else if ( op.equals( "removeUser" ) )
			{
				// Remove the user from the database
				final String userId = request.getParameter( "userId" );

				boolean ret = ManagerController.removeUser( userId );

				response.setContentType( "text/html" );
				response.setStatus( HttpServletResponse.SC_OK );
				baseRequest.setHandled( true );

				final PrintWriter ow = response.getWriter();

				if ( ret )
					ow.write( "Success: " + userId + " removed." );
				else
					ow.write( "Error: " + userId + " cannot be removed." );

				ow.close();
			}
			else if ( op.equals( "updateUser" ) )
			{
				final String field = request.getParameter( "name" );
				final String userId = request.getParameter( "pk" );
				final String value = request.getParameter( "value" );

				if ( field.equals( "manager" ) )
				{
					final boolean isManager = Boolean.parseBoolean( value );
					ManagerController.updateUserManager( userId, isManager );
				}
				else if ( field.equals( "name" ) )
				{
					ManagerController.updateUserManager( userId, value );
				}

				response.setContentType( "text/html" );
				response.setStatus( HttpServletResponse.SC_OK );
				baseRequest.setHandled( true );
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

	private void deploy( final String owner, final String datasetName, final String tags, final String description, final String fileLocation, final boolean isPublic, final Request baseRequest, final HttpServletResponse response ) throws IOException
	{
		boolean ret = false;

		if ( !owner.isEmpty() && !fileLocation.isEmpty() && !datasetName.isEmpty() )
		{
			final DataSet ds = new DataSet( datasetName, fileLocation, tags, description, isPublic );
			ds.setOwner( owner );

			// Store the new dataset
			UserController.addDataSet( owner, ds );

			final String context = "/" + ( isPublic ? Constants.PUBLIC_DATASET_CONTEXT_NAME : Constants.PRIVATE_DATASET_CONTEXT_NAME ) + "/id/" + ds.getIndex();

			LOG.info( "Add new context: " + ds.getName() + " on " + context );

			CellHandler ctx = getCellHandler( ds, isPublic, context );

			ctx.setContextPath( context );

			try
			{
				ctx.start();
			}
			catch ( Exception e )
			{
				LOG.warn( "Failed to start CellHandler", e );
				e.printStackTrace();
			}

			ret = true;
		}

		response.setContentType( "text/html" );
		response.setStatus( HttpServletResponse.SC_OK );
		baseRequest.setHandled( true );

		final PrintWriter ow = response.getWriter();
		if ( ret )
			ow.write( "Success: " + datasetName + " is added." );
		else
			ow.write( "Error: " + datasetName + " cannot be added." );
		ow.close();
	}

	private void undeploy( final long dsId, final Request baseRequest, final HttpServletResponse response ) throws IOException
	{
		LOG.info( "Remove the dataset: " + dsId );

		removeCellHandler( dsId );

		// Remove it from the database
		ManagerController.removeDataSet( dsId );

		response.setContentType( "text/html" );
		response.setStatus( HttpServletResponse.SC_OK );
		baseRequest.setHandled( true );

		final PrintWriter ow = response.getWriter();
		ow.write( "Success: " + dsId + " is undeployed." );
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
			writer.name( "tags" ).value( contextHandler.getDataSet().getTags().stream().collect( Collectors.joining( "," ) ) );
			writer.name( "name" ).value( contextHandler.getDataSet().getName() );
			writer.name( "owner" ).value( contextHandler.getDataSet().getOwner() );
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

		final STGroup g = new STRawGroupDir( "templates", '$', '$' );
		final ST t = g.getInstanceOf( "serverInfo" );

		t.add( "bytesSent", getByteSizeString( totalSentBytes ) );
		t.add( "msgPerSec", connectorStats.getMessagesOutPerSecond() );
		t.add( "openConnections", connectorStats.getConnectionsOpen() );
		t.add( "maxOpenConnections", connectorStats.getConnectionsOpenMax() );
		t.add( "noDataSets", noDataSets );
		t.add( "sizeDataSets", getByteSizeString( sizeDataSets ) );

		ow.write( t.render() );
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

	private void updateDataSet( Long datasetId, String field, String value, Request baseRequest, HttpServletResponse response ) throws IOException
	{
		response.setContentType( "text/html" );
		response.setStatus( HttpServletResponse.SC_OK );
		baseRequest.setHandled( true );

		for ( final Handler handler : server.getChildHandlersByClass( CellHandler.class ) )
		{
			final CellHandler contextHandler = ( CellHandler ) handler;
			if ( contextHandler.getDataSet().getIndex() == datasetId )
			{
				final DataSet dataSet = contextHandler.getDataSet();

				if ( field.equals( "name" ) )
					dataSet.setName( value );
				else if ( field.equals( "description" ) )
					dataSet.setDescription( value );

				ManagerController.updateDataSet( dataSet );

				break;
			}
		}
	}

	private void getUsers( final Request baseRequest, final HttpServletResponse response ) throws IOException
	{
		response.setContentType( "application/json" );
		response.setStatus( HttpServletResponse.SC_OK );
		baseRequest.setHandled( true );

		final PrintWriter ow = response.getWriter();
		getJsonUsers( ow );
		ow.close();
	}

	private void getJsonUsers( final PrintWriter out ) throws IOException
	{
		final JsonWriter writer = new JsonWriter( out );

		writer.setIndent( "\t" );

		writer.beginObject();

		writer.name( "data" );

		writer.beginArray();

		for ( final User user : ManagerController.getAllUsers() )
		{
			writer.beginObject();
			writer.name( "manager" ).value( user.isManager() );
			writer.name( "id" ).value( user.getId() );
			writer.name( "name" ).value( user.getName() );
			writer.name( "timestamp" ).value( new SimpleDateFormat( "MM/dd/yyyy HH:mm:ss" ).format( user.getUpdatedTime() ) );
			writer.endObject();
		}

		writer.endArray();

		writer.endObject();

		writer.flush();

		writer.close();
	}
}
