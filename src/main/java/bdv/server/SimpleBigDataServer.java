package bdv.server;

import bdv.model.DataSet;

import mpicbg.spim.data.SpimDataException;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.util.log.Log;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

/**
 * Simple BigDataServe XML/HDF5 datasets over HTTP.
 *
 * <pre>
 * usage: SimpleBigDataServer [OPTIONS] [NAME XML]...
 * Serves one or more XML/HDF5 datasets for remote access over HTTP.
 * Provide (NAME XML) pairs on the command line, where
 * NAME is the name under which the dataset should be made accessible and XML
 * is the path to the XML file of the dataset.
 *  -p &lt;PORT&gt;       Listening port.
 *                  (default: 8080)
 *  -s &lt;HOSTNAME&gt;   Hostname of the server.
 *  -t &lt;DIRECTORY&gt;  Directory to store thumbnails. (new temporary directory
 *                  by default.)
 * </pre>
 * @author Tobias Pietzsch <tobias.pietzsch@gmail.com>
 * @author HongKee Moon <moon@mpi-cbg.de>
 */
public class SimpleBigDataServer extends BigDataServer
{
	private static final org.eclipse.jetty.util.log.Logger LOG = Log.getLogger( SimpleBigDataServer.class );

	static Parameters getDefaultParameters()
	{
		final int port = 8080;
		String hostname;
		try
		{
			hostname = InetAddress.getLocalHost().getHostName();
		}
		catch ( final UnknownHostException e )
		{
			hostname = "localhost";
		}
		final String thumbnailDirectory = null;
		return new Parameters( port, 0, hostname, new HashMap< String, DataSet >(), thumbnailDirectory );
	}

	public static void main( final String[] args ) throws Exception
	{
		System.setProperty( "org.eclipse.jetty.util.log.class", "org.eclipse.jetty.util.log.StdErrLog" );

		final SimpleBigDataServer.Parameters params = processOptions( args, getDefaultParameters() );
		if ( params == null )
			return;

		final String thumbnailsDirectoryName = getThumbnailDirectoryPath( params );

		// Threadpool for multiple connections
		final Server server = new Server( new QueuedThreadPool( 200, 8 ) );

		// HTTP Configuration
		HttpConfiguration httpConfig = new HttpConfiguration();
		httpConfig.setSecureScheme( "https" );
		httpConfig.setSecurePort( params.getSslport() );

		// Setup buffers on http
		final HttpConnectionFactory httpConnectionFactory = new HttpConnectionFactory( httpConfig );
		httpConnectionFactory.setInputBufferSize( 64 * 1024 );

		// ServerConnector configuration
		final ServerConnector connector = new ServerConnector( server, httpConnectionFactory );
		connector.setHost( params.getHostname() );
		connector.setPort( params.getPort() );
		LOG.info( "Set connectors: " + connector );
		server.setConnectors( new Connector[] { connector } );
		PublicCellHandler.baseUrl = "http://" + server.getURI().getHost() + ":" + params.getPort();

		// Handler initialization
		final HandlerCollection handlers = new HandlerCollection();

		final ContextHandlerCollection datasetHandlers = createHandlers( params.getDatasets(), thumbnailsDirectoryName );

		final DataSetContextHandler dataSetContextHandler = new DataSetContextHandler( datasetHandlers, "/" + Constants.DATASET_CONTEXT_NAME, true );

		handlers.addHandler( datasetHandlers );

		handlers.addHandler( dataSetContextHandler );

		handlers.addHandler( new JsonDatasetListHandler( server, "/" + Constants.DATASETLIST_CONTEXT_NAME ) );

		Handler handler = handlers;

		handlers.addHandler( new IndexPageHandler( server ) );

		LOG.info( "Set handler: " + handler );
		server.setHandler( handler );
		LOG.info( "Server Base URL: " + PublicCellHandler.baseUrl );
		LOG.info( "BigDataServer starting" );
		server.start();
		server.join();
	}

	protected static void tryAddDataset( final HashMap< String, DataSet > datasetNameToDataSet, final int index, final String... args ) throws IllegalArgumentException
	{
		if ( args.length >= 2 )
		{
			final String name = args[ 0 ];
			final String xmlpath = args[ 1 ];

			for ( final String reserved : Constants.RESERVED_CONTEXT_NAMES )
				if ( name.equals( reserved ) )
					throw new IllegalArgumentException( "Cannot use dataset name: \"" + name + "\" (reserved for internal use)." );
			if ( datasetNameToDataSet.containsKey( name ) )
				throw new IllegalArgumentException( "Duplicate dataset name: \"" + name + "\"" );
			if ( Files.notExists( Paths.get( xmlpath ) ) )
				throw new IllegalArgumentException( "Dataset file does not exist: \"" + xmlpath + "\"" );

			String category = "";
			String desc = "";

			if ( args.length == 5 )
			{
				category = args[ 2 ];
				desc = args[ 3 ];
			}

			DataSet ds = new DataSet( index, name, xmlpath, category, desc );
			datasetNameToDataSet.put( name, ds );
			LOG.info( "Dataset added: {" + name + ", " + xmlpath + "}" );
		}
	}

	@SuppressWarnings( "static-access" )
	static private Parameters processOptions( final String[] args, final SimpleBigDataServer.Parameters defaultParameters ) throws IOException
	{
		// create Options object
		final Options options = new Options();

		final String cmdLineSyntax = "SimpleBigDataServer [OPTIONS] [NAME XML] ...\n";

		final String description =
				"Serves one or more XML/HDF5 datasets for remote access over HTTP.\n" +
						"Provide (NAME XML) pairs on the command line or in a dataset file, where NAME is the name under which the dataset should be made accessible and XML is the path to the XML file of the dataset.";

		options.addOption( OptionBuilder
				.withDescription( "Hostname of the server.\n(default: " + defaultParameters.getHostname() + ")" )
				.hasArg()
				.withArgName( "HOSTNAME" )
				.create( "s" ) );

		options.addOption( OptionBuilder
				.withDescription( "Listening port.\n(default: " + defaultParameters.getPort() + ")" )
				.hasArg()
				.withArgName( "PORT" )
				.create( "p" ) );

		options.addOption( OptionBuilder
				.withDescription( "Directory to store thumbnails. (new temporary directory by default.)" )
				.hasArg()
				.withArgName( "DIRECTORY" )
				.create( "t" ) );

		try
		{
			final CommandLineParser parser = new BasicParser();
			final CommandLine cmd = parser.parse( options, args );

			// Getting port number option
			final String portString = cmd.getOptionValue( "p", Integer.toString( defaultParameters.getPort() ) );
			final int port = Integer.parseInt( portString );

			// Getting server name option
			final String serverName = cmd.getOptionValue( "s", defaultParameters.getHostname() );

			// Getting thumbnail directory option
			final String thumbnailDirectory = cmd.getOptionValue( "t", defaultParameters.getThumbnailDirectory() );

			final HashMap< String, DataSet > datasets = new HashMap< String, DataSet >( defaultParameters.getDatasets() );

			// process additional {name, name.xml} pairs given on the
			// command-line
			final String[] leftoverArgs = cmd.getArgs();
			if ( leftoverArgs.length % 2 != 0 )
				throw new IllegalArgumentException( "Dataset list has an error while processing." );

			for ( int i = 0; i < leftoverArgs.length; i += 2 )
			{
				final String name = leftoverArgs[ i ];
				final String xmlpath = leftoverArgs[ i + 1 ];
				tryAddDataset( datasets, ( i / 2 ) + 1, name, xmlpath );
			}

			if ( datasets.isEmpty() )
				throw new IllegalArgumentException( "Dataset list is empty." );

			return new Parameters( port, 0, serverName, datasets, thumbnailDirectory );
		}
		catch ( final ParseException | IllegalArgumentException e )
		{
			LOG.warn( e.getMessage() );
			System.out.println();
			final HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp( cmdLineSyntax, description, options, null );
		}
		return null;
	}

	protected static ContextHandlerCollection createHandlers( final Map< String, DataSet > dataSet, final String thumbnailsDirectoryName ) throws SpimDataException, IOException
	{
		final ContextHandlerCollection handlers = new ContextHandlerCollection();

		for ( final Map.Entry< String, DataSet > entry : dataSet.entrySet() )
		{
			final String name = entry.getKey();
			final DataSet ds = entry.getValue();
			final String context = "/" + Constants.DATASET_CONTEXT_NAME + "/" + name;
			final PublicCellHandler ctx = new PublicCellHandler( context + "/", ds, thumbnailsDirectoryName );
			ctx.setContextPath( context );
			handlers.addHandler( ctx );
		}

		return handlers;
	}
}
