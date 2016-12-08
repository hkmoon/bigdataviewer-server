package bdv.server;

import bdv.model.DataSet;

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
import java.util.HashMap;

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
		final boolean enableManagerContext = false;
		return new Parameters( port, 0, hostname, new HashMap< String, DataSet >(), thumbnailDirectory, enableManagerContext );
	}

	public static void main( final String[] args ) throws Exception
	{
		System.setProperty( "org.eclipse.jetty.util.log.class", "org.eclipse.jetty.util.log.StdErrLog" );

		final BigDataServer.Parameters params = processOptions( args, getDefaultParameters() );
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
		final String baseURL = "http://" + server.getURI().getHost() + ":" + params.getPort();

		// Handler initialization
		final HandlerCollection handlers = new HandlerCollection();

		final ContextHandlerCollection datasetHandlers = createHandlers( baseURL, params.getDatasets(), thumbnailsDirectoryName );

		final DataSetContextHandler dataSetContextHandler = new DataSetContextHandler( datasetHandlers );

		handlers.addHandler( datasetHandlers );

		handlers.addHandler( dataSetContextHandler );

		handlers.addHandler( new JsonDatasetListHandler( server, datasetHandlers ) );

		Handler handler = handlers;

		handlers.addHandler( new IndexPageHandler( server ) );

		LOG.info( "Set handler: " + handler );
		server.setHandler( handler );
		LOG.info( "Server Base URL: " + baseURL );
		LOG.info( "BigDataServer starting" );
		server.start();
		server.join();
	}

	@SuppressWarnings( "static-access" )
	static private Parameters processOptions( final String[] args, final BigDataServer.Parameters defaultParameters ) throws IOException
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

			final boolean enableManagerContext = false;

			// process additional {name, name.xml} pairs given on the
			// command-line
			final String[] leftoverArgs = cmd.getArgs();
			if ( leftoverArgs.length % 2 != 0 )
				throw new IllegalArgumentException( "Dataset list has an error while processing." );

			for ( int i = 0; i < leftoverArgs.length; i += 2 )
			{
				final String name = leftoverArgs[ i ];
				final String xmlpath = leftoverArgs[ i + 1 ];
				tryAddDataset( datasets, name, xmlpath );
			}

			if ( datasets.isEmpty() )
				throw new IllegalArgumentException( "Dataset list is empty." );

			return new Parameters( port, 0, serverName, datasets, thumbnailDirectory, enableManagerContext );
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
}
