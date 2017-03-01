package bdv.server;

import bdv.db.DBConnection;
import bdv.db.ManagerController;
import bdv.model.DataSet;
import bdv.db.DBLoginService;
import bdv.util.Keystore;

import mpicbg.spim.data.SpimDataException;

import org.apache.commons.cli.*;
import org.eclipse.jetty.security.ConstraintMapping;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.security.authentication.BasicAuthenticator;
import org.eclipse.jetty.server.*;
import org.eclipse.jetty.server.handler.*;
import org.eclipse.jetty.util.log.Log;
import org.eclipse.jetty.util.security.Constraint;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

/**
 * Serve XML/HDF5 datasets over HTTP.
 *
 * <pre>
 * usage: BigDataServer [OPTIONS] [NAME XML]...
 * Serves one or more XML/HDF5 datasets for remote access over HTTP.
 * Provide (NAME XML) pairs on the command line or in a dataset file, where
 * NAME is the name under which the dataset should be made accessible and XML
 * is the path to the XML file of the dataset.
 *  -d &lt;FILE&gt;       Dataset file: A plain text file specifying one dataset
 *                  per line. Each line is formatted as "NAME &lt;TAB&gt; XML".
 *  -m &lt;SECURE_PORT&gt;Manager context HTTPS port. The manager context is automatically enabled.
 *                  (default: 8443)
 *  -p &lt;PORT&gt;       Listening port.
 *                  (default: 8080)
 *  -s &lt;HOSTNAME&gt;   Hostname of the server.
 *  -t &lt;DIRECTORY&gt;  Directory to store thumbnails. (new temporary directory
 *                  by default.)
 * </pre>
 * @author Tobias Pietzsch <tobias.pietzsch@gmail.com>
 * @author HongKee Moon <moon@mpi-cbg.de>
 */
public class BigDataServer
{
	private static final org.eclipse.jetty.util.log.Logger LOG = Log.getLogger( BigDataServer.class );

	static Parameters getDefaultParameters()
	{
		final int port = 8080;
		final int sslPort = 8443;
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
		return new Parameters( port, sslPort, hostname, new HashMap< String, DataSet >(), thumbnailDirectory );
	}

	public static void main( final String[] args ) throws Exception
	{
		System.setProperty( "org.eclipse.jetty.util.log.class", "org.eclipse.jetty.util.log.StdErrLog" );

		final Parameters params = processOptions( args, getDefaultParameters() );
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
		PrivateCellHandler.baseUrl = "https://" + server.getURI().getHost() + ":" + params.getSslport();

		{
			// Database check block
			DBConnection dbConnection = new DBConnection();

			// If this is the first time to create the database,
			// we need to create at least one manager user to control databases.
			if ( dbConnection.hasNoUser() )
			{
				System.out.println( "This is the first time to run BigDataServer with DBMS." );
				final String managerName = System.console().readLine( "Enter name for manager: " );
				final String managerId = System.console().readLine( "Enter id for manager : " );
				final char passwordArray[] = System.console().readPassword( "Enter your manager password: " );

				dbConnection.addUser( managerId, managerName, new String( passwordArray ), true );
			}
		}

		// Public dataset handlers
		final HandlerCollection handlers = new HandlerCollection();

		ContextHandlerCollection publicDatasetHandlers = createPublicHandlers( thumbnailsDirectoryName );

		DataSetContextHandler dataSetContextHandler = new DataSetContextHandler( publicDatasetHandlers, "/" + Constants.PUBLIC_DATASET_CONTEXT_NAME, true );

		handlers.addHandler( publicDatasetHandlers );

		handlers.addHandler( dataSetContextHandler );

		handlers.addHandler( new JsonDatasetListHandler( server, "/" + Constants.PUBLIC_DATASET_TAG_CONTEXT_NAME ) );

		// Private dataset handlers

		ContextHandlerCollection privateDatasetHandlers = createPrivateHandlers( thumbnailsDirectoryName );

		dataSetContextHandler = new DataSetContextHandler( privateDatasetHandlers, "/" + Constants.PRIVATE_DATASET_CONTEXT_NAME, false );

		handlers.addHandler( privateDatasetHandlers );

		handlers.addHandler( dataSetContextHandler );

		handlers.addHandler( new JsonDatasetListHandler( server, "/" + Constants.PRIVATE_DATASET_TAG_CONTEXT_NAME ) );

		handlers.addHandler( new UserPageHandler( server, publicDatasetHandlers, privateDatasetHandlers, thumbnailsDirectoryName ) );

		Handler handler = handlers;

		{
			// SSL for manager context
			if ( !Keystore.checkKeystore() )
				throw new IllegalArgumentException( "Keystore file does not exist." );

			final HttpConfiguration https = new HttpConfiguration();
			https.addCustomizer( new SecureRequestCustomizer() );

			final SslContextFactory sslContextFactory = new SslContextFactory();
			sslContextFactory.setKeyStorePath( Keystore.defaultPath );

			String predefinedKeystorePass = System.getProperty( "keystorepass" );

			if ( null == predefinedKeystorePass )
			{
				final char passwordArray[] = System.console().readPassword( "Please, enter your keystore password: " );
				String password = new String( passwordArray );
				sslContextFactory.setKeyStorePassword( password );
				sslContextFactory.setKeyManagerPassword( password );
			}
			else
			{
				sslContextFactory.setKeyStorePassword( predefinedKeystorePass );
				sslContextFactory.setKeyManagerPassword( predefinedKeystorePass );
			}

			final ServerConnector sslConnector = new ServerConnector( server,
					new SslConnectionFactory( sslContextFactory, "http/1.1" ),
					new HttpConnectionFactory( https ) );
			sslConnector.setHost( params.getHostname() );
			sslConnector.setPort( params.getSslport() );

			server.addConnector( sslConnector );
		}

		{
			// Add Statistics bean to the connector
			final ConnectorStatistics connectorStats = new ConnectorStatistics();
			connector.addBean( connectorStats );

			// create StatisticsHandler wrapper and ManagerHandler
			final StatisticsHandler statHandler = new StatisticsHandler();
			handlers.addHandler( new ManagerHandler( server, connectorStats, statHandler, publicDatasetHandlers, privateDatasetHandlers, thumbnailsDirectoryName ) );
			statHandler.setHandler( handlers );

			// For the manager constraint
			final Constraint constraint = new Constraint();
			constraint.setName( Constraint.__BASIC_AUTH );
			constraint.setRoles( new String[] { "admin" } );
			constraint.setAuthenticate( true );
			// 2 means CONFIDENTIAL. 1 means INTEGRITY
			constraint.setDataConstraint( Constraint.DC_CONFIDENTIAL );

			final ConstraintMapping managerConstraintMapping = new ConstraintMapping();
			managerConstraintMapping.setPathSpec( "/" + Constants.MANAGER_CONTEXT_NAME + "/*" );
			managerConstraintMapping.setConstraint( constraint );

			final DBLoginService loginService = new DBLoginService( "BigDataServerRealm" );
			server.addBean( loginService );

			// For the user constraint
			final Constraint userConstraint = new Constraint();
			userConstraint.setName( Constraint.__BASIC_AUTH );
			userConstraint.setRoles( new String[] { "user" } );
			userConstraint.setAuthenticate( true );
			userConstraint.setDataConstraint( Constraint.DC_INTEGRAL );

			final ConstraintMapping userConstraintMapping = new ConstraintMapping();
			userConstraintMapping.setPathSpec( "/" + Constants.PRIVATE_DOMAIN + "/*" );
			userConstraintMapping.setConstraint( userConstraint );

			final ConstraintSecurityHandler sh = new ConstraintSecurityHandler();
			sh.setLoginService( loginService );
			sh.setAuthenticator( new BasicAuthenticator() );
			sh.addConstraintMapping( managerConstraintMapping );
			sh.addConstraintMapping( userConstraintMapping );
			sh.setHandler( statHandler );

			final HandlerList handlerList = new HandlerList();
			handlerList.addHandler( sh );

			handler = handlerList;
		}

		handlers.addHandler( new IndexPageHandler( server ) );

		LOG.info( "Set handler: " + handler );
		server.setHandler( handler );
		LOG.info( "Server Base URL: " + PublicCellHandler.baseUrl );
		LOG.info( "Server Base HTTPS URL: " + PrivateCellHandler.baseUrl );
		LOG.info( "BigDataServer starting" );
		server.start();
		server.join();
	}

	/**
	 * Server parameters: hostname, port, sslPort, datasets.
	 */
	protected static class Parameters
	{
		private final int port;

		private final String hostname;

		private final int sslPort;

		/**
		 * maps from dataset name to dataset xml path.
		 */
		private final Map< String, DataSet > datasetNameToDataSet;

		private final String thumbnailDirectory;

		Parameters( final int port, final int sslPort, final String hostname, final Map< String, DataSet > datasetNameToDataSet, final String thumbnailDirectory )
		{
			this.port = port;
			this.sslPort = sslPort;
			this.hostname = hostname;
			this.datasetNameToDataSet = datasetNameToDataSet;
			this.thumbnailDirectory = thumbnailDirectory;
		}

		public int getPort()
		{
			return port;
		}

		public String getHostname()
		{
			return hostname;
		}

		public int getSslport()
		{
			return sslPort;
		}

		public String getThumbnailDirectory()
		{
			return thumbnailDirectory;
		}

		/**
		 * Get datasets.
		 * @return datasets as a map from dataset name to dataset xml path.
		 */
		public Map< String, DataSet > getDatasets()
		{
			return datasetNameToDataSet;
		}
	}

	@SuppressWarnings( "static-access" )
	static private Parameters processOptions( final String[] args, final Parameters defaultParameters ) throws IOException
	{
		// create Options object
		final Options options = new Options();

		final String cmdLineSyntax = "BigDataServer [OPTIONS] ...\n";

		final String description =
				"Serves one or more XML/HDF5 datasets for remote access over HTTP.\n" +
						"Please, use the web interface in order to add XML dataset.";

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

		options.addOption( OptionBuilder
				.withDescription( "Manager context HTTPS port. The manager context is automatically enabled." + "\n(default: " + defaultParameters.getSslport() + ")" )
				.hasArg()
				.withArgName( "SECURE_PORT" )
				.create( "m" ) );

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

			int sslPort = defaultParameters.getSslport();

			if ( cmd.hasOption( "m" ) )
			{
				final String securePortString = cmd.getOptionValue( "m", Integer.toString( defaultParameters.getSslport() ) );
				sslPort = Integer.parseInt( securePortString );
			}

			return new Parameters( port, sslPort, serverName, datasets, thumbnailDirectory );
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

	protected static String getThumbnailDirectoryPath( final Parameters params ) throws IOException
	{
		final String thumbnailDirectoryName = params.getThumbnailDirectory();
		if ( thumbnailDirectoryName != null )
		{
			Path thumbnails = Paths.get( thumbnailDirectoryName );
			if ( !Files.exists( thumbnails ) )
			{
				try
				{
					thumbnails = Files.createDirectories( thumbnails );
					return thumbnails.toFile().getAbsolutePath();
				}
				catch ( final IOException e )
				{
					LOG.warn( e.getMessage() );
					LOG.warn( "Could not create thumbnails directory \"" + thumbnailDirectoryName + "\".\n Trying to create temporary directory." );
				}
			}
			else
			{
				if ( !Files.isDirectory( thumbnails ) )
					LOG.warn( "Thumbnails directory \"" + thumbnailDirectoryName + "\" is not a directory.\n Trying to create temporary directory." );
				else
					return thumbnails.toFile().getAbsolutePath();
			}
		}
		final Path thumbnails = Files.createTempDirectory( "thumbnails" );
		thumbnails.toFile().deleteOnExit();
		return thumbnails.toFile().getAbsolutePath();
	}

	protected static ContextHandlerCollection createPrivateHandlers( final String thumbnailsDirectoryName ) throws SpimDataException, IOException
	{
		final ContextHandlerCollection handlers = new ContextHandlerCollection();

		for ( final DataSet ds : ManagerController.getPrivateDataSets() )
		{
			final String context = "/" + Constants.PRIVATE_DATASET_CONTEXT_NAME + "/id/" + ds.getIndex();
			final PrivateCellHandler ctx = new PrivateCellHandler( context + "/", ds, thumbnailsDirectoryName );
			ctx.setContextPath( context );
			handlers.addHandler( ctx );
		}

		return handlers;
	}

	protected static ContextHandlerCollection createPublicHandlers( final String thumbnailsDirectoryName ) throws SpimDataException, IOException
	{
		final ContextHandlerCollection handlers = new ContextHandlerCollection();

		for ( final DataSet ds : ManagerController.getPublicDataSets() )
		{
			final String context = "/" + Constants.PUBLIC_DATASET_CONTEXT_NAME + "/id/" + ds.getIndex();
			final PublicCellHandler ctx = new PublicCellHandler( context + "/", ds, thumbnailsDirectoryName );
			ctx.setContextPath( context );
			handlers.addHandler( ctx );
		}

		return handlers;
	}
}
