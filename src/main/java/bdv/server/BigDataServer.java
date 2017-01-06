package bdv.server;

import bdv.db.ManagerController;
import bdv.model.DataSet;
import bdv.db.DBLoginService;
import bdv.util.Keystore;

import mpicbg.spim.data.SpimDataException;

import org.apache.commons.cli.*;
import org.apache.commons.lang.StringUtils;
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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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
		final boolean enableManagerContext = false;
		return new Parameters( port, sslPort, hostname, new HashMap< String, DataSet >(), thumbnailDirectory, enableManagerContext );
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

		//TODO: if this is the first time to create the database, we need to create at least one manager user to control databases.

		Handler handler = handlers;
		if ( params.enableManagerContext() )
		{
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

			// Add Statistics bean to the connector
			final ConnectorStatistics connectorStats = new ConnectorStatistics();
			connector.addBean( connectorStats );

			// create StatisticsHandler wrapper and ManagerHandler
			final StatisticsHandler statHandler = new StatisticsHandler();
			handlers.addHandler( new ManagerHandler( server, connectorStats, statHandler, publicDatasetHandlers, privateDatasetHandlers, thumbnailsDirectoryName ) );
			statHandler.setHandler( handlers );

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

		private final boolean enableManagerContext;

		Parameters( final int port, final int sslPort, final String hostname, final Map< String, DataSet > datasetNameToDataSet, final String thumbnailDirectory, final boolean enableManagerContext )
		{
			this.port = port;
			this.sslPort = sslPort;
			this.hostname = hostname;
			this.datasetNameToDataSet = datasetNameToDataSet;
			this.thumbnailDirectory = thumbnailDirectory;
			this.enableManagerContext = enableManagerContext;
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

		public boolean enableManagerContext()
		{
			return enableManagerContext;
		}
	}

	@SuppressWarnings( "static-access" )
	static private Parameters processOptions( final String[] args, final Parameters defaultParameters ) throws IOException
	{
		// create Options object
		final Options options = new Options();

		final String cmdLineSyntax = "BigDataServer [OPTIONS] [NAME XML] ...\n";

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

		// -d or multiple {name name.xml} pairs
		options.addOption( OptionBuilder
				.withDescription( "Dataset file: A plain text file specifying one dataset per line. Each line is formatted as \"NAME <TAB> XML\"." )
				.hasArg()
				.withArgName( "FILE" )
				.create( "d" ) );

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

			boolean enableManagerContext = false;
			int sslPort = defaultParameters.getSslport();

			if ( cmd.hasOption( "m" ) )
			{
				enableManagerContext = true;

				final String securePortString = cmd.getOptionValue( "m", Integer.toString( defaultParameters.getSslport() ) );
				sslPort = Integer.parseInt( securePortString );

				if ( !cmd.hasOption( "d" ) )
					throw new IllegalArgumentException( "Dataset list file is necessary for BigDataServer manager" );
			}

			// Path for holding the dataset file
			if ( cmd.hasOption( "d" ) )
			{
				// process the file given with "-d"
				final String datasetFile = cmd.getOptionValue( "d" );

				// check the file presence
				final Path path = Paths.get( datasetFile );

				if ( Files.notExists( path ) )
					throw new IllegalArgumentException( "Dataset list file does not exist." );

				readDatasetFile( datasets, path );
			}

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

			return new Parameters( port, sslPort, serverName, datasets, thumbnailDirectory, enableManagerContext );
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

	private static void readDatasetFile( final HashMap< String, DataSet > datasets, final Path path ) throws IOException
	{
		// Process dataset list file
		DataSet.setDataSetListPath( path );
		final List< String > lines = Files.readAllLines( path, StandardCharsets.UTF_8 );

		for ( final String str : lines )
		{
			final String[] tokens = str.split( "\\s*\\t\\s*" );
			if ( tokens.length >= 2 && StringUtils.isNotEmpty( tokens[ 0 ].trim() ) && StringUtils.isNotEmpty( tokens[ 1 ].trim() ) )
			{
				final String name = tokens[ 0 ].trim();
				final String xmlpath = tokens[ 1 ].trim();

				if ( tokens.length == 2 )
				{
					tryAddDataset( datasets, name, xmlpath );
				}
				else if ( tokens.length == 5 )
				{
					final String category = tokens[ 2 ].trim();
					final String desc = tokens[ 3 ].trim();
					final String index = tokens[ 4 ].trim();

					tryAddDataset( datasets, name, xmlpath, category, desc, index );
				}
			}
			else
			{
				LOG.warn( "Invalid dataset file line (will be skipped): {" + str + "}" );
			}
		}
	}

	protected static void tryAddDataset( final HashMap< String, DataSet > datasetNameToDataSet, final String... args ) throws IllegalArgumentException
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
			String index = "";

			if ( args.length == 5 )
			{
				category = args[ 2 ];
				desc = args[ 3 ];
				index = args[ 4 ];
			}

			DataSet ds = new DataSet( Integer.parseInt( index ), name, xmlpath, category, desc );
			datasetNameToDataSet.put( name, ds );
			LOG.info( "Dataset added: {" + name + ", " + xmlpath + "}" );
		}
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
