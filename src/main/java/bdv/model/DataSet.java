package bdv.model;

import org.eclipse.jetty.util.log.Log;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.TreeSet;

/**
 * DataSet class holds SPIM dataset information
 */
public class DataSet
{
	private static final org.eclipse.jetty.util.log.Logger LOG = Log.getLogger( DataSet.class );

	private static Path dataSetListPath;
	/**
	 * DataSet Context name of this {@link bdv.server.CellHandler} is serving.
	 */
	private String name;

	/**
	 * Full path of the dataset xml file this {@link bdv.server.CellHandler} is serving.
	 */
	private final String xmlPath;

	private String category;
	private String description;
	private long index;
	private long size;

	private String thumbnailUrl;
	private String datasetUrl;

	public static int numBackups = 5;

	private String owner;
	private boolean publicAccessble = false;
	private java.sql.Timestamp updatedTime;

	private final TreeSet< String > sharedUsers = new TreeSet<>();
	private final TreeSet< String > tags = new TreeSet<>();

	/**
	 * Instantiates a new DataSet
	 *
	 * @param index the index of the dataset
	 * @param name the dataset Context name
	 * @param xmlPath the dataset XML file path
	 * @param category the category of the dataset
	 * @param description the description of the dataset
	 */
	public DataSet( long index, String name, String xmlPath, String category, String description )
	{
		this.name = name;
		this.xmlPath = xmlPath;
		this.category = category;
		this.description = description;
		this.index = index;
	}

	public DataSet( String name, String xmlPath, String category, String description )
	{
		this.name = name;
		this.xmlPath = xmlPath;
		this.category = category;
		this.description = description;
	}

	public DataSet( String name, String xmlPath, String category, String description, boolean isPublic )
	{
		this.name = name;
		this.xmlPath = xmlPath;
		this.category = category;
		this.description = description;
		this.publicAccessble = isPublic;
	}

	public DataSet( long index, String name, String xmlPath, String description, String owner, boolean isPublic, Timestamp updatedTime )
	{
		this.index = index;
		this.name = name;
		this.xmlPath = xmlPath;
		this.description = description;
		this.owner = owner;
		this.publicAccessble = isPublic;
		this.updatedTime = updatedTime;
	}


	/**
	 * Sets dataSetList path.
	 *
	 * @param dataSetListPath the data set list path
	 */
	public static void setDataSetListPath( Path dataSetListPath )
	{
		DataSet.dataSetListPath = dataSetListPath;
	}

	/**
	 * Gets the dataset name.
	 *
	 * @return the dataset name
	 */
	public String getName()
	{
		return name;
	}

	public void setName( String name )
	{
		this.name = name;
	}

	/**
	 * Gets the dataset xml file path.
	 *
	 * @return the xml file path
	 */
	public String getXmlPath()
	{
		return xmlPath;
	}

	/**
	 * Gets the dataset category.
	 *
	 * @return the dataset category name
	 */
	public String getCategory()
	{
		return category;
	}

	/**
	 * Sets the dataset category name.
	 *
	 * @param category the category name
	 */
	public void setCategory( String category )
	{
		this.category = category;
	}

	/**
	 * Gets description.
	 *
	 * @return the description
	 */
	public String getDescription()
	{
		return description;
	}

	/**
	 * Sets description.
	 *
	 * @param description the description
	 */
	public void setDescription( String description )
	{
		this.description = description;
	}

	/**
	 * Gets index.
	 *
	 * @return the index
	 */
	public long getIndex()
	{
		return index;
	}

	/**
	 * Sets index.
	 *
	 * @param index the index
	 */
	public void setIndex( long index )
	{
		this.index = index;
	}

	/**
	 * Gets dataset size.
	 *
	 * @return the dataset size
	 */
	public long getSize()
	{
		return size;
	}

	/**
	 * Sets dataset size.
	 *
	 * @param size the dataset size
	 */
	public void setSize( long size )
	{
		this.size = size;
	}

	/**
	 * Gets thumbnail url.
	 *
	 * @return the thumbnail url
	 */
	public String getThumbnailUrl()
	{
		return datasetUrl + "png";
	}

	/**
	 * Gets dataset url.
	 *
	 * @return the dataset url
	 */
	public String getDatasetUrl()
	{
		return datasetUrl;
	}

	/**
	 * Sets dataset url.
	 *
	 * @param datasetUrl the dataset url
	 */
	public void setDatasetUrl( String datasetUrl )
	{
		this.datasetUrl = datasetUrl;
	}

	public String getOwner()
	{
		return owner;
	}

	public void setOwner( String owner )
	{
		this.owner = owner;
	}

	public boolean isOwned( String user )
	{
		return owner.equals( user );
	}

	public Timestamp getUpdatedTime()
	{
		return updatedTime;
	}

	public void setUpdatedTime( Timestamp updatedTime )
	{
		this.updatedTime = updatedTime;
	}

	public boolean isPublic()
	{
		return publicAccessble;
	}

	public void setPublic( boolean publicAccessble )
	{
		this.publicAccessble = publicAccessble;
	}

	public TreeSet< String > getTags()
	{
		return tags;
	}

	public void addTag( String title )
	{
		tags.add( title );
	}

	public TreeSet< String > getSharedUsers()
	{
		return sharedUsers;
	}

	public void addSharedUser( String userId )
	{
		sharedUsers.add( userId );
	}

	/**
	 * Store datasets
	 * @param list the dataset list
	 * @throws IOException the iO exception
	 */
	public static void storeDataSet( ArrayList< DataSet > list ) throws IOException
	{
		if ( dataSetListPath != null )
		{
			// fist make a copy of the XML and save it to not loose it
			if ( Files.exists( dataSetListPath ) )
			{
				int maxExistingBackup = 0;
				for ( int i = 1; i < numBackups; ++i )
					if ( Files.exists( Paths.get( dataSetListPath + "~" + i ) ) )
						maxExistingBackup = i;
					else
						break;

				// copy the backups
				try
				{
					for ( int i = maxExistingBackup; i >= 1; --i )
						Files.copy( Paths.get( dataSetListPath + "~" + i ),
								Paths.get( dataSetListPath + "~" + ( i + 1 ) ),
								StandardCopyOption.COPY_ATTRIBUTES, StandardCopyOption.REPLACE_EXISTING );

					Files.copy( dataSetListPath, Paths.get( dataSetListPath + "~" + 1 ),
							StandardCopyOption.COPY_ATTRIBUTES, StandardCopyOption.REPLACE_EXISTING );
				}
				catch ( final IOException e )
				{
					LOG.warn( "Could not save backup of data list file: " + e );
					e.printStackTrace();
				}
			}

			BufferedWriter writer = Files.newBufferedWriter( dataSetListPath, StandardCharsets.UTF_8 );
			for ( DataSet ds : list )
			{
				writer.write( ds.getName() );
				writer.write( '\t' );
				writer.write( ds.getXmlPath() );
				writer.write( '\t' );
				writer.write( ds.getCategory() );
				writer.write( '\t' );
				writer.write( ds.getDescription() );
				writer.write( '\t' );
				writer.write( ds.getIndex() + "" );
				writer.write( '\n' );
			}
			writer.flush();
			writer.close();
		}
	}
}
