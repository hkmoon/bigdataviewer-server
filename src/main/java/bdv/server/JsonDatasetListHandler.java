package bdv.server;

import bdv.model.DataSet;
import com.google.gson.stream.JsonWriter;
import org.apache.commons.lang.StringUtils;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.util.log.Log;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.stream.Collectors;

/**
 * Provides a list of available datasets on this {@link BigDataServer}
 *
 * @author HongKee Moon &lt;moon@mpi-cbg.de&gt;
 */
public class JsonDatasetListHandler extends ContextHandler
{
	private static final org.eclipse.jetty.util.log.Logger LOG = Log.getLogger( JsonDatasetListHandler.class );

	private final Server server;

	public JsonDatasetListHandler( final Server server, final String contextPath ) throws IOException, URISyntaxException
	{
		this.server = server;
		setContextPath( contextPath );
	}

	@Override
	public void doHandle( final String target, final Request baseRequest, final HttpServletRequest request, final HttpServletResponse response ) throws IOException, ServletException
	{
		Principal user = request.getUserPrincipal();

		if ( null != user )
			LOG.info( user.getName() );

		final String tag = request.getParameter( "tag" );
		if ( StringUtils.isEmpty( tag ) )
		{
			if ( user != null )
				list( user.getName(), baseRequest, response );
			else
				list( null, baseRequest, response );
		}
		else
		{
			listCategory( user.getName(), tag, baseRequest, response );
		}
	}

	private void listCategory( final String userId, String tag, Request baseRequest, HttpServletResponse response ) throws IOException
	{
		response.setContentType( "application/json" );
		response.setStatus( HttpServletResponse.SC_OK );
		baseRequest.setHandled( true );

		final PrintWriter ow = response.getWriter();
		getJsonDatasetList( userId, tag, ow );
		ow.close();
	}

	private void list( final String userId, final Request baseRequest, final HttpServletResponse response ) throws IOException
	{
		response.setContentType( "application/json" );
		response.setStatus( HttpServletResponse.SC_OK );
		baseRequest.setHandled( true );

		final PrintWriter ow = response.getWriter();
		getJsonDatasetList( userId, null, ow );
		ow.close();
	}

	private void getJsonDatasetList( final String userId, String tag, final PrintWriter out ) throws IOException
	{
		final JsonWriter writer = new JsonWriter( out );

		writer.setIndent( "\t" );

		writer.beginObject();

		getContexts( userId, tag, writer );

		writer.endObject();

		writer.flush();

		writer.close();
	}

	private String getContexts( final String userId, final String tag, final JsonWriter writer ) throws IOException
	{
		final ArrayList<DataSet> list = new ArrayList<>();

		for ( final Handler handler : server.getChildHandlersByClass( CellHandler.class ) )
		{
			CellHandler contextHandler = null;
			if ( handler instanceof CellHandler )
			{
				contextHandler = ( CellHandler ) handler;

				if ( contextHandler.isActive() )
				{
					DataSet ds = contextHandler.getDataSet();
					if ( ds.isPublic() )
					{
						if ( null == tag || tag.isEmpty() ||
								( null != tag && ds.getTags().contains( tag ) ) )
							list.add( ds );
					}
					else
					{
						if ( null != userId && ( ds.getOwner().equals( userId ) || ds.getSharedUsers().contains( userId ) ) )
							list.add( ds );
					}
				}
			}
		}

		// Sort the list by Category and Index
		//		Collections.sort( list, new Comparator< DataSet >()
		//		{
		//			@Override
		//			public int compare( final DataSet lhs, DataSet rhs )
		//			{
		//				// return 1 if rhs should be before lhs
		//				// return -1 if lhs should be before rhs
		//				// return 0 otherwise
		//				if( lhs.getCategory().equals( rhs.getCategory() ))
		//				{
		//					return (int) (lhs.getIndex() - rhs.getIndex());
		//				}
		//				else
		//				{
		//					return lhs.getCategory().compareToIgnoreCase( rhs.getCategory() );
		//				}
		//			}
		//		} );

		// Build json list
		final StringBuilder sb = new StringBuilder();
		for(DataSet ds : list)
		{
			writer.name( ds.getIndex() + "" ).beginObject();

			writer.name( "name" ).value( ds.getName() );

			writer.name( "tags" ).value( ds.getTags().stream().collect( Collectors.joining( "," ) ) );

			writer.name( "description" ).value( ds.getDescription() );

			writer.name( "index" ).value( ds.getIndex() );

			writer.name( "thumbnailUrl" ).value( ds.getThumbnailUrl() );

			writer.name( "datasetUrl" ).value( ds.getDatasetUrl() );

			writer.name( "sharedBy" ).value( !ds.getOwner().equals( userId ) ? ds.getOwner() : "" );

			writer.name( "isPublic" ).value( ds.isPublic() );

			writer.endObject();
		}

		return sb.toString();
	}
}
