package bdv.server;

import bdv.model.DataSet;
import bdv.util.Render;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.util.log.Log;
import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STGroup;
import org.stringtemplate.v4.STRawGroupDir;

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
 * Provides the default index page of available datasets on this {@link BigDataServer}
 * @author HongKee Moon <moon@mpi-cbg.de>
 */
public class IndexPageHandler extends ContextHandler
{
	private final Server server;

	public IndexPageHandler( final Server server ) throws IOException, URISyntaxException
	{
		this.server = server;
		setContextPath( "/" );
	}

	@Override
	public void doHandle( final String target, final Request baseRequest, final HttpServletRequest request, final HttpServletResponse response ) throws IOException, ServletException
	{
		if ( target.equals( "/" ) )
			list( baseRequest, response );
		else
			super.doHandle( target, baseRequest, request, response );
	}

	private void list( final Request baseRequest, final HttpServletResponse response ) throws IOException
	{
		response.setContentType( "text/html" );
		response.setStatus( HttpServletResponse.SC_OK );
		baseRequest.setHandled( true );

		final PrintWriter ow = response.getWriter();
		getHtmlDatasetList( ow );
		ow.close();
	}

	private void getHtmlDatasetList( final PrintWriter out ) throws IOException
	{
		final ArrayList< DataSet > list = new ArrayList<>();

		for ( final Handler handler : server.getChildHandlersByClass( PublicCellHandler.class ) )
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

		// Sort the list by Category and Index
		//		Collections.sort( list, new Comparator< DataSet >()
		//		{
		//			@Override
		//			public int compare( final DataSet lhs, DataSet rhs )
		//			{
		//				// return 1 if rhs should be before lhs
		//				// return -1 if lhs should be before rhs
		//				// return 0 otherwise
		//				if ( lhs.getCategory().equals( rhs.getCategory() ) )
		//				{
		//					return (int) (lhs.getIndex() - rhs.getIndex());
		//				}
		//				else
		//				{
		//					return lhs.getCategory().compareToIgnoreCase( rhs.getCategory() );
		//				}
		//			}
		//		} );

		final STGroup g = new STRawGroupDir( "templates", '$', '$' );

		final ST indexPage = g.getInstanceOf( "indexPage" );

		// Build html table for dataset list
		final StringBuilder sb = new StringBuilder();

		for ( DataSet ds : list )
		{
			final ST dataSetTr = g.getInstanceOf( "publicDataSetTr" );

			dataSetTr.add( "thumbnailUrl", ds.getThumbnailUrl() );
			dataSetTr.add( "dataSetTags", Render.createTagsLabel( ds ) );
			dataSetTr.add( "dataSetName", ds.getName() );
			dataSetTr.add( "dataSetDescription", ds.getDescription() );

			String url = ds.getDatasetUrl();
			if ( url.endsWith( "/" ) )
				url = url.substring( 0, url.lastIndexOf( "/" ) );

			dataSetTr.add( "dataSetUrl", url );

			sb.append( dataSetTr.render() );
		}

		indexPage.add( "dataSetTable", sb.toString() );

		out.write( indexPage.render() );
		out.close();
	}
}