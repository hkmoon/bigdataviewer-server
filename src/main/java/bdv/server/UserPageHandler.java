package bdv.server;

import bdv.db.UserController;
import bdv.model.DataSet;
import bdv.util.Render;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
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
import java.util.List;
import java.util.stream.Collectors;

/**
 * Author: HongKee Moon (moon@mpi-cbg.de), Scientific Computing Facility
 * Organization: MPI-CBG Dresden
 * Date: December 2016
 */
public class UserPageHandler extends BaseContextHandler
{
	private static final org.eclipse.jetty.util.log.Logger LOG = Log.getLogger( UserPageHandler.class );

	UserPageHandler( final Server server,
			final ContextHandlerCollection publicDatasetHandlers, final ContextHandlerCollection privateDatasetHandlers,
			final String thumbnailsDirectoryName ) throws IOException, URISyntaxException
	{
		super( server, publicDatasetHandlers, privateDatasetHandlers, thumbnailsDirectoryName );

		setContextPath( "/private/user/*" );
	}

	@Override
	public void doHandle( final String target, final Request baseRequest, final HttpServletRequest request, final HttpServletResponse response ) throws IOException, ServletException
	{
		//		System.out.println(target);

		Principal principal = request.getUserPrincipal();
		//		System.out.println( principal.getName() );

		if ( null == request.getQueryString() )
		{
			list( baseRequest, response, principal.getName() );
		}
		else
		{
			//			System.out.println(request.getQueryString());
			updateDataSet( baseRequest, request, response, principal.getName() );
		}
	}

	private void updateDataSet( Request baseRequest, HttpServletRequest request, HttpServletResponse response, String userId ) throws IOException
	{
		final String op = request.getParameter( "op" );

		if ( null != op )
		{
			if ( op.equals( "addDS" ) )
			{
				final String dataSetName = request.getParameter( "name" );
				final String tags = request.getParameter( "tags" );
				final String description = request.getParameter( "description" );
				final String file = request.getParameter( "file" );
				final boolean isPublic = Boolean.parseBoolean( request.getParameter( "public" ) );

				//				System.out.println( "name: " + dataSetName );
				//				System.out.println( "tags: " + tags );
				//				System.out.println( "description: " + description );
				//				System.out.println( "file: " + file );
				//				System.out.println( "isPublic: " + isPublic );

				final DataSet ds = new DataSet( dataSetName, file, tags, description, isPublic );

				// Add it the database
				UserController.addDataSet( userId, ds );

				// Add new CellHandler depending on public property
				addDataSet( ds, baseRequest, response );
			}
			else if ( op.equals( "removeDS" ) )
			{
				final long dsId = Long.parseLong( request.getParameter( "dataset" ) );

				// Remove it from the database
				UserController.removeDataSet( userId, dsId );

				// Remove the CellHandler
				removeDataSet( dsId, baseRequest, response );
			}
			else if ( op.equals( "updateDS" ) )
			{
				// UpdateDS uses x-editable
				// Please, refer http://vitalets.github.io/x-editable/docs.html
				final String field = request.getParameter( "name" );
				final long dataSetId = Long.parseLong( request.getParameter( "pk" ) );
				final String value = request.getParameter( "value" );

				// Update the database
				final DataSet ds = UserController.getDataSet( userId, dataSetId );

				if ( field.equals( "dataSetName" ) )
				{
					ds.setName( value );
				}
				else if ( field.equals( "dataSetDescription" ) )
				{
					ds.setDescription( value );
				}

				// Update DataBase
				UserController.updateDataSet( userId, ds );

				// Update the CellHandler
				updateHandler( ds, baseRequest, response );

				//				System.out.println( field + ":" + value );
			}
			else if ( op.equals( "addTag" ) || op.equals( "removeTag" ) )
			{
				processTag( op, baseRequest, request, response );
			}
			else if ( op.equals( "setPublic" ) )
			{
				final long dataSetId = Long.parseLong( request.getParameter( "dataset" ) );
				final boolean checked = Boolean.parseBoolean( request.getParameter( "checked" ) );

				final DataSet ds = UserController.getDataSet( userId, dataSetId );

				ds.setPublic( checked );

				UserController.updateDataSet( userId, ds );

				ds.setPublic( !checked );

				updateHandlerVisibility( ds, checked, baseRequest, response );
			}
			else if ( op.equals( "addSharedUser" ) || op.equals( "removeSharedUser" ) )
			{
				final long dataSetId = Long.parseLong( request.getParameter( "dataset" ) );
				final String sharedUserId = request.getParameter( "userId" );

				if ( op.equals( "addSharedUser" ) )
				{
					//					System.out.println("Add a shared user");
					UserController.addReadableSharedUser( userId, dataSetId, sharedUserId );
				}
				else if ( op.equals( "removeSharedUser" ) )
				{
					//					System.out.println("Remove the shared user");
					UserController.removeReadableSharedUser( userId, dataSetId, sharedUserId );
				}

				response.setContentType( "text/html" );
				response.setStatus( HttpServletResponse.SC_OK );
				baseRequest.setHandled( true );

				final PrintWriter ow = response.getWriter();
				ow.write( "Success: " );
				ow.close();
			}
		}
	}

	private void updateHandlerVisibility( DataSet ds, boolean newVisibility, Request baseRequest, HttpServletResponse response ) throws IOException
	{
		String dsName = ds.getName();
		boolean ret = removeCellHandler( ds.getIndex() );

		if ( ret )
		{
			ds.setPublic( newVisibility );

			final boolean isPublic = newVisibility;

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
				ret = false;
			}
		}

		response.setContentType( "text/html" );
		response.setStatus( HttpServletResponse.SC_OK );
		baseRequest.setHandled( true );

		final PrintWriter ow = response.getWriter();

		if ( ret )
		{
			ow.write( "Success: " + dsName + " is removed." );
		}
		else
		{
			ow.write( "Error: " + dsName + " cannot be removed." );
		}
		ow.close();
	}

	private void updateHandler( DataSet ds, Request baseRequest, HttpServletResponse response ) throws IOException
	{
		boolean ret = false;
		String dsName = "";

		for ( final Handler handler : server.getChildHandlersByClass( CellHandler.class ) )
		{
			final CellHandler contextHandler = ( CellHandler ) handler;
			if ( contextHandler.getDataSet().getIndex() == ds.getIndex() )
			{
				final DataSet dataSet = contextHandler.getDataSet();

				dataSet.setName( ds.getName() );
				dataSet.setDescription( ds.getDescription() );

				ret = true;
				dsName = ds.getName();

				break;
			}
		}

		response.setContentType( "text/html" );
		response.setStatus( HttpServletResponse.SC_OK );
		baseRequest.setHandled( true );

		final PrintWriter ow = response.getWriter();

		if ( ret )
		{
			ow.write( "Success: " + dsName + " is updated." );
		}
		else
		{
			ow.write( "Error: " + dsName + " cannot be updated." );
		}
		ow.close();
	}

	private void removeDataSet( long dsId, Request baseRequest, HttpServletResponse response ) throws IOException
	{
		boolean ret = removeCellHandler( dsId );

		response.setContentType( "text/html" );
		response.setStatus( HttpServletResponse.SC_OK );
		baseRequest.setHandled( true );

		final PrintWriter ow = response.getWriter();

		if ( ret )
		{
			ow.write( "Success: " + dsId + " is removed." );
		}
		else
		{
			ow.write( "Error: " + dsId + " cannot be removed." );
		}
		ow.close();
	}

	private void addDataSet( DataSet ds, Request baseRequest, HttpServletResponse response ) throws IOException
	{
		boolean ret = false;

		if ( !ds.getXmlPath().isEmpty() && !ds.getName().isEmpty() )
		{
			final boolean isPublic = ds.isPublic();

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
			ow.write( "Success: " + ds.getName() + " is added." );
		else
			ow.write( "Error: " + ds.getName() + " cannot be added." );

		ow.close();
	}

	private void list( final Request baseRequest, final HttpServletResponse response, final String userId ) throws IOException
	{
		response.setContentType( "text/html" );
		response.setStatus( HttpServletResponse.SC_OK );
		baseRequest.setHandled( true );

		final PrintWriter ow = response.getWriter();
		getHtmlDatasetList( ow, userId );
		ow.close();
	}

	private void getHtmlDatasetList( final PrintWriter out, final String userId ) throws IOException
	{
		final List< DataSet >[] dataSets = UserController.getDataSets( userId );
		final List< DataSet > myDataSetList = dataSets[ 0 ];
		final List< DataSet > sharedDataSetList = dataSets[ 1 ];

		final STGroup g = new STRawGroupDir( "templates", '$', '$' );

		final ST userPage = g.getInstanceOf( "userPage" );

		final STGroup g2 = new STRawGroupDir( "templates", '~', '~' );
		final ST userPageJS = g2.getInstanceOf( "userPageJS" );

		final StringBuilder dsString = new StringBuilder();

		for ( DataSet ds : myDataSetList )
		{
			StringBuilder sharedUserString = new StringBuilder();

			final ST dataSetTr = g.getInstanceOf( "privateDataSetTr" );

			for ( String sharedUser : ds.getSharedUsers() )
			{
				final ST userToBeRemoved = g.getInstanceOf( "userToBeRemoved" );

				userToBeRemoved.add( "dataSetId", ds.getIndex() );
				userToBeRemoved.add( "sharedUserId", sharedUser );

				sharedUserString.append( userToBeRemoved.render() );
			}

			if ( ds.getDatasetUrl() == null )
			{
				// Setup the dataset url
				if ( ds.isPublic() )
				{
					ds.setDatasetUrl( "/" + Constants.PUBLIC_DATASET_CONTEXT_NAME + "/id/" + ds.getIndex() + "/" );
				}
				else
				{
					ds.setDatasetUrl( "/" + Constants.PRIVATE_DATASET_CONTEXT_NAME + "/id/" + ds.getIndex() + "/" );
				}
			}

			dataSetTr.add( "thumbnailUrl", ds.getThumbnailUrl() );
			dataSetTr.add( "dataSetId", ds.getIndex() );
			dataSetTr.add( "dataSetTags", ds.getTags().stream().collect( Collectors.joining( "," ) ) );
			dataSetTr.add( "dataSetName", ds.getName() );
			dataSetTr.add( "dataSetDescription", ds.getDescription() );
			dataSetTr.add( "dataSetLocation", ds.getXmlPath() );

			String url = ds.getDatasetUrl();
			if ( url.endsWith( "/" ) )
				url = url.substring( 0, url.lastIndexOf( "/" ) );

			dataSetTr.add( "dataSetUrl", url );
			dataSetTr.add( "dataSetIsPublic", ds.isPublic() );
			dataSetTr.add( "sharedUsers", sharedUserString.toString() );

			dsString.append( dataSetTr.render() );
		}

		if ( sharedDataSetList != null )
		{
			for ( DataSet ds : sharedDataSetList )
			{
				final ST dataSetTr = g.getInstanceOf( "sharedDataSetTr" );

				if ( ds.getDatasetUrl() == null )
				{
					// Setup the dataset url
					if ( ds.isPublic() )
					{
						ds.setDatasetUrl( "/" + Constants.PUBLIC_DATASET_CONTEXT_NAME + "/id/" + ds.getIndex() + "/" );
					}
					else
					{
						ds.setDatasetUrl( "/" + Constants.PRIVATE_DATASET_CONTEXT_NAME + "/id/" + ds.getIndex() + "/" );
					}
				}

				dataSetTr.add( "thumbnailUrl", ds.getThumbnailUrl() );
				dataSetTr.add( "dataSetId", ds.getIndex() );

				dataSetTr.add( "dataSetTags", Render.createTagsLabel( ds ) );
				dataSetTr.add( "dataSetName", ds.getName() );
				dataSetTr.add( "dataSetDescription", ds.getDescription() );
				dataSetTr.add( "dataSetLocation", ds.getXmlPath() );

				String url = ds.getDatasetUrl();
				if ( url.endsWith( "/" ) )
					url = url.substring( 0, url.lastIndexOf( "/" ) );

				dataSetTr.add( "dataSetUrl", url );
				dataSetTr.add( "dataSetIsPublic", ds.isPublic() );
				dataSetTr.add( "sharedUsers", ds.getOwner() );

				dsString.append( dataSetTr.render() );
			}
		}

		userPage.add( "userId", userId );
		userPage.add( "dataSetTr", dsString.toString() );

		userPage.add( "JS", userPageJS.render() );

		out.write( userPage.render() );
		out.close();
	}
}
