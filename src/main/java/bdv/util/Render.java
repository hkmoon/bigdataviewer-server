package bdv.util;

import bdv.model.DataSet;
import org.stringtemplate.v4.ST;

/**
 * Author: HongKee Moon (moon@mpi-cbg.de), Scientific Computing Facility
 * Organization: MPI-CBG Dresden
 * Date: January 2017
 */
public class Render
{
	public static String createTagsLabel( final DataSet ds )
	{
		String ret = "";

		if ( ds.getTags().size() > 0 )
		{
			ST s = new ST( "<tags:{ tag | \\<span class=\"label label-info\"> <tag> \\</span>&nbsp; }>" );
			s.add( "tags", ds.getTags() );

			ret = s.render();
		}

		return ret;
	}
}
