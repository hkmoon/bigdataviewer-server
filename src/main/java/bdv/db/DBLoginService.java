package bdv.db;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.eclipse.jetty.security.MappedLoginService;
import org.eclipse.jetty.security.PropertyUserStore.UserListener;
import org.eclipse.jetty.server.UserIdentity;
import org.eclipse.jetty.util.log.Log;
import org.eclipse.jetty.util.log.Logger;
import org.eclipse.jetty.util.security.Credential;

/**
 * Author: HongKee Moon (moon@mpi-cbg.de), Scientific Computing Facility
 * Organization: MPI-CBG Dresden
 * Date: December 2016
 */
public class DBLoginService extends MappedLoginService implements UserListener
{
	private static final Logger LOG = Log.getLogger( DBLoginService.class );

	private final Map< String, UserIdentity > _knownUserIdentities = new HashMap< String, UserIdentity >();

	public class HashKnownUser extends KnownUser
	{
		String[] _roles;

		public HashKnownUser( String name, Credential credential )
		{
			super( name, credential );
		}

		public void setRoles( String[] roles )
		{
			_roles = roles;
		}

		public String[] getRoles()
		{
			return _roles;
		}
	}

	/* ------------------------------------------------------------ */
	public DBLoginService()
	{
	}

	/* ------------------------------------------------------------ */
	public DBLoginService( String name )
	{
		setName( name );
	}

	/* ------------------------------------------------------------ */
	@Override
	protected UserIdentity loadUser( String username )
	{
		return null;
	}

	/* ------------------------------------------------------------ */
	@Override
	public void loadUsers() throws IOException
	{
		// TODO: Consider refactoring MappedLoginService to not have to override with unused methods
	}

	@Override
	protected String[] loadRoleInfo( KnownUser user )
	{
		if ( !_knownUserIdentities.containsKey( user.getName() ) )
		{
			DBConnection conn = new DBConnection();

			_knownUserIdentities.put( user.getName(), conn.getUserIdentity( user.getName() ) );
		}
		final UserIdentity id = _knownUserIdentities.get( user.getName() );

		if ( id == null )
			return null;

		Set< RolePrincipal > roles = id.getSubject().getPrincipals( RolePrincipal.class );
		if ( roles == null )
			return null;

		List< String > list = new ArrayList<>();
		for ( RolePrincipal r : roles )
			list.add( r.getName() );

		return list.toArray( new String[ roles.size() ] );
	}

	@Override
	protected KnownUser loadUserInfo( String userName )
	{
		if ( !_knownUserIdentities.containsKey( userName ) )
		{
			DBConnection conn = new DBConnection();

			_knownUserIdentities.put( userName, conn.getUserIdentity( userName ) );
		}
		final UserIdentity id = _knownUserIdentities.get( userName );

		if ( id != null )
		{
			return ( KnownUser ) id.getUserPrincipal();
		}

		return null;
	}

    /* ------------------------------------------------------------ */

	/**
	 * @see org.eclipse.jetty.util.component.AbstractLifeCycle#doStart()
	 */
	@Override
	protected void doStart() throws Exception
	{
		super.doStart();
	}

    /* ------------------------------------------------------------ */

	/**
	 * @see org.eclipse.jetty.util.component.AbstractLifeCycle#doStop()
	 */
	@Override
	protected void doStop() throws Exception
	{
		super.doStop();
	}

	/* ------------------------------------------------------------ */
	@Override
	public void update( String userName, Credential credential, String[] roleArray )
	{
		if ( LOG.isDebugEnabled() )
			LOG.debug( "update: " + userName + " Roles: " + roleArray.length );
		//TODO need to remove and replace the authenticated user?
	}

	/* ------------------------------------------------------------ */
	@Override
	public void remove( String userName )
	{
		if ( LOG.isDebugEnabled() )
			LOG.debug( "remove: " + userName );
		removeUser( userName );
	}
}
