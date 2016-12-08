package bdv.util;

import sun.security.x509.AlgorithmId;
import sun.security.x509.CertificateAlgorithmId;
import sun.security.x509.CertificateIssuerName;
import sun.security.x509.CertificateSerialNumber;
import sun.security.x509.CertificateSubjectName;
import sun.security.x509.CertificateValidity;
import sun.security.x509.CertificateVersion;
import sun.security.x509.CertificateX509Key;
import sun.security.x509.X500Name;
import sun.security.x509.X509CertImpl;
import sun.security.x509.X509CertInfo;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.SignatureException;
import java.security.cert.CertificateException;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Author: HongKee Moon (moon@mpi-cbg.de), Scientific Computing Facility
 * Organization: MPI-CBG Dresden
 * Date: December 2016
 */
public class Keystore
{
	public static final Pattern JAVA_VERSION = Pattern.compile( "([0-9]*.[0-9]*)(.*)?" );

	/**
	 * Checks whether the current Java runtime has a version equal or higher then the given one. As Java version are
	 * not double (because they can use more digits such as 1.8.0), this method extracts the two first digits and
	 * transforms it as a double.
	 * @param version the version
	 * @return {@literal true} if the current Java runtime is at least the specified one,
	 * {@literal false} if not or if the current version cannot be retrieve or is the retrieved version cannot be
	 * parsed as a double.
	 */
	public static boolean isJavaAtLeast( double version )
	{
		String javaVersion = System.getProperty( "java.version" );
		if ( javaVersion == null )
		{
			return false;
		}

		// if the retrieved version is one three digits, remove the last one.
		Matcher matcher = JAVA_VERSION.matcher( javaVersion );
		if ( matcher.matches() )
		{
			javaVersion = matcher.group( 1 );
		}

		try
		{
			double v = Double.parseDouble( javaVersion );
			return v >= version;
		}
		catch ( NumberFormatException e )
		{
			return false;
		}
	}

	public static boolean checkKeystore()
	{
		// check if "etc/keystore.jks" exists
		if ( Files.exists( Paths.get( "etc/keystore.jks" ) ) )
			return true;
		else
		{
			try
			{
				// keytool -genkey -alias localhost -keyalg RSA -keystore keystore.jks -keysize 2048
				if ( Files.notExists( Paths.get( "etc/" ) ) )
					Files.createDirectory( Paths.get( "etc/" ) );

				KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance( "RSA" );
				keyPairGenerator.initialize( 2048 );
				KeyPair KPair = keyPairGenerator.generateKeyPair();
				PrivateKey privkey = KPair.getPrivate();

				X509CertInfo info = new X509CertInfo();
				Date from = new Date();
				Date to = new Date( from.getTime() + 365 * 86400000l );
				CertificateValidity interval = new CertificateValidity( from, to );
				BigInteger sn = new BigInteger( 64, new SecureRandom() );
				X500Name owner = new X500Name( "CN=Unknown, L=Unknown, ST=Unknown, O=Unknown, OU=Unknown, C=Unknown" );

				info.set( X509CertInfo.VALIDITY, interval );
				info.set( X509CertInfo.SERIAL_NUMBER, new CertificateSerialNumber( sn ) );
				boolean justName = isJavaAtLeast( 1.8 );
				if ( justName )
				{
					info.set( X509CertInfo.SUBJECT, owner );
					info.set( X509CertInfo.ISSUER, owner );
				}
				else
				{
					info.set( X509CertInfo.SUBJECT, new CertificateSubjectName( owner ) );
					info.set( X509CertInfo.ISSUER, new CertificateIssuerName( owner ) );
				}

				info.set( X509CertInfo.KEY, new CertificateX509Key( KPair.getPublic() ) );
				info.set( X509CertInfo.VERSION, new CertificateVersion( CertificateVersion.V3 ) );
				AlgorithmId algo = new AlgorithmId( AlgorithmId.sha256WithRSAEncryption_oid );
				info.set( X509CertInfo.ALGORITHM_ID, new CertificateAlgorithmId( algo ) );

				// Sign the cert to identify the algorithm that's used.
				X509CertImpl cert = new X509CertImpl( info );
				cert.sign( privkey, "SHA256withRSA" );

				// Update the algorithm, and resign.
				algo = ( AlgorithmId ) cert.get( X509CertImpl.SIG_ALG );
				info.set( CertificateAlgorithmId.NAME + "." + CertificateAlgorithmId.ALGORITHM, algo );
				cert = new X509CertImpl( info );
				cert.sign( privkey, "SHA256withRSA" );

				KeyStore keyStore = null;
				FileOutputStream keyStoreFile = null;

				// Load the default Java keystore
				keyStore = KeyStore.getInstance( KeyStore.getDefaultType() );
				keyStore.load( null, "changeit".toCharArray() );

				final char passwordArray[] = System.console().readPassword( "Enter your new keystore password for SSL connection: " );

				String password = new String( passwordArray );

				// Put our information
				keyStore.setCertificateEntry( "localhost", cert );
				keyStore.setKeyEntry( "localhost", privkey,
						password.toCharArray(),
						new java.security.cert.Certificate[] { cert } );

				// Generate new cert
				keyStoreFile = new FileOutputStream( "etc/keystore.jks" );
				keyStore.store( keyStoreFile, password.toCharArray() );
				keyStoreFile.close();
			}
			catch ( FileNotFoundException e )
			{
				e.printStackTrace();
				return false;
			}
			catch ( KeyStoreException e )
			{
				e.printStackTrace();
				return false;
			}
			catch ( IOException e )
			{
				e.printStackTrace();
				return false;
			}
			catch ( NoSuchAlgorithmException e )
			{
				e.printStackTrace();
				return false;
			}
			catch ( CertificateException e )
			{
				e.printStackTrace();
				return false;
			}
			catch ( SignatureException e )
			{
				e.printStackTrace();
				return false;
			}
			catch ( NoSuchProviderException e )
			{
				e.printStackTrace();
				return false;
			}
			catch ( InvalidKeyException e )
			{
				e.printStackTrace();
				return false;
			}
		}
		return true;
	}
}
