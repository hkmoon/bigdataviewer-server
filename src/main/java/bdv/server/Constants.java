package bdv.server;

public class Constants
{
	public static final String DATASETLIST_CONTEXT_NAME = "json";

	public static final String MANAGER_CONTEXT_NAME = "manager";

	public static final String DATASET_CONTEXT_NAME = "dataset";

	public static final String[] RESERVED_CONTEXT_NAMES = new String[]
			{
					DATASETLIST_CONTEXT_NAME,
					MANAGER_CONTEXT_NAME
			};

	public static final int THUMBNAIL_WIDTH = 100;

	public static final int THUMBNAIL_HEIGHT = 100;

	// PUBLIC context
	public static final String PUBLIC_DATASET_TAG_CONTEXT_NAME = "public/tag";

	public static final String PUBLIC_DATASET_CONTEXT_NAME = "public/dataset";

	// PRIVATE context
	public static final String PRIVATE_DOMAIN = "private";

	public static final String PRIVATE_DATASET_TAG_CONTEXT_NAME = PRIVATE_DOMAIN + "/tag";

	public static final String PRIVATE_DATASET_CONTEXT_NAME = PRIVATE_DOMAIN + "/dataset";

	public static final String PRIVATE_USER_CONTEXT_NAME = PRIVATE_DOMAIN + "/user";
}
