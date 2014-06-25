package tool;

public class Messager {

	public static String CONNECT_FAIL = "500: connect failure";
	public static String INIT_FAIL = "500: initialization failure";
	public static String INSERTION_FAIL = "500: insersion failure";
	public static String SEARCH_FAIL = "400: search failure";
	public static String SET_LOCATOR_FAIL = "500: set server locators failure";
	public static String CHANGE_INDEX_FAIL = "500: change index file failure";
	public static String CLOSE_INDEX_FAIL = "500: close index writer failure";
	public static String SEARCH_DONE = "200: searching done";
	public static String START_SERVICE_FAIL = "500: start service failure";
	public static String UNKNOWN_ERROR = "500: unknown error";
	public static String BAD_REQUEST = "400: bad request";
	
	public static String setIndex(String id) {
		return "[id="+id.trim()+"]";
	}
}
