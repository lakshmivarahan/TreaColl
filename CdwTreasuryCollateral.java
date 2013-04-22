package com.ntrs.cdwfeed;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.log4j.Logger;

import com.ntrs.dis.util.DISProperties;

/* This class contains the logic to 
 * generate the monthly csv file that will 
 * be used by CDW
 */

/**
 * @author gh69
 * 
 */
public class CdwTreasuryCollateral {

  // Properties file to get the static data
	private DISProperties brokerStatementProperty;
	private int month;
	private int year;
	private int date;
	
	private Date startDate;
	private Date endDate;
	private HashMap queryMap = new HashMap();
	private String accountQueryKey = "accountQueryKey";
	private String tradeQueryKey = "tradeQueryKey";
	private static Logger logWriter = Logger.getLogger(CdwTreasuryCollateral.class);
	private String executionDateString;

/**
 * Constructor - Loads the properties file and sets the Month and Year
 */
public CdwTreasuryCollateral() {
	
	Logger.getLogger("Inside the constructor");
	// Set the Month and Year and build the Start and End Date parameters for the query
	this.setMonthAndYear();
	this.buildStartDateAndEndDateParameters();
	
	//Get the queries from the properties file and set them in a map
	setPropertyAndQueryMap();
	Logger.getLogger("The queryHashMap has been set and exitting the constructor");
     }


/*+++++++++++++++++++++++Getters and Setter Methods- Start+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++*/
/**
 * @return the date
 */
public int getDate() {
		return date;
	}

/**
 * @param date
 */
private void setDate(int date) {
		this.date = date;

	}

/**
 * @return the accountQueryKey
 */
public String getAccountQueryKey() {
		return accountQueryKey;
	}

/**
 * @param accountQueryKey
 */
public void setAccountQueryKey(String accountQueryKey) {
	this.accountQueryKey = accountQueryKey;
}

/**
 * @return the tradeQueryKey
 */
public String getTradeQueryKey() {
		return tradeQueryKey;
	}

/**
 * @param tradeQueryKey
 */
public void setTradeQueryKey(String tradeQueryKey) {
		this.tradeQueryKey = tradeQueryKey;
	}

/**
* Get the brokerStatementProperty
* @return the brokerStatementProperty
*/
public DISProperties getBrokerStatementProperty() {
		return brokerStatementProperty;
	}

/**
 * @param brokerStatementProperty
 */
public void setBrokerStatementProperty(DISProperties brokerStatementProperty) {
	this.brokerStatementProperty = brokerStatementProperty;
}

/**
 * @return the month
 */
public int getMonth() {
	return month;
}

/**
 * @param month
 */
public void setMonth(int month) {
	this.month = month;
}

/**
 * @return the year
 */
public int getYear() {
	return year;
}

/**
 * @param year
 */
public void setYear(int year) {
		this.year = year;
	}

/**
 * @return the startDate
 */
public Date getStartDate() {
	return startDate;
}

/**
 * @param startDate
 */
public void setStartDate(Date startDate) {
	this.startDate = startDate;
}

/**
 * @return the endDate
 */
public Date getEndDate() {
	return endDate;
}

/**
 * @param endDate
 */
public void setEndDate(Date endDate) {
	this.endDate = endDate;
}

/**
 * @return the queryMap
 */
public HashMap getQueryMap() {
	return queryMap;
}

/**
 * The queryMap contains the 2 queries that will be executed
 * @param queryMap
 *            
 */
public void setQueryMap(HashMap queryMap) {
	this.queryMap = queryMap;
}

/**
 * @return the executionDateString
 */
public String getExecutionDateString() {
	return executionDateString;
}

/**
 * @param executionDateString
 */
public void setExecutionDateString(String executionDateString) {
	this.executionDateString = executionDateString;
}

/* ++++++++++++++++++++++++Getters and Setter Methods-- End+++++++++++++++++++*/
	 

/*----------------Methods to operate on the the Dates- Starts------------------------------------------------------------------------------------*/


/**
 * Set the month and the Year 
 */
public void setMonthAndYear() {
	Calendar batchExecutionTime = Calendar.getInstance();
	setMonth((batchExecutionTime.get(Calendar.MONTH)) + 1);
	setYear(batchExecutionTime.get(Calendar.YEAR));
	setDate(batchExecutionTime.get(Calendar.DAY_OF_MONTH));
	Logger.getLogger("Executed Method setMonthAndYear()");
}

/**
 * Set the Start, End Date values that will be used in the SQL query
 * @return void
 */

public void buildStartDateAndEndDateParameters() {
	
	String currentMonthStartDate = buildDateString(getYear(), getMonth());
	String executionDateString = buildExecutionDateString(getYear(),getMonth(), getDate());
    String nextMonthStartDate = buildNextMonthDateString();
	SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
	
	try {
		// Set the Start & Date that will be used in the query and the execution time of the batch job
		setStartDate(new java.sql.Date(dateFormat.parse(currentMonthStartDate).getTime()));
		setEndDate(new java.sql.Date(dateFormat.parse(nextMonthStartDate).getTime()));
		setExecutionDateString(executionDateString);
		Logger.getLogger("Completed Method buildStartDateAndEndDateParameters()");
		} catch (ParseException e) {
			logWriter.error("Parse Exception while operating on Dates in buildStartDateAndEndDateParameters method");
	}
}

/**
 * Build the Date String
 * @return String
 */
protected String buildDateString(int year, int month) {
	return year + "-" + (month) + "-01";
}

/**
 * Build the Execution Date String to capture when the Job has been executed.This string format will be used in the csv file
 * @param year,month and date
 */
protected String buildExecutionDateString(int year, int month, int date) {

		return year + "-" + (month) + "-" + (date);
}

/**
 * Build the next month start Date String 
 * @return TreasuryCollateral#buildDateString(int, int)
 */
public String buildNextMonthDateString() {
	int month = getMonth();
	int year = getYear();
	if (month == 12) {
		month = 1;
		++year;
	} else {
		month++;
	}
	return buildDateString(year, month);
}

/*-----------------Methods to operate on the the Dates-Ends--------------------------------------------------------------------- */
/*-----------------Methods to get the DB connection-Start--------------------------------------------------------------------- */

/**
 * Description:Method to get the DB connection
 * @return Connection
 */
	private Connection getConnection() {
		Connection connection = null;
		DISProperties brokerStatementProperty = getBrokerStatementProperty();
		String dbServer = brokerStatementProperty.getProperty("db_server_cdw");
		String dbUser = brokerStatementProperty.getProperty("db_user_cdw");
		String dbPassword = brokerStatementProperty
				.getProperty("db_password_cdw");
		try {
			connection = connect(dbServer, dbUser, dbPassword);
			Logger.getLogger("Successfully Got the DB Connection");
		} catch (Exception e) {
			logWriter.error("There is a problem in getting the Database Connection in method getConnection()");
			e.printStackTrace();
		}
		return connection;
	}

/**
 * Description:Method connect to the DB 
 * @return Connection
 */
public Connection connect(String serverName, String userName,
		String password) throws Exception {
	Connection connection = null;

	try {
		Class.forName("oracle.jdbc.driver.OracleDriver");
		connection = DriverManager.getConnection("jdbc:oracle:thin:@"+ serverName + ":1521:" + serverName, userName, password);
		connection.setAutoCommit(false);
	} catch (SQLException e) {
		logWriter.error("Exception thrown in the constructor");
		e.printStackTrace();
		throw new Exception("Unable to establish connection ..."+ e.toString());
	} catch (Exception e) {
		throw new Exception("Unable to establish connection ..."+ e.toString());
	}

	return connection;
}

/*-----------------Methods to get the DB connection-End--------------------------------------------------------------------- */

/**
 * Description: Get the SQL queries that need to be executed and store them
 * in a HashMap 
 */
private void setPropertyAndQueryMap() {

	try {
		// Get the properties file and load its data when constructing the object
	DISProperties brokerStatementProperty;
	brokerStatementProperty = DISProperties.getInstance("brokerstatement.properties");
	setBrokerStatementProperty(brokerStatementProperty);
    
	//The queries are stored in the HashMap
	Logger.getLogger("Setting the queries in the HashMap");
	queryMap.put(this.accountQueryKey, (String) brokerStatementProperty.getProperty("query_account_margin_excess_deficit"));
	queryMap.put(this.tradeQueryKey, (String) brokerStatementProperty.getProperty("query_trade_table"));
	Logger.getLogger("Executed Method setPropertyAndQueryMap()");
} catch (Exception e) {
	logWriter.error("Exception thrown in the constructor");
		e.printStackTrace();
	}
	setBrokerStatementProperty(brokerStatementProperty);
	setQueryMap(queryMap);
 }

/**
 * Description:This method initiates the process to execute the query and
 * then calls the appropriate methods to write to a csv
 * @param queryList
 * @return arraylist of QueryList
 */
public void executeQuery() {

	Logger.getLogger("In the executeQuery method");

	// ResultSet for the 2 queries rows from Account Table and the trade table
	DISProperties brokerStatementProperties = getBrokerStatementProperty();
	ResultSet accountTableResultSet = null,tradeTableResultSet = null;
	ArrayList cdwTradeQueryRows = null,cdwDataWithInitialMargin = null,cdwCsvData = null;
	HashSet accountIdAsOfDateSet = null,tradeIdSet = null;
	HashMap queryResultSetMap = new HashMap();
	String accountTableResultSetKey = "accountTableResultSetKey";
	String tradeTableResultSetKey = "tradeTableResultSetKey";
	int tradeRowcount = 0,accountRowCount = 0;
	
	// Query prepared statement
	PreparedStatement prepareAccountTableQuery = null,prepareTradeTableQuery = null;;
	
    //Getting the Database connection
	Connection dbConnection = getConnection();

     try {
		// Get the Queries from the Hashmap
		prepareAccountTableQuery = dbConnection.prepareStatement((String) getQueryMap().get(getAccountQueryKey()));
		prepareTradeTableQuery = dbConnection.prepareStatement((String) getQueryMap().get(getTradeQueryKey()));
        
		//Set the parameter wildcards in an array
		String[] accountTableQueryParam = {brokerStatementProperties.getProperty("TREASURY_CLIENT"),getStartDate().toString(),getEndDate().toString()};
		String[] tradeTableQueryParam = {brokerStatementProperties.getProperty("BROKER_CODE"), brokerStatementProperties.getProperty("TREASURY_CLIENT"),getStartDate().toString(),getEndDate().toString(),brokerStatementProperties.getProperty("BROKER_CODE")};
		
		//Set the wildcards in the query before execution
		prepareAccountTableQuery=setPreparedStatementQuery(accountTableQueryParam,prepareAccountTableQuery);
		prepareTradeTableQuery=setPreparedStatementQuery(tradeTableQueryParam,prepareTradeTableQuery);
		

		// Execute the 2 queries and assign to the correct ResultSet
		accountTableResultSet = prepareAccountTableQuery.executeQuery();
		tradeTableResultSet = prepareTradeTableQuery.executeQuery();
		
		Logger.getLogger("Executed the Query without any exceptions");

		// Store the query results in a HashMap
		queryResultSetMap.put(accountTableResultSetKey,accountTableResultSet);
		queryResultSetMap.put(tradeTableResultSetKey, tradeTableResultSet);

		// Process the ResultSet
		cdwTradeQueryRows = processResultSet(queryResultSetMap,accountTableResultSetKey, tradeTableResultSetKey);

		// Get the unique ClientId-AsofDate combination and TradeIds and store them in a HashSet
		ArrayList uniqueValueList = getUniqueClientAccountDateTradeIds(cdwTradeQueryRows);
		accountIdAsOfDateSet = (HashSet) uniqueValueList.get(0);
		tradeIdSet = (HashSet) uniqueValueList.get(1);

		// Using the accountId--AsOfDate calculate the Total Notional and store it in a Map with key accountID$AsOfDate - Key and Value as TotalNotional
		HashMap accountIdTotalNotional = processAccountTradeQueryResult(accountIdAsOfDateSet, cdwTradeQueryRows);
		cdwDataWithInitialMargin = setInitialMargin(accountIdTotalNotional,cdwTradeQueryRows);

		// Use the unique trade ids to calculate Sum the margin data and send the ArrayList to a csv writer
		cdwCsvData = cdwCsvData(tradeIdSet, cdwDataWithInitialMargin);

		// Write the Data to a CSV file
		writeToCsv(cdwCsvData);
		
     } catch (SQLException ex) {
		logWriter.error("Exception In Query Execution-SQL Exception thrown");
		ex.printStackTrace();
	} catch (Exception ex) {
		logWriter.error("Exception In Query Execution- NON-SQL exception thrown");
		ex.printStackTrace();

	} finally {
		try {
			if (prepareAccountTableQuery != null)
				prepareAccountTableQuery.close();
			if (prepareTradeTableQuery != null)
				prepareAccountTableQuery.close();

			if (accountTableResultSet != null)
				accountTableResultSet.close();
			if (tradeTableResultSet != null)
				tradeTableResultSet.close();

			if (dbConnection != null)
				dbConnection.close();

		} catch (SQLException ex) {
			logWriter.error("Exception while trying to close");
		}
	}
	Logger.getLogger("Wrote to a CSV file without any errors");
	System.out.println("--------COMPLETED-------------");
}

/**Description:Sets the Paramater strings for the preparedStatement
 * @param tradeTableQueryParam
 * @param prepareTableQuery
 * @return
 */
private PreparedStatement setPreparedStatementQuery(String[] tradeTableQueryParam,
		PreparedStatement prepareTableQuery) {
int j = 1;
for (int i=0;i<tradeTableQueryParam.length;i++){
	try {
		prepareTableQuery.setString(j,tradeTableQueryParam[i]);
		j++;
	} catch (SQLException e) {
		
		Logger.getLogger("Error in method setPreparedStatementQuery()");
		e.printStackTrace();
	}
}
	return prepareTableQuery;
}


/**
 * @Description: Description:This method processes the ResultSet from both the Queries
 * @param queryResultSetMap
 * @param accountTableResultSetKey
 * @param tradeTableResultSetKey
 * @return cdwRows
 */
private ArrayList processResultSet(HashMap queryResultSetMap,
		String accountTableResultSetKey, String tradeTableResultSetKey) {

	ResultSet accountResultSet = (ResultSet) queryResultSetMap
			.get(accountTableResultSetKey);
	ResultSet tradeResultSet = (ResultSet) queryResultSetMap
			.get(tradeTableResultSetKey);

	HashMap accountIdInitMargin = new HashMap();
	accountIdInitMargin = createAccountIdInitMarginMap(accountResultSet);
	ArrayList cdwRows = processTradeResultSet(tradeResultSet,accountIdInitMargin);
	Logger.getLogger("Executed method processResultSet()");
	return cdwRows;
}

/**
 * Description: Returns a HashMap containing the unique ClientId as the Key
 * and value with a HashMap (Date,AccountInitMarginExcessDeficit)
 * @param accountResultSet
 * @return clientAccountMap
 */
private HashMap createAccountIdInitMarginMap(ResultSet accountResultSet) {
	int accountResultSetLength = 0;
	String account_id = "";
	// Contains the Client Account ID as a Key and Value of HashMap containing Date (K) ; Init_Margin (Value)
	HashMap clientAccountMap = null;

	// This HashMap is stored as a Value in the clientAccountMap for each Client Account ID key
	HashMap dateInitMarginMap = null;

	try {
		clientAccountMap = new HashMap();
		while (accountResultSet.next()) {
			
			accountResultSetLength++;
            // Get the client account id,as_of_date and accountInitMargin from the Result Set
			String client_account_id = accountResultSet.getString("ACCOUNT_ID");
			Date asOfDate = accountResultSet.getDate("AS_OF_DATE");
			
			Float accountInitMargin = new Float(accountResultSet.getFloat("ACCOUNT_INIT_MARGIN"));
			Logger.getLogger("The Clients are \n" + account_id);
			
			if (client_account_id.equals(account_id)) {
				// Get the existing map for the client_account_id
				HashMap existingMap = (HashMap) clientAccountMap.get(client_account_id);

				// Put the values for the date,margin into the existing map
				existingMap.put(asOfDate,accountInitMargin);
                //Update the Map 				
				clientAccountMap.remove(client_account_id);
				clientAccountMap.put(client_account_id, existingMap);

			} else {
				// If we have a new Client ID add a new entry into the clientAccountMap
				dateInitMarginMap = new HashMap();
				dateInitMarginMap.put(asOfDate, accountInitMargin);
				clientAccountMap.put(client_account_id, dateInitMarginMap);
				
				// Set the account_id as the client_account_id from the ResultSet
				account_id = client_account_id;
			}

		}// End of the Iterator
	} catch (SQLException e) {
		logWriter.error("Error when Iterating through the accountTable result set in method createAccountIdInitMarginSet()");
		e.printStackTrace();
		}
	Logger.getLogger("The lenght of the client_account_id hash map is\n"+ clientAccountMap.size());
	Logger.getLogger("Executed Method createAccountIdInitMarginMap()");
	return clientAccountMap;
}

/**
 *Description:Iterate through the tradeTable results and store their values in the
 * CdwMarginData class object
 * @param tradeResultSet
 * @param accountIdInitMargin
 * @return
 */
private ArrayList processTradeResultSet(ResultSet tradeResultSet,
		HashMap accountIdInitMargin) {

	CdwMarginData cdwMarginData;
	ArrayList tradeIdList = new ArrayList();
	try {
		while (tradeResultSet.next()) {
			cdwMarginData = new CdwMarginData(getBrokerStatementProperty());
			// Set the values in the CdwMarginData object and add them to the ArrayList
			tradeIdList.add(storeTradeTableResults(tradeResultSet,cdwMarginData, accountIdInitMargin));
		}
	} catch (SQLException e) {
		logWriter.error("Error when processing the Trade table result set");
		e.printStackTrace();
	}
	Logger.getLogger("Executed Method processTradeResultSet()");
	return tradeIdList;
}

/**
 * Description: Get the data from the Trade Table results and store them in
 * CdwMarginData
 * @param tradeResultSet
 * @param cdwMarginData
 * @param accountIdInitMargin
 * @return
 */
private CdwMarginData storeTradeTableResults(ResultSet tradeResultSet,CdwMarginData cdwMarginData, HashMap accountIdInitMargin) {

	
	DecimalFormat df = new DecimalFormat("#.##");
	try {
        //Set the DB values into the cdwMarginData objects
		cdwMarginData.setAccount_id((String) (tradeResultSet.getString("ACCOUNT_ID")));
		cdwMarginData.setAsOfDate((Date) (tradeResultSet.getDate("AS_OF_DATE")));
		cdwMarginData.setTradeId((String) (tradeResultSet.getString("TRADE_ID")));
		cdwMarginData.setNotional((float) (tradeResultSet.getFloat("NOTIONAL")));
		cdwMarginData.setBrokerBicCode((String) tradeResultSet.getString("BROKER_BIC_CODE"));
        cdwMarginData.setVariationMargin((float) (tradeResultSet.getFloat("VARIATION_MARGIN")));
        cdwMarginData.setAccruedInterest((float) (tradeResultSet.getFloat("PI")));
        cdwMarginData.setCurrency((String) (tradeResultSet.getString("CURRENCY")));
		cdwMarginData.setAccountIdAsOfDate(cdwMarginData.getAccount_id(),cdwMarginData.getAccountIdAsOfDate());
        cdwMarginData.setAccountInitMargin(getAccountMargin(cdwMarginData,accountIdInitMargin).floatValue());

	} catch (SQLException e) {

		logWriter.error("Exception in storeTradeTableResults()");
		e.printStackTrace();
	}
	Logger.getLogger("Executed Method storeTradeTableResults()");
	return cdwMarginData;
}

/**
 * Description: Using the accountIdInitMargin hashmap we get the
 * AccountMargin excess deficit for that specific account for that specific
 * date
 * @param cdwMarginData
 * @param accountIdInitMargin
 * @return
 */
	private Float getAccountMargin(CdwMarginData cdwMarginData,HashMap accountIdInitMargin) {
		
		Float accountInitMargin = null;
		HashMap dateAccountInitMargin = null;
		try {
              dateAccountInitMargin = (HashMap) accountIdInitMargin.get(cdwMarginData.getAccount_id());

     //  The value for the HashMap will be a HashMap containing a HashMap for Date - Key and Value -- Account Init Margin

		accountInitMargin = (Float) dateAccountInitMargin.get(cdwMarginData.getAsOfDate());

	} catch (Exception e) {
		logWriter.error("Exception in  ");
        }
	Logger.getLogger("Executed Method getAccountMargin()");
	return accountInitMargin;
   }

/**
 * Description:Using the set of values for accountId$AsOfDate string
 * calculate the total notional for an account for that day
 * @param accountIdAsOfDate
 * @param cdwTradeQueryRows
 * @return
 */
private HashMap processAccountTradeQueryResult(HashSet accountIdAsOfDate,ArrayList cdwTradeQueryRows) {
	CdwMarginData cdwData = null;
	float totalNotional = 0;
	String clientIdAndDate = "";
	HashMap clientAccountTotalNotionalMap = new HashMap();
	Logger.getLogger("The size of the parameters are"+ accountIdAsOfDate.size() + "&&&&" + cdwTradeQueryRows.size());
	Iterator accountIdAsOfDateiterator = accountIdAsOfDate.iterator();
	while (accountIdAsOfDateiterator.hasNext()) {

		clientIdAndDate = (String) accountIdAsOfDateiterator.next();

		for (int j = 0; j < cdwTradeQueryRows.size(); j++) {
			cdwData = (CdwMarginData) cdwTradeQueryRows.get(j);
			if (clientIdAndDate.equals(cdwData.getAccountIdAsOfDate()))
				totalNotional = totalNotional + cdwData.getNotional();
		}
		// Put the value as AccIDDate as Key and TotalNotional as Value
		clientAccountTotalNotionalMap.put(clientIdAndDate, new Float(totalNotional));
		totalNotional = 0;
	}// End Iterator
	Logger.getLogger("Executed Method processAccountTradeQueryResult()");
	return clientAccountTotalNotionalMap;
}

/**
 * Set the Initial Margin for each Trade ID in the ArrayList
 * 
 * @param accountIdTotalNotional
 * @param cdwTradeQueryRows
 * @return
 */
private ArrayList setInitialMargin(HashMap accountIdTotalNotional,ArrayList cdwTradeQueryRows) {
	
	CdwMarginData cdwData = null;
	float initialMargin = 0;
	for (int i = 0; i < cdwTradeQueryRows.size(); i++) {
		
		cdwData = (CdwMarginData) cdwTradeQueryRows.get(i);
		Float totalNotional = (Float) accountIdTotalNotional.get(cdwData.getAccountIdAsOfDate());
		
		// Calculate the initial margin for each trade
		initialMargin = calculateInitialMargin(totalNotional, cdwData);
		System.out.println("The Initial Margin Value is"+initialMargin);
		//logWriter.error("The IM value is"+initialMargin);
		cdwData.setInitialMargin(initialMargin);
	}
	Logger.getLogger("Executed Method setInitialMargin()");
	return cdwTradeQueryRows;
}

/**
 * Calculates the InitialMargin based on the formula- - {(Trade Notional for
 * TRADE T1*AccountInitialMarginExcessDeficit for that day for that Account)/Total Notional from the Account or Agreement for that specific
 * Day)
 * @param totalNotional
 * @param cdwData
 * @return
 */
private float calculateInitialMargin(Float totalNotional,CdwMarginData cdwData) {
	float initialMargin = 0;
    initialMargin = (cdwData.getNotional() * cdwData.getAccountInitMargin())/totalNotional.floatValue();
    Logger.getLogger("Executed Method calculateInitialMargin()");
	return initialMargin;
}

/**
 * Iterates through the cdwRows and gets the unique combination of ClientId
 * and Dates and passes them into a HashSet Also creates a HashSet
 * containing the Unique TradeIds 
 * @param cdwRows
 * @return
 */
private ArrayList getUniqueClientAccountDateTradeIds(ArrayList cdwRows) {
	HashSet accountIdAsOfDate = new HashSet();
	HashSet tradeIdSet = new HashSet();
	ArrayList uniqueValueList = new ArrayList();
	for (int i = 0; i < cdwRows.size(); i++) {
		CdwMarginData cdwData = (CdwMarginData) cdwRows.get(i);
		accountIdAsOfDate.add(cdwData.getAccountIdAsOfDate());
		tradeIdSet.add(cdwData.getTradeId());
              }
    uniqueValueList.add(accountIdAsOfDate);
	uniqueValueList.add(tradeIdSet);
	Logger.getLogger("Executed Method getUniqueClientAccountDateTradeIds()");
	return uniqueValueList;
}

/*-----------------------Writing to a CSV file starts-----------------------------*/
/**
 * Description: Total the margin values per tradeId
 * @param tradeIdSet
 * @param cdwDataWithInitialMargin
 * @return
 */
private ArrayList cdwCsvData(HashSet tradeIdSet,ArrayList cdwDataWithInitialMargin) {
	
	ArrayList cdwCsvData = new ArrayList();
	Iterator tradeIdSetiterator = tradeIdSet.iterator();
	
	while (tradeIdSetiterator.hasNext()) {
		CdwMarginData cdwData = null;
		String tradeId = "",brokerBicCode = "",currency = "";
		float totalInitialMargin = 0,totalVariationMargin = 0,totalAccruedInterest = 0,totalPostedCollateral=0;
		tradeId = (String) tradeIdSetiterator.next();
		for (int i = 0; i < cdwDataWithInitialMargin.size(); i++) {
			cdwData = (CdwMarginData) cdwDataWithInitialMargin.get(i);
			brokerBicCode = cdwData.getBrokerBicCode();
			currency = cdwData.getCurrency();
			if (tradeId.equals(cdwData.getTradeId())) {
				totalInitialMargin += cdwData.getInitialMargin();
				totalVariationMargin += cdwData.getVariationMargin();
				totalAccruedInterest += cdwData.getAccruedInterest();
				 totalPostedCollateral = totalInitialMargin+totalVariationMargin+totalAccruedInterest;
			}
		}
		//Create a new object for the Trade ID with the collateral Information and is added to an ArrayList
		cdwCsvData.add(new CdwMarginData(getBrokerStatementProperty(),tradeId,
				totalInitialMargin, totalVariationMargin,
				totalAccruedInterest,totalPostedCollateral, brokerBicCode, currency));
	}// end of the iterator
	Logger.getLogger("Executed Method cdwCsvData()");
	return cdwCsvData;
}

/**
 * Description: Method to Set Data and call the method csvWriter passing the appropriate data
 * @param cdwCsvData
 */
private void writeToCsv(ArrayList cdwCsvData) {
	
	try {
		
		CdwMarginData cdwMargin = null;
		String fileLocation =  getBrokerStatementProperty().getProperty("FILE_LOCATION");
		Integer rowCount = new Integer(0);
		String comma=",";
		String emptyString[] = {"No treasury trades to process this month"};
		String[] tradeInformation=null;
		String[] csvHeaders=null,csvColumnNames=null,csvTrailers = null;
		
		
		FileWriter csvFile = new FileWriter(fileLocation);
		
		//Set the precision upto 2 places after the decimal point 
		DecimalFormat df = new DecimalFormat("#.##");
		//Set the csv Header and columnNames
		csvHeaders = new String[]{brokerStatementProperty.getProperty("RECORD_TYPE_HEADER"),getExecutionDateString(),brokerStatementProperty.getProperty("FILENAME")};
		csvColumnNames = new String[]{brokerStatementProperty.getProperty("CH_RECORD_TYPE_T"),brokerStatementProperty.getProperty("CH_SOURCE_SYSID"),brokerStatementProperty.getProperty("CH_BROKER_TRADEID"),brokerStatementProperty.getProperty("CH_CLEARING_MEMNAME"),brokerStatementProperty.getProperty("CH_CLEARING_SWIFT_BIC"),brokerStatementProperty.getProperty("CH_INITIALMARGIN"),brokerStatementProperty.getProperty("CH_INITIALMARGIN_TYPE"),brokerStatementProperty.getProperty("CH_VARIATIONMARGIN"),brokerStatementProperty.getProperty("CH_VARIATIONMARGIN_TYPE"),brokerStatementProperty.getProperty("CH_ACCRUEDINTEREST"),brokerStatementProperty.getProperty("CH_TOTAL_COLLATERAL"),brokerStatementProperty.getProperty("CH_COLLATERAL_CURRENCY")};
	     
		//Write the Headers and the column name to the csv file
		csvWriter(csvFile,csvHeaders,comma);
		csvWriter(csvFile,csvColumnNames,comma);
         
		//If the data isnt empty write the rows for the trades in the csv file
		if (!cdwCsvData.isEmpty()) {
		
			for (int i = 0; i < cdwCsvData.size(); i++) {
				rowCount = rowCount + 1;
				cdwMargin = (CdwMarginData) cdwCsvData.get(i);
                tradeInformation = new String[]{brokerStatementProperty.getProperty("RECORD_TYPE"),brokerStatementProperty.getProperty("SOURCE_SYSTEM_ID"),cdwMargin.getTradeId(),brokerStatementProperty.getProperty("CLEARING_MEMBER_NAME"),brokerStatementProperty.getProperty("BROKER_NAME"),df.format(new Float(cdwMargin.getTotalInitialMargin())),brokerStatementProperty.getProperty("INITIAL_MARGIN_TYPE"),df.format(new Float(cdwMargin.getTotalVariationMargin())),brokerStatementProperty.getProperty("VARIATION_MARGIN_TYPE"),df.format(new Float(cdwMargin.getTotalAccruedInterest())),df.format(new Float(cdwMargin.getTotalPostedCollateral())),cdwMargin.getCurrency()};
               csvWriter(csvFile, tradeInformation, comma);
             }
		} else {
			//If empty write the empty string output to the csv file
			csvWriter(csvFile,emptyString,comma);
		}
		
		// Trailer
		csvTrailers = new String[]{brokerStatementProperty.getProperty("RECORD_TYPE_TRAILER"),rowCount.toString()};
		csvWriter(csvFile,csvTrailers,comma);
		//Empty the Buffer and close the Stream permanently
		csvFile.flush();
		csvFile.close();
		Logger.getLogger("Executed Method writeToCsv()");
	
	} catch (IOException e) {
     	logWriter.error("Exception when writing data to the file");
	}
}
	

/**
 * Function to loop through the String array and write the output to the CSV file
 * @param csvFile
 * @param csvData
 * @param seperator
 * @throws IOException
 */
public void csvWriter(FileWriter csvFile, String[] csvData,String seperator) throws IOException{
	
	for (int size=0;size<csvData.length;size++){
		csvFile.append(csvData[size]);
		if(size!=(csvData.length-1)) csvFile.append(seperator);
		//Insert a new line of you have reached the last element in the String Array
		else csvFile.append('\n');
	     }
	Logger.getLogger("Executed Method csvWriter()");
}

/*-----------------------Writing to a CSV file ends-----------------------------*/
}// End of Class
