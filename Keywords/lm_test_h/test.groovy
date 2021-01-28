package lm_test_h

import static com.kms.katalon.core.checkpoint.CheckpointFactory.findCheckpoint
import static com.kms.katalon.core.testcase.TestCaseFactory.findTestCase
import static com.kms.katalon.core.testdata.TestDataFactory.findTestData
import static com.kms.katalon.core.testobject.ObjectRepository.findTestObject
import com.kms.katalon.core.annotation.Keyword
import com.kms.katalon.core.checkpoint.Checkpoint
import com.kms.katalon.core.checkpoint.CheckpointFactory
import com.kms.katalon.core.mobile.keyword.MobileBuiltInKeywords
import com.kms.katalon.core.model.FailureHandling
import com.kms.katalon.core.testcase.TestCase
import com.kms.katalon.core.testcase.TestCaseFactory
import com.kms.katalon.core.testdata.TestData
import com.kms.katalon.core.testdata.TestDataFactory
import com.kms.katalon.core.testobject.ObjectRepository
import com.kms.katalon.core.testobject.TestObject
import com.kms.katalon.core.webservice.keyword.WSBuiltInKeywords
import com.kms.katalon.core.webui.keyword.WebUiBuiltInKeywords
import internal.GlobalVariable
import MobileBuiltInKeywords as Mobile
import WSBuiltInKeywords as WS
import WebUiBuiltInKeywords as WebUI
import groovy.sql.Sql



public class test {
	//Oracle DB Connection
	@Keyword
	def connectDB (String ora_connect, String ora_username, String ora_password	) {
		def cnnct = Sql.newInstance(ora_connect, ora_username, ora_password,
				"oracle.jdbc.pool.OracleDataSource")
		return cnnct
	}

	// BO Count
	@Keyword
	def bo_Count(String ora_connect, String ora_username,  String ora_password, String ora_schema) {
		def cnnct = Sql.newInstance(ora_connect, ora_username, ora_password,
				"oracle.jdbc.pool.OracleDataSource")
		def str_Query = 'SELECT COUNT(1) AS BO_COUNT FROM '+ora_schema+'.C_REPOS_TABLE WHERE type_ind = 1' as String
		def row_BO = cnnct.firstRow(str_Query)
		cnnct.close()
		return row_BO
	}
	
	// BO Column Count
	@Keyword
	def bo_Col_Count(String ora_connect, String ora_username,  String ora_password, String ora_schema) {
		def cnnct = Sql.newInstance(ora_connect, ora_username, ora_password,
				"oracle.jdbc.pool.OracleDataSource")
		def str_Query = 'SELECT count (1) as BO_COLUMN_COUNT FROM '+ora_schema+'.C_REPOS_COLUMN crc JOIN '+ora_schema+'.C_REPOS_TABLE crt ON crc.rowid_table = crt.rowid_table WHERE CRT.TYPE_IND = 1' as String
		def row_BO = cnnct.firstRow(str_Query)
		cnnct.close()
		return row_BO
	}
	
	//Landing Count
	@Keyword
	def ldg_count(String ora_connect, String ora_schema, String ora_username, String ora_password) {
		def cnnct = Sql.newInstance(ora_connect, ora_username, ora_password,
				"oracle.jdbc.pool.OracleDataSource")
		def str_Query = 'SELECT COUNT(1) AS LDG_COUNT FROM '+ora_schema+'.C_REPOS_TABLE WHERE type_ind = 17' as String
		def row_LDG = cnnct.firstRow(str_Query)
		cnnct.close()
		return row_LDG
	}
	
	//Landing Column Count
	@Keyword
	def ldg_col_count(String ora_connect, String ora_schema, String ora_username, String ora_password) {
		def cnnct = Sql.newInstance(ora_connect, ora_username, ora_password,
				"oracle.jdbc.pool.OracleDataSource")
		def str_Query = 'SELECT count (1) as LDG_COLUMN_COUNT FROM '+ora_schema+'.C_REPOS_COLUMN crc JOIN '+ora_schema+'.C_REPOS_TABLE crt ON crc.rowid_table = crt.rowid_table WHERE CRT.TYPE_IND = 17' as String
		def row_LDG = cnnct.firstRow(str_Query)
		cnnct.close()
		return row_LDG
	}
	
	//Stage Count
	@Keyword
	def stg_count(String ora_connect, String ora_schema, String ora_username, String ora_password) {
		def cnnct = Sql.newInstance(ora_connect, ora_username, ora_password,
				"oracle.jdbc.pool.OracleDataSource")
		def str_Query = 'SELECT COUNT(1) AS STG_COUNT FROM '+ora_schema+'.C_REPOS_TABLE WHERE type_ind = 10' as String
		def row_STG = cnnct.firstRow(str_Query)
		cnnct.close()
		return row_STG
	}
	
	//Stage Column Count
	@Keyword
	def stg_col_count(String ora_connect, String ora_schema, String ora_username, String ora_password) {
		def cnnct = Sql.newInstance(ora_connect, ora_username, ora_password,
				"oracle.jdbc.pool.OracleDataSource")
		def str_Query = 'SELECT count (1) as STG_COLUMN_COUNT FROM '+ora_schema+'.C_REPOS_COLUMN crc JOIN '+ora_schema+'.C_REPOS_TABLE crt ON crc.rowid_table = crt.rowid_table WHERE CRT.TYPE_IND = 10 AND crc.SYS_IND = 0' as String
		def row_STG = cnnct.firstRow(str_Query)
		cnnct.close()
		return row_STG
	}
	
	//Source Count
	@Keyword
	def src_count(String ora_connect, String ora_schema, String ora_username, String ora_password) {
		def cnnct = Sql.newInstance(ora_connect, ora_username, ora_password,
				"oracle.jdbc.pool.OracleDataSource")
		def str_Query = 'select count (1) as SRC_System_Count from '+ora_schema+'.C_REPOS_SYSTEM' as String
		def row_SRC = cnnct.firstRow(str_Query)
		cnnct.close()
		return row_SRC
	}
	
	//Source Trust Count
	@Keyword
	def srcTrust_count(String ora_connect, String ora_schema, String ora_username, String ora_password) {
		def cnnct = Sql.newInstance(ora_connect, ora_username, ora_password,
				"oracle.jdbc.pool.OracleDataSource")
		def str_Query = 'select count (1) as TRUST_COLUMN_Count from '+ora_schema+'.C_REPOS_SYS_COL_TRUST' as String
		def row_TRUST = cnnct.firstRow(str_Query)
		cnnct.close()
		return row_TRUST
	}
	
	//Match Path Count
	@Keyword
	def match_path_count(String ora_connect, String ora_schema, String ora_username, String ora_password) {
		def cnnct = Sql.newInstance(ora_connect, ora_username, ora_password,
				"oracle.jdbc.pool.OracleDataSource")
		def str_Query = 'select count (1) as mtch_Path_Count  from '+ora_schema+'.C_REPOS_MATCH_PATH_COMPONENT pc left join '+ora_schema+'.C_REPOS_MATCH_PATH_COMPONENT pc1 on pc.rowid_parent_component = pc1.rowid_match_path_component join '+ora_schema+'.C_REPOS_TABLE t on t.rowid_table = PC.PARENT_ROWID_TABLE join '+ora_schema+'.C_REPOS_TABLE t1 on t1.rowid_table = PC.child_ROWID_TABLE join '+ora_schema+'.C_REPOS_TABLE t2 on t2.rowid_table = PC.MATCH_FOR_ROWID_TABLE' as String
		def row_MTCH = cnnct.firstRow(str_Query)
		cnnct.close()
		return row_MTCH
	}
	
	//Match Fuzzy Count
	@Keyword
	def match_fuzzy_count(String ora_connect, String ora_schema, String ora_username, String ora_password) {
		def cnnct = Sql.newInstance(ora_connect, ora_username, ora_password,
				"oracle.jdbc.pool.OracleDataSource")
		def str_Query = "SELECT CASE WHEN COUNT (1) = 1 THEN 1 ELSE COUNT (1) - 1 END AS MTCH_Fuzzy_Match_Key_Count FROM (SELECT DISTINCT T.DISPLAY_NAME AS Source_Table, mc.match_column_Name, mc.match_key_width_str FROM "+ora_schema+".C_REPOS_MATCH_COLUMN mc JOIN "+ora_schema+".C_REPOS_TABLE t ON MC.MATCH_FOR_ROWID_TABLE = T.ROWID_TABLE WHERE match_key_ind = 1 UNION SELECT 'N/A' AS Source_Table, 'N/A' AS match_column_Name, 'N/A' AS match_key_width_str FROM DUAL)" as String
		def row_MTCH = cnnct.firstRow(str_Query)
		cnnct.close()
		return row_MTCH
	}
	
	//Match Column Count
	@Keyword
	def match_col_count(String ora_connect, String ora_schema, String ora_username, String ora_password) {
		def cnnct = Sql.newInstance(ora_connect, ora_username, ora_password,
				"oracle.jdbc.pool.OracleDataSource")
		def str_Query = "SELECT COUNT (1) AS MTCH_Column_Count FROM "+ora_schema+".C_REPOS_MATCH_COLUMN mc join "+ora_schema+".C_REPOS_MATCH_COL_SRC mcs ON mc.ROWID_MATCH_COLUMN = MCS.ROWID_MATCH_COLUMN" as String
		def row_MTCH = cnnct.firstRow(str_Query)
		cnnct.close()
		return row_MTCH
	}
	
	//BO Query Count
	@Keyword
	def BO_Query_count(String ora_connect, String ora_schema, String ora_username, String ora_password) {
		def cnnct = Sql.newInstance(ora_connect, ora_username, ora_password,
				"oracle.jdbc.pool.OracleDataSource")
		def str_Query = 'select count(1) as BO_QUERY_COUNT from '+ora_schema+'.C_REPOS_QUERY' as String
		//SELECT count (1) as BO_QUERY_COUNT FROM '+ora_schema+'.C_REPOS_COLUMN crc JOIN '+ora_schema+'.C_REPOS_TABLE crt ON crc.rowid_table = crt.rowid_table WHERE CRT.TYPE_IND = 1'
		def row_Qry = cnnct.firstRow(str_Query)
		cnnct.close()
		return row_Qry
	}
	
	//BO Query Group Count
	@Keyword
	def BO_query_grp_count(String ora_connect, String ora_schema, String ora_username, String ora_password) {
		def cnnct = Sql.newInstance(ora_connect, ora_username, ora_password,
				"oracle.jdbc.pool.OracleDataSource")
		def str_Query = 'select count(1) as BO_QUERY_COUNT from '+ora_schema+'.C_REPOS_QUERY_GROUP' as String
		//SELECT count (1) as BO_QUERY_COUNT FROM '+ora_schema+'.C_REPOS_COLUMN crc JOIN '+ora_schema+'.C_REPOS_TABLE crt ON crc.rowid_table = crt.rowid_table WHERE CRT.TYPE_IND = 1'
		def row_Qry = cnnct.firstRow(str_Query)
		cnnct.close()
		return row_Qry
	}
	
	//BO SQL Count
	@Keyword
	def BO_sql_count(String ora_connect, String ora_schema, String ora_username, String ora_password) {
		def cnnct = Sql.newInstance(ora_connect, ora_username, ora_password,
				"oracle.jdbc.pool.OracleDataSource")
		def str_Query = 'select count(1) as BO_QUERY_COUNT from '+ora_schema+'.C_REPOS_QUERY_SQL' as String
		//SELECT count (1) as BO_QUERY_COUNT FROM '+ora_schema+'.C_REPOS_COLUMN crc JOIN '+ora_schema+'.C_REPOS_TABLE crt ON crc.rowid_table = crt.rowid_table WHERE CRT.TYPE_IND = 1'
		def row_Qry = cnnct.firstRow(str_Query)
		cnnct.close()
		return row_Qry
	}
	
	//Package Count
	@Keyword
	def package_count(String ora_connect, String ora_schema, String ora_username, String ora_password) {
		def cnnct = Sql.newInstance(ora_connect, ora_username, ora_password,
				"oracle.jdbc.pool.OracleDataSource")
		def str_Query = 'select count(1) as BO_PACKAGE_COUNT from '+ora_schema+'.C_REPOS_PACKAGE_ATTRIBUTES' as String
		//SELECT count (1) as BO_QUERY_COUNT FROM '+ora_schema+'.C_REPOS_COLUMN crc JOIN '+ora_schema+'.C_REPOS_TABLE crt ON crc.rowid_table = crt.rowid_table WHERE CRT.TYPE_IND = 1'
		def row_Qry = cnnct.firstRow(str_Query)
		cnnct.close()
		return row_Qry
	}
	
	//BG Count
	@Keyword
	def BG_count(String ora_connect, String ora_schema, String ora_username, String ora_password) {
		def cnnct = Sql.newInstance(ora_connect, ora_username, ora_password,
				"oracle.jdbc.pool.OracleDataSource")
		def str_Query = 'select count(1) as BO_BG_COUNT from '+ora_schema+'.c_repos_job_group' as String
		//SELECT count (1) as BO_QUERY_COUNT FROM '+ora_schema+'.C_REPOS_COLUMN crc JOIN '+ora_schema+'.C_REPOS_TABLE crt ON crc.rowid_table = crt.rowid_table WHERE CRT.TYPE_IND = 1'
		def row_Qry = cnnct.firstRow(str_Query)
		cnnct.close()
		return row_Qry
	}
}


