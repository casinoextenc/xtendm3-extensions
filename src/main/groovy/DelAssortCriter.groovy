/**
 * README
 * This extension is used by Mashup
 *
 * Name : EXT020MI.DelAssortCriter
 * Description : The DelAssortCriter transaction delete records to the EXT020 table.
 * Date         Changed By   Description
 * 20220112     YBLUTEAU     COMX01 - Add assortment
 */
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
public class DelAssortCriter extends ExtendM3Transaction {
  private final MIAPI mi;
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final MICallerAPI miCaller;
  private final ProgramAPI program;
  private final UtilityAPI utility;

  public DelAssortCriter(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program,UtilityAPI utility) {
	this.mi = mi;
	this.database = database;
	this.logger = logger;
	this.program = program;
	this.utility = utility;
  }

  public void main() {
	Integer currentCompany;
	String cuno = "";
	String ascd = "";
	String fdat ="";
	if (mi.in.get("CONO") == null) {
	  currentCompany = (Integer)program.getLDAZD().CONO;
	} else {
	  currentCompany = mi.in.get("CONO");
	}

	if(mi.in.get("CUNO") != null){
	  /**
	   DBAction countryQuery = database.table("OCUSMA").index("00").build();
	   DBContainer OCUSMA = countryQuery.getContainer();
	   OCUSMA.set("OKCONO",currentCompany);
	   OCUSMA.set("OKCUNO",mi.in.get("CUNO"));
	   if (!countryQuery.read(OCUSMA)) {
	   mi.error("Code Client " + mi.in.get("CUNO") + " n'existe pas");
	   return;
	   }
	   **/
	  cuno = mi.in.get("CUNO");
	}

	if(mi.in.get("ASCD") != null){
	  /**
	   DBAction countryQuery = database.table("CSYTAB").index("00").build();
	   DBContainer CSYTAB = countryQuery.getContainer();
	   CSYTAB.set("CTCONO",currentCompany);
	   CSYTAB.set("CTSTCO",  "ASCD");
	   CSYTAB.set("CTSTKY", mi.in.get("ASCD"));
	   if (!countryQuery.read(CSYTAB)) {
	   mi.error("Code Assortiment  " + mi.in.get("ASCD") + " n'existe pas");
	   return;
	   }
	   **/
	  ascd = mi.in.get("ASCD");
	}

	if(mi.in.get("FDAT") != null){
	  fdat = mi.in.get("FDAT");
	  /**
	   if (!utility.call("DateUtil", "isDateValid", fdat, "yyyyMMdd")) {
	   mi.error("Format Date de Validité incorrect");
	   return;
	   }
	   **/

	}
	// Delete EXT020
	LocalDateTime timeOfCreation = LocalDateTime.now();
	DBAction query = database.table("EXT020").index("00").build()
	DBContainer EXT020 = query.getContainer();
	EXT020.set("EXCONO", currentCompany);
	EXT020.set("EXCUNO", cuno);
	EXT020.set("EXASCD", ascd);
	EXT020.setInt("EXFDAT", fdat as Integer);
	if(!query.readLock(EXT020, updateCallBack)){
	  mi.error("L'enregistrement n'existe pas")
	  return
	}
	 // Delete EXT021
	DBAction EXT021_query = database.table("EXT021").index("00").build()
	DBContainer EXT021 = EXT021_query.getContainer();
	EXT021.set("EXCONO", currentCompany);
	EXT021.set("EXCUNO", cuno);
	EXT021.set("EXASCD", ascd);
	EXT021.setInt("EXFDAT", fdat as Integer);
	if(!EXT021_query.readAllLock(EXT021, 4, updateCallBack)){
	}
//	 // Delete EXT022
//	DBAction EXT022_query = database.table("EXT022").index("00").build()
//	DBContainer EXT022 = EXT022_query.getContainer();
//	EXT022.set("EXCONO", currentCompany);
//	EXT022.set("EXASCD", ascd);
//	EXT022.set("EXCUNO", cuno);
//	EXT022.set("EXFDAT", fdat as Integer);
//	if(!EXT022_query.readAllLock(EXT022, 4, updateCallBack)){
//	}
  }
  Closure<?> updateCallBack = { LockedResult lockedResult ->
	lockedResult.delete()
  }
}
