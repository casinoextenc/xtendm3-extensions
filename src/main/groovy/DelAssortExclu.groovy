/**
 * README
 * This extension is used by Mashup
 *
 * Name : EXT025MI.DelAssortExclu
 * Description : The DelAssortExclu transaction delete records to the EXT025 table.
 * Date         Changed By   Description
 * 20240206     YVOYOU     COMX01 - Assortment
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
public class DelAssortExclu extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final MICallerAPI miCaller
  private final ProgramAPI program
  private final UtilityAPI utility

  public DelAssortExclu(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program,UtilityAPI utility) {
	this.mi = mi
	this.database = database
	this.logger = logger
	this.program = program
	this.utility = utility
  }

  public void main() {
	Integer currentCompany
	String cuno = ""
	String itno = ""
	String fdat =""
	if (mi.in.get("CONO") == null) {
	  currentCompany = (Integer)program.getLDAZD().CONO
	} else {
	  currentCompany = mi.in.get("CONO")
	}

	if(mi.in.get("CUNO") != null){
	  /**
	   DBAction countryQuery = database.table("OCUSMA").index("00").build()
	   DBContainer OCUSMA = countryQuery.getContainer()
	   OCUSMA.set("OKCONO",currentCompany)
	   OCUSMA.set("OKCUNO",mi.in.get("CUNO"))
	   if (!countryQuery.read(OCUSMA)) {
	   mi.error("Code Client " + mi.in.get("CUNO") + " n'existe pas")
	   return
	   }
	   **/
	  cuno = mi.in.get("CUNO")
	}

	if(mi.in.get("ITNO") != null){
			itno = mi.in.get("ITNO")
			DBAction queryMITMAS = database.table("MITMAS").index("00").selection("MMCONO", "MMITNO", "MMSTAT", "MMFUDS").build()
			DBContainer containerMITMAS = queryMITMAS.getContainer()
			containerMITMAS.set("MMCONO", currentCompany)
			containerMITMAS.set("MMITNO", itno)
			if (queryMITMAS.read(containerMITMAS)) {
			  String stat = (String)containerMITMAS.get("MMSTAT")
			  if (!stat.equals("20")){
				mi.error("Statut Article ${itno} est invalide")
				return
			  }
			} else {
			  mi.error("Article ${itno} n'existe pas")
			  return
			}
	}else{
		mi.error("Code Article est obligatoire")
		return
	}

	if(mi.in.get("FDAT") != null){
	  fdat = mi.in.get("FDAT")
	  /**
	   if (!utility.call("DateUtil", "isDateValid", fat, "yyyyMMdd")) {
	   mi.error("Format Date de Validité incorrect")
	   return
	   }
	   **/

	}
	// Delete EXT025
	LocalDateTime timeOfCreation = LocalDateTime.now()
	DBAction query = database.table("EXT025").index("00").build()
	DBContainer EXT025 = query.getContainer()
	EXT025.set("EXCONO", currentCompany)
	EXT025.set("EXCUNO", cuno)
	EXT025.set("EXITNO", itno)
	EXT025.setInt("EXFDAT", fdat as Integer)
	if(!query.readLock(EXT025, updateCallBack)){
	  mi.error("L'enregistrement n'existe pas")
	  return
	}
  }
  Closure<?> updateCallBack = { LockedResult lockedResult ->
	lockedResult.delete()
  }
}
