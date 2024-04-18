/**
 * README
 * This extension is used by Mashup
 *
 * Name : EXT023MI.UpdAssortItems
 * Description : Read EXT022 table, delete items from the assortment that no longer apply (CRS105MI/DltAssmItem) and add new items (CRS105MI/AddAssmItem)
 * Date         Changed By   Description
 * 20220112     YBLUTEAU     COMX01 - Add assortment
 * 20240206     YVOYOU       COMX01 - Exclu item
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class UpdAssortItems extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final MICallerAPI miCaller
  private final ProgramAPI program
  private final UtilityAPI utility
  private int currentCompany
  private String ascd
  private String cuno
  private String fdat
  private String itno
  private boolean exclu

  public UpdAssortItems(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility, MICallerAPI miCaller) {
	this.mi = mi
	this.database = database
	this.logger = logger
	this.program = program
	this.utility = utility
	this.miCaller = miCaller
  }

  public void main() {
	if (mi.in.get("CONO") == null) {
	  currentCompany = (Integer) program.getLDAZD().CONO;
	} else {
	  currentCompany = mi.in.get("CONO")
	}
	ascd = mi.in.get("ASCD")
	cuno = mi.in.get("CUNO")

	fdat ="";
	if (mi.in.get("FDAT") == null){
	  mi.error("Date de début est obligatoire")
	  return
	} else {
	  fdat = mi.in.get("FDAT");
	  if (!utility.call("DateUtil", "isDateValid", fdat, "yyyyMMdd")) {
		mi.error("Date de début est invalide")
		return
	  }
	}
	logger.debug("EXT023MI_UpdAssortItems fdat = " + fdat)

	// Check selection header
	DBAction EXT020_query = database.table("EXT020").index("00").build()
	DBContainer EXT020 = EXT020_query.getContainer()
	EXT020.set("EXCONO", currentCompany)
	EXT020.set("EXASCD", ascd)
	EXT020.set("EXCUNO", cuno)
	EXT020.setInt("EXFDAT", fdat as Integer)
	if(!EXT020_query.readAll(EXT020, 4, EXT020_outData)){
	  mi.error("Entête sélection n'existe pas")
	  return
	}

	// Delete of non-selected items present in the assortment
	ExpressionFactory expression = database.getExpressionFactory("OASITN")
	expression = expression.eq("OIFDAT", fdat)
	DBAction OASITN_query = database.table("OASITN").index("00").matching(expression).build()
	DBContainer OASITN = OASITN_query.getContainer()
	OASITN.set("OICONO", currentCompany)
	OASITN.set("OIASCD", ascd)
	if (!OASITN_query.readAll(OASITN, 2, OASITN_outData)) {
	}

	// Add of selected items that are not in the assortment
	DBAction EXT022_query = database.table("EXT022").index("00").selection("EXITNO").build()
	DBContainer EXT022 = EXT022_query.getContainer()
	EXT022.set("EXCONO", currentCompany)
	EXT022.set("EXASCD", ascd)
	EXT022.set("EXCUNO", cuno)
	EXT022.set("EXFDAT", fdat as Integer)
	if (!EXT022_query.readAll(EXT022, 4, EXT022_outData)) {
	}
  }

  Closure<?> EXT020_outData = { DBContainer EXT020 ->
  }
  Closure<?> EXT022_outData = { DBContainer EXT022 ->
    	itno = EXT022.get("EXITNO")
    	DBAction query = database.table("OASITN").index("00").build()
    	DBContainer OASITN = query.getContainer()
    	OASITN.set("OICONO", currentCompany)
    	OASITN.set("OIASCD", ascd)
    	OASITN.set("OIITNO", itno)
    	OASITN.set("OIFDAT", fdat as Integer)
    	if (!query.read(OASITN)) {
        	logger.debug("logger EXT023MI executeCRS105MIAddAssmItem : ascd = " + ascd)
        	logger.debug("logger EXT023MI executeCRS105MIAddAssmItem : itno = " + itno)
        	logger.debug("logger EXT023MI executeCRS105MIAddAssmItem : fdat = " + fdat)
      		//Search Item exclusion
      		exclu = false
      		ExpressionFactory expression_EXT025 = database.getExpressionFactory("EXT025")
      		expression_EXT025 = expression_EXT025.le("EXFDAT", fdat)
      		
      		DBAction EXT025_query = database.table("EXT025").index("00").matching(expression_EXT025).selection("EXCONO", "EXITNO", "EXCUNO", "EXFDAT", "EXRGDT", "EXRGTM", "EXLMDT", "EXCHNO", "EXCHID").build()
      		DBContainer EXT025 = EXT025_query.getContainer()
      		EXT025.set("EXCONO", currentCompany)
      		EXT025.set("EXCUNO", cuno)
      		EXT025.set("EXITNO", itno)
      		if(!EXT025_query.readAll(EXT025, 3, EXT025_outData)){
      		}
      		
      		if (!exclu) {
      		  // Add assortment item
      	    executeCRS105MIAddAssmItem(ascd, itno, fdat)
      		}
	    }
  }
  Closure<?> EXT025_outData = { DBContainer EXT025 ->
		exclu = true
	}
	
  Closure<?> OASITN_outData = { DBContainer OASITN ->
	itno = OASITN.get("OIITNO")
	DBAction EXT022_query = database.table("EXT022").index("00").selection("EXITNO").build()
	DBContainer EXT022 = EXT022_query.getContainer()
	EXT022.set("EXCONO", currentCompany)
	EXT022.set("EXASCD", ascd)
	EXT022.set("EXCUNO", cuno)
	EXT022.set("EXFDAT", fdat as Integer)
	EXT022.set("EXITNO", itno)
	if (!EXT022_query.read(EXT022)) {
	  // Delete non-selected item from assortment
	  executeCRS105MIDltAssmItem(ascd, itno, fdat)
	}
  }
  private executeCRS105MIAddAssmItem(String ASCD, String ITNO, String FDAT){
	def parameters = ["ASCD": ASCD, "ITNO": ITNO, "FDAT": FDAT]
	Closure<?> handler = { Map<String, String> response ->
	  if (response.error != null) {
	  } else {
	  }
	}
	miCaller.call("CRS105MI", "AddAssmItem", parameters, handler)
  }
  private executeCRS105MIDltAssmItem(String ASCD, String ITNO, String FDAT){
	def parameters = ["ASCD": ASCD, "ITNO": ITNO, "FDAT": FDAT]
	Closure<?> handler = { Map<String, String> response ->
	  if (response.error != null) {
		return mi.error("Failed CRS105MI.DltAssmItem: "+ response.errorMessage)
	  } else {
	  }
	}
	miCaller.call("CRS105MI", "DltAssmItem", parameters, handler)
  }
}
