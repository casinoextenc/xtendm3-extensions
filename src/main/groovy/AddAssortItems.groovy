/**
 * README
 * This extension is used by Mashup
 *
 * Name : EXT023MI.AddAssortItems
 * Description : Read EXT022 table and call "CRS105MI/AddAssmItem" for each item
 * Date         Changed By   Description
 * 20220112     YBLUTEAU     COMX01- Add assortment
 */
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
public class AddAssortItems extends ExtendM3Transaction {
  private final MIAPI mi;
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final MICallerAPI miCaller;
  private final ProgramAPI program;
  private final UtilityAPI utility;
  private int currentCompany
  private String ascd = ""
  private String cuno = ""
  private String fdat = ""
  private String itno = ""

  public AddAssortItems(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility, MICallerAPI miCaller) {
	this.mi = mi;
	this.database = database;
	this.logger = logger;
	this.program = program;
	this.utility = utility;
	this.miCaller = miCaller
  }

  public void main() {
	if (mi.in.get("CONO") == null) {
	  currentCompany = (Integer) program.getLDAZD().CONO;
	} else {
	  currentCompany = mi.in.get("CONO");
	}
	ascd = mi.in.get("ASCD")
	cuno = mi.in.get("CUNO")
	
	fdat ="";
	if (mi.in.get("FDAT") == null){
	  mi.error("Date de début est obligatoire");
	  return;
	} else {
	  fdat = mi.in.get("FDAT");
	  if (!utility.call("DateUtil", "isDateValid", fdat, "yyyyMMdd")) {
		mi.error("Date de début est invalide");
		return;
	  }
	}
	logger.debug("EXT023MI_AddAssortItems fdat = " + fdat)

	// Check selection header
	DBAction EXT020_query = database.table("EXT020").index("00").build()
	DBContainer EXT020 = EXT020_query.getContainer();
	EXT020.set("EXCONO", currentCompany);
	EXT020.set("EXASCD", ascd);
	EXT020.set("EXCUNO", cuno);
	EXT020.setInt("EXFDAT", fdat as Integer);
	if(!EXT020_query.readAll(EXT020, 4, EXT020_outData)){
	  mi.error("Entête sélection n'existe pas");
	  return;
	}

	DBAction EXT022_query = database.table("EXT022").index("00").selection("EXITNO").build();
	DBContainer EXT022 = EXT022_query.getContainer();
	EXT022.set("EXCONO", currentCompany);
	EXT022.set("EXASCD", ascd);
	EXT022.set("EXCUNO", cuno);
	EXT022.set("EXFDAT", fdat as Integer);
	if (!EXT022_query.readAll(EXT022, 4, EXT022_outData)) {
	}
  }

  Closure<?> EXT020_outData = { DBContainer EXT020 ->
  }
  Closure<?> EXT022_outData = { DBContainer EXT022 ->
	itno = EXT022.get("EXITNO")
	logger.debug("logger EXT023MI executeCRS105MIAddAssmItem : ascd = " + ascd)
	logger.debug("logger EXT023MI executeCRS105MIAddAssmItem : itno = " + itno)
	logger.debug("logger EXT023MI executeCRS105MIAddAssmItem : fdat = " + fdat)
	executeCRS105MIAddAssmItem(ascd, itno, fdat)
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
}
