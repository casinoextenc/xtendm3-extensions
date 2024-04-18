/**
 * README
 * This extension is used by Mashup
 *
 * Name : EXT042MI.AddAssortSel
 * Description : The AddAssortSel transaction adds records to the EXT042 table.
 * Date         Changed By   Description
 * 20230317     ARENARD      COMX02 - Cadencier
 * 20240305     FLEBARS       Controle OASITN
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
public class AddAssortSel extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final MICallerAPI miCaller
  private final ProgramAPI program
  private final UtilityAPI utility
  private Integer fltx
  private Integer flcs
  private Integer flxl

  public AddAssortSel(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program,UtilityAPI utility) {
    this.mi = mi
    this.database = database
    this.logger = logger
    this.program = program
    this.utility = utility
  }

  public void main() {
    Integer currentCompany

    // Check company
    if (mi.in.get("CONO") == null) {
      currentCompany = (Integer)program.getLDAZD().CONO
    } else {
      currentCompany = mi.in.get("CONO")
    }

    // Check customer
    if(mi.in.get("CUNO") != null){
      DBAction OCUSMAquery = database.table("OCUSMA").index("00").build()
      DBContainer OCUSMA = OCUSMAquery.getContainer()
      OCUSMA.set("OKCONO",currentCompany)
      OCUSMA.set("OKCUNO",mi.in.get("CUNO"))
      if (!OCUSMAquery.read(OCUSMA)) {
        mi.error("Code Client " + mi.in.get("CUNO") + " n'existe pas")
        return
      }
    }else{
      mi.error("Code Client est obligatoire")
      return
    }

    if (mi.in.get("CUNO") == "97290"){
      mi.error("CUNO A LA CON")
      return
    }

    // Check calendar
    if(mi.in.get("CDNN") == null){
      mi.error("Code cadencier est obligatoire")
      return
    }

    // Check assortment
    if(mi.in.get("ASCD") != null){
      DBAction CSYTABquery = database.table("CSYTAB").index("00").build()
      DBContainer CSYTAB = CSYTABquery.getContainer()
      CSYTAB.set("CTCONO",currentCompany)
      CSYTAB.set("CTSTCO",  "ASCD")
      CSYTAB.set("CTSTKY", mi.in.get("ASCD"))
      if (!CSYTABquery.read(CSYTAB)) {
        mi.error("Code Assortiment  " + mi.in.get("ASCD") + " n'existe pas")
        return
      }
    }else{
      mi.error("Code Assortiment est obligatoire")
      return
    }

    DBAction OASITN_query = database.table("OASITN").index("00").build()
    DBContainer OASITN_container = OASITN_query.getContainer()
    OASITN_container.set("OICONO",currentCompany)
    OASITN_container.set("OIASCD",  mi.in.get("ASCD"))

    Closure<?> OASITN_reader = { DBContainer container ->
      //Use found record(s) as intended
    }

    if (!OASITN_query.readAll(OASITN_container, 2, 1, OASITN_reader)) {
      mi.error("Aucun article pour l'assortiment  " + mi.in.get("ASCD"))
      return
    }

    //Check Output Type
    fltx = (int)(mi.in.get("FLTX") != null ? mi.in.get("FLTX") : 0)
    flcs = (int)(mi.in.get("FLCS") != null ? mi.in.get("FLCS") : 0)
    flxl = (int)(mi.in.get("FLXL") != null ? mi.in.get("FLXL") : 0)
    //if ((fltx+flcs+flxl) ==0) {
    //    mi.error("Au moins un type de sortie doit être selectionné")
    //   return
    //}

    LocalDateTime timeOfCreation = LocalDateTime.now()
    DBAction query = database.table("EXT042").index("00").build()
    DBContainer EXT042 = query.getContainer()
    EXT042.set("EXCONO", currentCompany)
    EXT042.set("EXCUNO", mi.in.get("CUNO"))
    EXT042.set("EXCDNN", mi.in.get("CDNN"))
    EXT042.set("EXASCD", mi.in.get("ASCD"))
    if (!query.read(EXT042)) {
      EXT042.setInt("EXFLTX", fltx)
      EXT042.setInt("EXFLCS", flcs)
      EXT042.setInt("EXFLXL", flxl)
      EXT042.setInt("EXRGDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      EXT042.setInt("EXRGTM", timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")) as Integer)
      EXT042.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      EXT042.setInt("EXCHNO", 1)
      EXT042.set("EXCHID", program.getUser())
      query.insert(EXT042)
    } else {
      mi.error("L'enregistrement existe déjà")
      return
    }
  }
}
