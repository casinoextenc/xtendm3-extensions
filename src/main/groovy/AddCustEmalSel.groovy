/**
 * README
 * This extension is used by Mashup
 *
 * Name : EXT043MI.AddCustEmalSel
 * Description : The AddCustEmalSel transaction adds records to the EXT043 table.
 * Date         Changed By   Description
 * 20230317     ARENARD      COMX02 - Cadencier
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
public class AddCustEmalSel extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final MICallerAPI miCaller
  private final ProgramAPI program
  private final UtilityAPI utility

  public AddCustEmalSel(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program,UtilityAPI utility) {
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

    // Check calendar
    if(mi.in.get("CDNN") == null){
      mi.error("Code cadencier est obligatoire")
      return
    }
    
    // Check email
    if(mi.in.get("EMAL") == null){
      mi.error("Adresse mail est obligatoire")
      return
    }

    LocalDateTime timeOfCreation = LocalDateTime.now()
    DBAction query = database.table("EXT043").index("00").build()
    DBContainer EXT043 = query.getContainer()
    EXT043.set("EXCONO", currentCompany)
    EXT043.set("EXCUNO", mi.in.get("CUNO"))
    EXT043.set("EXCDNN", mi.in.get("CDNN"))
    EXT043.set("EXEMAL", mi.in.get("EMAL"))
    if (!query.read(EXT043)) {
      EXT043.setInt("EXRGDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      EXT043.setInt("EXRGTM", timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")) as Integer)
      EXT043.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      EXT043.setInt("EXCHNO", 1)
      EXT043.set("EXCHID", program.getUser())
      query.insert(EXT043)
    } else {
      mi.error("L'enregistrement existe déjà")
      return
    }
  }
}
