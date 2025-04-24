/**
 * README
 * This extension is used by Mashup
 *
 * Name : EXT044MI.AddIntEmalSel
 * Description : The AddIntEmalSel transaction adds records to the EXT044 table.
 * Date         Changed By   Description
 * 20230317     ARENARD      COMX02 - Cadencier
 * 20250416     ARENARD      The code has been checked
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
public class AddIntEmalSel extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final MICallerAPI miCaller
  private final ProgramAPI program
  private final UtilityAPI utility

  public AddIntEmalSel(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program,UtilityAPI utility) {
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
      DBAction ocusmaQuery = database.table("OCUSMA").index("00").build()
      DBContainer OCUSMA = ocusmaQuery.getContainer()
      OCUSMA.set("OKCONO",currentCompany)
      OCUSMA.set("OKCUNO",mi.in.get("CUNO"))
      if (!ocusmaQuery.read(OCUSMA)) {
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
    DBAction query = database.table("EXT044").index("00").build()
    DBContainer EXT044 = query.getContainer()
    EXT044.set("EXCONO", currentCompany)
    EXT044.set("EXCUNO", mi.in.get("CUNO"))
    EXT044.set("EXCDNN", mi.in.get("CDNN"))
    EXT044.set("EXEMAL", mi.in.get("EMAL"))
    if (!query.read(EXT044)) {
      EXT044.setInt("EXRGDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      EXT044.setInt("EXRGTM", timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")) as Integer)
      EXT044.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      EXT044.setInt("EXCHNO", 1)
      EXT044.set("EXCHID", program.getUser())
      query.insert(EXT044)
    } else {
      mi.error("L'enregistrement existe déjà")
      return
    }
  }
}
