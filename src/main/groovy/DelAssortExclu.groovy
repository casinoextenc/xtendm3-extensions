/**
 * This extension is used by Mashup
 * Name : EXT025MI.DelAssortExclu
 * COMX01 Gestion des assortiments clients
 * Description : The DelAssortExclu transaction delete records to the EXT025 table.
 * Date         Changed By   Description
 * 20240206     YVOYOU     COMX01 - Assortment
 * 20240620     FLEBARS       COMX01 - Controle code pour validation Infor
 */
public class DelAssortExclu extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program
  private final UtilityAPI utility

  public DelAssortExclu(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility) {
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
    String fdat = ""
    if (mi.in.get("CONO") == null) {
      currentCompany = (Integer) program.getLDAZD().CONO
    } else {
      currentCompany = mi.in.get("CONO")
    }

    if (mi.in.get("CUNO") != null) {
      cuno = mi.in.get("CUNO")
    } else {
      mi.error("Code client est obligatoire")
      return
    }

    if (mi.in.get("ITNO") != null) {
      itno = mi.in.get("ITNO")
    } else {
      mi.error("Code Article est obligatoire")
      return
    }

    if (mi.in.get("FDAT") != null) {
      fdat = mi.in.get("FDAT")
    } else {
      mi.error("Date de début est obligatoire")
      return
    }
    // Delete EXT025
    DBAction ext025Query = database.table("EXT025").index("00").build()
    DBContainer ext025Request = ext025Query.getContainer()
    ext025Request.set("EXCONO", currentCompany)
    ext025Request.set("EXCUNO", cuno)
    ext025Request.set("EXITNO", itno)
    ext025Request.setInt("EXFDAT", fdat as Integer)
    Closure<?> ext025Updater = { LockedResult ext025LockedResult ->
      ext025LockedResult.delete()
    }
    if (!ext025Query.readLock(ext025Request, ext025Updater)) {
      mi.error("L'enregistrement n'existe pas")
      return
    }
  }
}
