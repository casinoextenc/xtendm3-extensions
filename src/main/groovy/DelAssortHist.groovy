/**
 * This extension is used by Mashup
 * Name : EXT021MI.DelAssortHist
 * COMX01 Gestion des assortiments clients
 * Description : The DelAssortHist transaction delete records to the EXT021 table.
 * Date         Changed By   Description
 * 20220112     YBLUTEAU     COMX01 - Add assortment
 * 20240620     FLEBARS       COMX01 - Controle code pour validation Infor
 */
public class DelAssortHist extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program
  private final UtilityAPI utility

  private int currentCompany


  public DelAssortHist(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility) {
    this.mi = mi
    this.database = database
    this.logger = logger
    this.program = program
    this.utility = utility
  }

  public void main() {
    String cuno = ""
    String ascd = ""
    String fdat = ""
    String type = ""
    String data = ""
    int chb1 = 0
    if (mi.in.get("CONO") == null) {
      currentCompany = (Integer) program.getLDAZD().CONO
    } else {
      currentCompany = mi.in.get("CONO")
    }

    if (mi.in.get("CUNO") != null) {
      cuno = mi.in.get("CUNO")
    } else {
      mi.error("Code Client est obligatoire")
      return
    }
    if (mi.in.get("ASCD") != null) {
      ascd = mi.in.get("ASCD")
    } else {
      mi.error("Code Assortiment est obligatoire")
      return
    }
    if (mi.in.get("FDAT") == null) {
      mi.error("Date de Validité est obligatoire")
      return
    } else {
      fdat = mi.in.get("FDAT")
    }
    if (mi.in.get("TYPE") == null) {
      mi.error("Le type est obligatoire")
      return
    } else {
      type = mi.in.get("TYPE")
    }

    if (mi.in.get("DATA") != null) {
      data = mi.in.get("DATA")
    }

    DBAction ext021Query = database.table("EXT021").index("00").build()
    DBContainer ext021Request = ext021Query.getContainer()
    ext021Request.set("EXCONO", currentCompany)
    ext021Request.set("EXCUNO", cuno)
    ext021Request.set("EXASCD", ascd)
    ext021Request.setInt("EXFDAT", fdat as Integer)
    ext021Request.set("EXTYPE", type)
    ext021Request.set("EXDATA", data)
    Closure<?> ext021Updater = {LockedResult ext021LockedResult ->
      ext021LockedResult.delete()

    }
    if (!ext021Query.readLock(ext021Request, ext021Updater)) {
      mi.error("L'enregistrement n'existe pas")
      return
    }
  }
}
