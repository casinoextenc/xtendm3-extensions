/**
 * This extension is used by Mashup
 * Name : EXT025MI.GetAssortExclu
 * COMX01 Gestion des assortiments clients
 * Description : The GetAssortExclu transaction get records to the EXT025 table.
 * Date         Changed By   Description
 * 20240206     YVOYOU     COMX01 - Assortment
 * 20240620     FLEBARS       COMX01 - Controle code pour validation Infor
 */
public class GetAssortExclu extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program
  private final UtilityAPI utility

  public GetAssortExclu(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility) {
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
      mi.error("Code Client est obligatoire")
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
      if (!utility.call("DateUtil", "isDateValid", fdat, "yyyyMMdd")) {
        mi.error("Format Date de Validité incorrect")
        return
      }
    } else {
      mi.error("Date de Validité est obligatoire")
      return
    }

    DBAction ext025Query = database.table("EXT025").index("00").selection("EXCONO", "EXITNO", "EXCUNO", "EXFDAT", "EXRGDT", "EXRGTM", "EXLMDT", "EXCHNO", "EXCHID").build()
    DBContainer ext025Request = ext025Query.getContainer()
    ext025Request.set("EXCONO", currentCompany)
    ext025Request.set("EXCUNO", cuno)
    ext025Request.set("EXITNO", itno)
    ext025Request.setInt("EXFDAT", fdat as Integer)
    if (ext025Query.read(ext025Request)) {
      mi.outData.put("CONO", ext025Request.get("EXCONO") as String)
      mi.outData.put("CUNO", ext025Request.get("EXCUNO") as String)
      mi.outData.put("ITNO", ext025Request.get("EXITNO") as String)
      mi.outData.put("FDAT", ext025Request.get("EXFDAT") as String)
      mi.outData.put("RGDT", ext025Request.get("EXRGDT") as String)
      mi.outData.put("RGTM", ext025Request.get("EXRGTM") as String)
      mi.outData.put("LMDT", ext025Request.get("EXLMDT") as String)
      mi.outData.put("CHNO", ext025Request.get("EXCHNO") as String)
      mi.outData.put("CHID", ext025Request.get("EXCHID") as String)
      mi.write()
    } else {
      mi.error("L'enregistrement n'existe pas")
      return
    }
  }
}
