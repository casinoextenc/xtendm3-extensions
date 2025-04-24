/**
 * This extension is used by Mashup
 * Name : EXT025MI.LstAssortExclu
 * COMX01 Gestion des assortiments clients
 * Description : The LstAssortExclu transaction list records to the EXT025 table.
 * Date         Changed By   Description
 * 20240206     YVOYOU     COMX01 - Assortment
 * 20240620     FLEBARS       COMX01 - Controle code pour validation Infor
 */
public class LstAssortExclu extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program
  private final UtilityAPI utility
  private Integer nbMaxRecord = 10000

  public LstAssortExclu(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility) {
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
    cuno = mi.in.get("CUNO")
    itno = mi.in.get("ITNO")

    if (mi.in.get("FDAT") != null) {
      fdat = mi.in.get("FDAT")
      if (!utility.call("DateUtil", "isDateValid", fdat, "yyyyMMdd")) {
        mi.error("Format Date de Validité incorrect")
        return
      }
    }
    //Create Expression
    ExpressionFactory ext025Expression = database.getExpressionFactory("EXT025")
    ext025Expression = ext025Expression.eq("EXCONO", currentCompany.toString())
    if (cuno != "") {
      ext025Expression = ext025Expression.and(ext025Expression.ge("EXCUNO", cuno))
    }
    if (itno != "") {
      ext025Expression = ext025Expression.and(ext025Expression.ge("EXITNO", itno))
    }
    if (fdat != "") {
      ext025Expression = ext025Expression.and(ext025Expression.ge("EXFDAT", fdat))
    }
    //Run Select
    DBAction ext025Query = database.table("EXT025").index("00").matching(ext025Expression).selection("EXCONO", "EXITNO", "EXCUNO", "EXFDAT", "EXRGDT", "EXRGTM", "EXLMDT", "EXCHNO", "EXCHID").build()
    DBContainer ext025Request = ext025Query.getContainer()
    ext025Request.setInt("EXCONO", currentCompany)
    if (!ext025Query.readAll(ext025Request, 1, nbMaxRecord, ext025Reader)) {
      mi.error("L'enregistrement n'existe pas")
      return
    }
  }

  // Write outData
  Closure<?> ext025Reader = { DBContainer ext025Result ->
    String cono = ext025Result.get("EXCONO")
    String cuno = ext025Result.get("EXCUNO")
    String itno = ext025Result.get("EXITNO")
    String fdat = ext025Result.get("EXFDAT")
    String entryDate = ext025Result.get("EXRGDT")
    String entryTime = ext025Result.get("EXRGTM")
    String changeDate = ext025Result.get("EXLMDT")
    String changeNumber = ext025Result.get("EXCHNO")
    String changedBy = ext025Result.get("EXCHID")
    mi.outData.put("CONO", cono)
    mi.outData.put("CUNO", cuno)
    mi.outData.put("ITNO", itno)
    mi.outData.put("FDAT", fdat)
    mi.outData.put("RGDT", entryDate)
    mi.outData.put("RGTM", entryTime)
    mi.outData.put("LMDT", changeDate)
    mi.outData.put("CHNO", changeNumber)
    mi.outData.put("CHID", changedBy)
    mi.write()
  }
}
