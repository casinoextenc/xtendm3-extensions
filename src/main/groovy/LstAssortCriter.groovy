/**
 * This extension is used by Mashup
 * Name : EXT020MI.LstAssortCriter
 * COMX01 Gestion des assortiments clients
 * Description : The LstAssortCriter transaction list records to the EXT020 table.
 * Date         Changed By   Description
 * 20220112     YBLUTEAU     COMX01 - Add assortment
 * 20240620     FLEBARS       COMX01 - Controle code pour validation Infor
 */
public class LstAssortCriter extends ExtendM3Transaction {
  private final MIAPI mi;
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program
  private final UtilityAPI utility

  private int currentCompany

  public LstAssortCriter(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility) {
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
    if (mi.in.get("CONO") == null) {
      currentCompany = (Integer) program.getLDAZD().CONO
    } else {
      currentCompany = mi.in.get("CONO") as Integer
    }

    if (mi.in.get("FDAT") != null) {
      fdat = mi.in.get("FDAT")
      if (!utility.call("DateUtil", "isDateValid", fdat, "yyyyMMdd")) {
        mi.error("Format Date de Validité incorrect")
        return
      }
    }

    //Create Expression
    ExpressionFactory ext020Expression = database.getExpressionFactory("EXT020")
    ext020Expression = ext020Expression.eq("EXCONO", currentCompany.toString())
    if (cuno != "") {
      ext020Expression = ext020Expression.and(ext020Expression.ge("EXCUNO", cuno))
    }
    if (ascd != "") {
      ext020Expression = ext020Expression.and(ext020Expression.ge("EXASCD", ascd))
    }
    if (fdat != "") {
      ext020Expression = ext020Expression.and(ext020Expression.ge("EXFDAT", fdat))
    }
    //Run Select
    DBAction ext020Query = database.table("EXT020").index("00").matching(ext020Expression).selection("EXCONO", "EXASCD", "EXCUNO", "EXFDAT", "EXSTAT", "EXSTTS", "EXNDTS", "EXRGDT", "EXRGTM", "EXLMDT", "EXCHNO", "EXCHID").build()
    DBContainer ext020Request = ext020Query.getContainer()
    ext020Request.setInt("EXCONO", currentCompany)
    if (!ext020Query.readAll(ext020Request, 1, ext020Reader)) {
      mi.error("L'enregistrement n'existe pas")
      return
    }
  }
  Closure<?> ext020Reader = { DBContainer ext020Result ->
    String cono = ext020Result.get("EXCONO")
    String ascd = ext020Result.get("EXASCD")
    String cuno = ext020Result.get("EXCUNO")
    String fdat = ext020Result.get("EXFDAT")
    String stat = ext020Result.get("EXSTAT")
    String stts = ext020Result.get("EXSTTS")
    String ndts = ext020Result.get("EXNDTS")

    String entryDate = ext020Result.get("EXRGDT")
    String entryTime = ext020Result.get("EXRGTM")
    String changeDate = ext020Result.get("EXLMDT")
    String changeNumber = ext020Result.get("EXCHNO")
    String changedBy = ext020Result.get("EXCHID")

    mi.outData.put("CONO", cono)
    mi.outData.put("CUNO", cuno)
    mi.outData.put("ASCD", ascd)
    mi.outData.put("FDAT", fdat)
    mi.outData.put("STAT", stat)
    mi.outData.put("STTS", stts)
    mi.outData.put("NDTS", ndts)
    mi.outData.put("RGDT", entryDate)
    mi.outData.put("RGTM", entryTime)
    mi.outData.put("LMDT", changeDate)
    mi.outData.put("CHNO", changeNumber)
    mi.outData.put("CHID", changedBy)
    mi.write()
  }
}

