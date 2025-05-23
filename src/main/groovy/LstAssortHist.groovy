/**
 * This extension is used by Mashup
 * Name : EXT021MI.LstAssortHist
 * COMX01 Gestion des assortiments clients
 * Description : The LstAssortHist transaction list records to the EXT021 table.
 * Date         Changed By   Description
 * 20220112     YBLUTEAU     COMX01 - Add assortment
 * 20240620     FLEBARS       COMX01 - Controle code pour validation Infor
 */
public class LstAssortHist extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program
  private final UtilityAPI utility
  private Integer nbMaxRecord = 10000

  public LstAssortHist(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility) {
    this.mi = mi
    this.database = database
    this.logger = logger
    this.program = program
    this.utility = utility
  }

  public void main() {
    Integer currentCompany
    String cuno = ""
    String ascd = ""
    String fdat = ""
    String type = ""
    if (mi.in.get("CONO") == null) {
      currentCompany = (Integer) program.getLDAZD().CONO
    } else {
      currentCompany = mi.in.get("CONO")
    }

    if (mi.in.get("CUNO") != null) {
      cuno = mi.in.get("CUNO")
    }

    if (mi.in.get("ASCD") != null) {
      ascd = mi.in.get("ASCD")
    }

    if (mi.in.get("FDAT") != null) {
      fdat = mi.in.get("FDAT")
      if (!utility.call("DateUtil", "isDateValid", fdat, "yyyyMMdd")) {
        mi.error("Format Date de Validité incorrect")
        return
      }
    }

    if (mi.in.get("TYPE") != null) {
      type = mi.in.get("TYPE")
    }

    //Create Expression
    ExpressionFactory ext021Expression = database.getExpressionFactory("EXT021")
    ext021Expression = ext021Expression.eq("EXCONO", currentCompany.toString())
    if (cuno != "") {
      ext021Expression = ext021Expression.and(ext021Expression.eq("EXCUNO", cuno))
    }
    if (ascd != "") {
      ext021Expression = ext021Expression.and(ext021Expression.eq("EXASCD", ascd))
    }
    if (fdat != "") {
      ext021Expression = ext021Expression.and(ext021Expression.ge("EXFDAT", fdat))
    }
    if (type != "") {
      ext021Expression = ext021Expression.and(ext021Expression.eq("EXTYPE", type))
    }
    //Run Select
    DBAction ext021Query = database.table("EXT021").index("00").matching(ext021Expression).selection("EXCONO", "EXASCD", "EXCUNO", "EXFDAT", "EXTYPE", "EXCHB1", "EXDATA", "EXRGDT", "EXRGTM", "EXLMDT", "EXCHNO", "EXCHID", "EXTX60").build()
    DBContainer ext021Request = ext021Query.getContainer()
    ext021Request.setInt("EXCONO", currentCompany)
    if (!ext021Query.readAll(ext021Request, 1, nbMaxRecord, ext021Reader)) {
      mi.error("L'enregistrement n'existe pas")
      return
    }
  }
  // Retrieve EXT021
  Closure<?> ext021Reader = {
    DBContainer ext021Result ->
      String cono = ext021Result.get("EXCONO")
      String ascd = ext021Result.get("EXASCD")
      String cuno = ext021Result.get("EXCUNO")
      String fdat = ext021Result.get("EXFDAT")
      String type = ext021Result.get("EXTYPE")
      String chb1 = ext021Result.get("EXCHB1")
      String data = ext021Result.get("EXDATA")
      String entryDate = ext021Result.get("EXRGDT")
      String entryTime = ext021Result.get("EXRGTM")
      String changeDate = ext021Result.get("EXLMDT")
      String changeNumber = ext021Result.get("EXCHNO")
      String changedBy = ext021Result.get("EXCHID")
      String tx60 = ext021Result.get("EXTX60")

      mi.outData.put("CONO", cono)
      mi.outData.put("CUNO", cuno)
      mi.outData.put("ASCD", ascd)
      mi.outData.put("FDAT", fdat)
      mi.outData.put("TYPE", type)
      mi.outData.put("CHB1", chb1)
      mi.outData.put("DATA", data)
      mi.outData.put("TX60", tx60)
      mi.outData.put("RGDT", entryDate)
      mi.outData.put("RGTM", entryTime)
      mi.outData.put("LMDT", changeDate)
      mi.outData.put("CHNO", changeNumber)
      mi.outData.put("CHID", changedBy)
      mi.write()
  }
}
