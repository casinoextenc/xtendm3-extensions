/**
 * README
 * This extension is used by Mashup
 * QUAX01 Gestion du référentiel qualité
 * Name : EXT033MI.LstConstrFeat
 * Description : List records from the EXT033 table.
 * Date         Changed By   Description
 * 20210125     SEAR         QUAX01 - Constraints matrix
 * 20240605     FLEBARS      QUAX01 - Controle code pour validation Infor
 * 20240712     FLEBARS      QUAX01 - Controle code pour validation Infor retours
 */
public class LstConstrFeat extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program

  private int currentCompany

  public LstConstrFeat(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program) {
    this.mi = mi
    this.database = database
    this.logger = logger
    this.program = program
  }

  public void main() {
    if (mi.in.get("CONO") == null) {
      currentCompany = (Integer) program.getLDAZD().CONO
    } else {
      currentCompany = mi.in.get("CONO")
    }
    if (mi.in.get("ZCAR") == null) {
      DBAction ext033Query = database.table("EXT033").index("00").selection("EXZCAR", "EXZDES", "EXRGDT", "EXRGTM", "EXLMDT", "EXCHNO", "EXCHID").build()
      DBContainer ext033Request = ext033Query.getContainer()
      ext033Request.set("EXCONO", currentCompany)
      if (!ext033Query.readAll(ext033Request, 1, 10000, ext033Reader)) {
        mi.error("L'enregistrement n'existe pas")
      }
    } else {
      String constraintType = mi.in.get("ZCAR") as String
      DBAction ext033Query = database.table("EXT033").index("00")
        .selection("EXZCAR", "EXZDES", "EXRGDT", "EXRGTM", "EXLMDT", "EXCHNO", "EXCHID")
        .build()
      DBContainer ext033Request = ext033Query.getContainer()
      ext033Request.set("EXCONO", currentCompany)
      ext033Request.set("EXZCAR", constraintType)
      if (ext033Query.read(ext033Request)) {
        mi.outData.put("ZCAR", ext033Request.get("EXZCAR") as String)
        mi.outData.put("ZDES", ext033Request.get("EXZDES") as String)
        mi.outData.put("RGDT", ext033Request.get("EXRGDT") as String)
        mi.outData.put("RGTM", ext033Request.get("EXRGTM") as String)
        mi.outData.put("LMDT", ext033Request.get("EXLMDT") as String)
        mi.outData.put("CHNO", ext033Request.get("EXCHNO") as String)
        mi.outData.put("CHID", ext033Request.get("EXCHID") as String)
        mi.write()
      } else {
        mi.error("L'enregistrement n'existe pas")
        return
      }
    }
  }

  Closure<?> ext033Reader = { DBContainer ext033Result ->
    String constraintType = ext033Result.get("EXZCAR")
    String description = ext033Result.get("EXZDES")
    String entryDate = ext033Result.get("EXRGDT")
    String entryTime = ext033Result.get("EXRGTM")
    String changeDate = ext033Result.get("EXLMDT")
    String changeNumber = ext033Result.get("EXCHNO")
    String changedBy = ext033Result.get("EXCHID")
    mi.outData.put("ZCAR", constraintType)
    mi.outData.put("ZDES", description)
    mi.outData.put("RGDT", entryDate)
    mi.outData.put("RGTM", entryTime)
    mi.outData.put("LMDT", changeDate)
    mi.outData.put("CHNO", changeNumber)
    mi.outData.put("CHID", changedBy)
    mi.write()
  }
}
