/**
 * README
 * This extension is used by Mashup
 * QUAX01 Gestion du référentiel qualité
 * Name : EXT034MI.LstCodification
 * Description : List records from the EXT034 table.
 * Date         Changed By   Description
 * 20210125     SEAR         QUAX01 - Constraints matrix
 * 20230620     FLEBARS      QUAX01 - evol contrainte
 * 20240605     FLEBARS      QUAX01 - Controle code pour validation Infor
 */
public class LstCodification extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program

  private int currentCompany

  public LstCodification(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program) {
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
    if (mi.in.get("ZCOD") == null) {
      DBAction ext034Query = database.table("EXT034").index("00").selection(
        "EXZCOD",
        "EXZDES",
        "EXZCTY",
        "EXRGDT",
        "EXRGTM",
        "EXLMDT",
        "EXCHNO",
        "EXZSTY",
        "EXCHID").build()
      DBContainer ext034Request = ext034Query.getContainer()
      ext034Request.set("EXCONO", currentCompany)
      if (!ext034Query.readAll(ext034Request, 1, 10000, ext034Reader)) {
        mi.error("L'enregistrement n'existe pas")
        return
      }
    } else {
      String codification = mi.in.get("ZCOD")
      ExpressionFactory ext034Expression = database.getExpressionFactory("EXT034")
      ext034Expression = ext034Expression.ge("EXZCOD", codification)
      DBAction ext034Query = database.table("EXT034").index("00").matching(ext034Expression).selection(
        "EXZCOD",
        "EXZDES",
        "EXZCTY",
        "EXRGDT",
        "EXRGTM",
        "EXLMDT",
        "EXCHNO",
        "EXZSTY",
        "EXCHID").build()
      DBContainer ext034Request = ext034Query.getContainer()
      ext034Request.set("EXCONO", currentCompany)
      if (!ext034Query.readAll(ext034Request, 1, 10000, ext034Reader)) {
        mi.error("L'enregistrement n'existe pas")
      }
    }
  }

  Closure<?> ext034Reader = { DBContainer ext034Result ->
    String codification = ext034Result.get("EXZCOD")
    String description = ext034Result.get("EXZDES")
    String constraintType = ext034Result.get("EXZCTY")
    String entryDate = ext034Result.get("EXRGDT")
    String entryTime = ext034Result.get("EXRGTM")
    String changeDate = ext034Result.get("EXLMDT")
    String changeNumber = ext034Result.get("EXCHNO")
    String changedBy = ext034Result.get("EXCHID")
    mi.outData.put("ZCOD", codification)
    mi.outData.put("ZDES", description)
    mi.outData.put("ZCTY", constraintType)
    mi.outData.put("RGDT", entryDate)
    mi.outData.put("RGTM", entryTime)
    mi.outData.put("LMDT", changeDate)
    mi.outData.put("CHNO", changeNumber)
    mi.outData.put("CHID", changedBy)
    mi.outData.put("ZSTY", ext034Result.get("EXZSTY") as String)
    mi.write()
  }
}
