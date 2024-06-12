/**
 * README
 * This extension is used by Mashup
 * QUAX01 Gestion du référentiel qualité
 * Name : EXT031MI.LstConstrType
 * Description : List records from the EXT031 table.
 * Date         Changed By   Description
 * 20210125     SEAR         QUAX01 - Constraints matrix 
 * 20240605     FLEBARS      QUAX01 - Controle code pour validation Infor
 */


public class LstConstrType extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program

  private int currentCompany

  public LstConstrType(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program) {
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

    if (mi.in.get("ZCTY") == null) {
      DBAction ext031Query = database.table("EXT031").index("00").selection("EXZCTY", "EXZTYP", "EXRGDT", "EXRGTM", "EXLMDT", "EXCHNO", "EXCHID").build()
      DBContainer ext031Request = ext031Query.getContainer()
      ext031Request.set("EXCONO", currentCompany)
      if (!ext031Query.readAll(ext031Request, 1, 10000, ext031Reader)) {
        mi.error("L'enregistrement n'existe pas")
        return
      }
    } else {
      String constraintType = mi.in.get("ZCTY")
      ExpressionFactory ext031Expression = database.getExpressionFactory("EXT031")
      ext031Expression = ext031Expression.ge("EXZCTY", constraintType)
      DBAction ext031Query = database.table("EXT031").index("00").matching(ext031Expression).selection("EXZCTY", "EXZTYP", "EXRGDT", "EXRGTM", "EXLMDT", "EXCHNO", "EXCHID").build()
      DBContainer ext031Request = ext031Query.getContainer()
      ext031Request.set("EXCONO", currentCompany)
      if (!ext031Query.readAll(ext031Request, 1, 10000, ext031Reader)) {
        mi.error("L'enregistrement n'existe pas")
        return
      }
    }
  }

  Closure<?> ext031Reader = { DBContainer ext031Result ->
    String constraintType = ext031Result.get("EXZCTY")
    String description = ext031Result.get("EXZTYP")
    String entryDate = ext031Result.get("EXRGDT")
    String entryTime = ext031Result.get("EXRGTM")
    String changeDate = ext031Result.get("EXLMDT")
    String changeNumber = ext031Result.get("EXCHNO")
    String changedBy = ext031Result.get("EXCHID")
    mi.outData.put("ZCTY", constraintType)
    mi.outData.put("ZTYP", description)
    mi.outData.put("RGDT", entryDate)
    mi.outData.put("RGTM", entryTime)
    mi.outData.put("LMDT", changeDate)
    mi.outData.put("CHNO", changeNumber)
    mi.outData.put("CHID", changedBy)
    mi.write()
  }
}
