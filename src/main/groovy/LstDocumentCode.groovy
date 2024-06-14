/**
 * README
 * This extension is used by Mashup
 * QUAX01 Gestion du référentiel qualité
 * Name : EXT035MI.LstDocumentCode
 * Description : List records from the EXT035 table.
 * Date         Changed By   Description
 * 20210125     SEAR         QUAX01 - Constraints matrix
 * 20240605     FLEBARS      QUAX01 - Controle code pour validation Infor
 */
public class LstDocumentCode extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program

  private int currentCompany

  public LstDocumentCode(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program) {
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
      DBAction ext035Query = database.table("EXT035").index("00").selection("EXCONO", "EXZCOD", "EXCSCD", "EXCUNO", "EXDOID", "EXADS1", "EXRGDT", "EXRGTM", "EXLMDT", "EXCHNO", "EXCHID").build()
      DBContainer ext035Request = ext035Query.getContainer()
      ext035Request.set("EXCONO", currentCompany)
      if (!ext035Query.readAll(ext035Request, 1, ext035Reader)) {
        mi.error("L'enregistrement n'existe pas")
        return
      }
    } else {
      String constraintCode = (String) (mi.in.get("ZCOD") != null ? mi.in.get("ZCOD") : "")
      String countryCode = (String) (mi.in.get("CSCD") != null ? mi.in.get("CSCD") : "")
      String customerCode = (String) (mi.in.get("CUNO") != null ? mi.in.get("CUNO") : "")
      String documentCode = (String) (mi.in.get("DOID") != null ? mi.in.get("DOID") : "")

      ExpressionFactory ext035Expression = database.getExpressionFactory("EXT035")
      ext035Expression = ext035Expression.ge("EXZCOD", constraintCode)
        .and(ext035Expression.ge("EXCSCD", countryCode))
        .and(ext035Expression.ge("EXCUNO", customerCode))
        .and(ext035Expression.ge("EXDOID", documentCode))

      DBAction ext035Query = database.table("EXT035").index("00")
        .matching(ext035Expression)
        .selection("EXCONO", "EXZCOD", "EXCSCD", "EXCUNO", "EXDOID", "EXADS1", "EXRGDT", "EXRGTM", "EXLMDT", "EXCHNO", "EXCHID").build()
      DBContainer ext035Request = ext035Query.getContainer()
      ext035Request.set("EXCONO", currentCompany)
      if (!ext035Query.readAll(ext035Request, 1, ext035Reader)) {
        mi.error("L'enregistrement n'existe pas")
        return
      }
    }
  }

  Closure<?> ext035Reader = { DBContainer ext035Result ->
    String constraintCode = ext035Result.get("EXZCOD")
    String countryCode = ext035Result.get("EXCSCD")
    String customerCode = ext035Result.get("EXCUNO")
    String documentCode = ext035Result.get("EXDOID")
    String documentCodeType = ext035Result.get("EXADS1")
    String entryDate = ext035Result.get("EXRGDT")
    String entryTime = ext035Result.get("EXRGTM")
    String changeDate = ext035Result.get("EXLMDT")
    String changeNumber = ext035Result.get("EXCHNO")
    String changedBy = ext035Result.get("EXCHID")
    String company = ext035Result.get("EXCONO")
    mi.outData.put("ZCOD", constraintCode)
    mi.outData.put("CSCD", countryCode)
    mi.outData.put("CUNO", customerCode)
    mi.outData.put("DOID", documentCode)
    mi.outData.put("ADS1", documentCodeType)
    mi.outData.put("RGDT", entryDate)
    mi.outData.put("RGTM", entryTime)
    mi.outData.put("LMDT", changeDate)
    mi.outData.put("CHNO", changeNumber)
    mi.outData.put("CHID", changedBy)
    mi.outData.put("CONO", company)
    mi.write()
  }
}
