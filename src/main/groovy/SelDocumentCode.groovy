/**
 * README
 * This extension is used by Mashup
 * QUAX01 Gestion du référentiel qualité
 * Name : EXT035MI.SelDocumentCode
 * Description : select records from the EXT035 table.
 * Date         Changed By   Description
 * 20210125     SEAR         QUAX01 - Constraints matrix
 * 20240605     FLEBARS      QUAX01 - Controle code pour validation Infor
 */
public class SelDocumentCode extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program

  private int currentCompany
  private int countLine
  private int nbli
  private int nbsl
  private int maxSel

  public SelDocumentCode(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program) {
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
    nbli = (mi.in.get("NBLI") != null ? (Integer) mi.in.get("NBLI") : 0)
    nbsl = (mi.in.get("NBSL") != null ? (Integer) mi.in.get("NBSL") : 50)

    // maxsel to return
    maxSel = nbli + nbsl

    String constraintCode = (String) (mi.in.get("ZCOD") != null ? mi.in.get("ZCOD") : "")
    String countryCode = (String) (mi.in.get("CSCD") != null ? mi.in.get("CSCD") : "")
    String customerCode = (String) (mi.in.get("CUNO") != null ? mi.in.get("CUNO") : "")
    String documentCode = (String) (mi.in.get("DOID") != null ? mi.in.get("DOID") : "")

    ExpressionFactory ext035Expression = database.getExpressionFactory("EXT035")

    int countExpression = 0
    if (constraintCode.length() > 0) {
      ext035Expression = ext035Expression.eq("EXZCOD", constraintCode)
      countExpression++
    }

    if (countryCode.length() > 0) {
      if (countExpression == 0) {
        ext035Expression = ext035Expression.eq("EXCSCD", countryCode)
      } else {
        ext035Expression = ext035Expression.and(ext035Expression.eq("EXCSCD", countryCode))
      }
      countExpression++
    }

    if (customerCode.length() > 0) {
      if (countExpression == 0) {
        ext035Expression = ext035Expression.eq("EXCUNO", customerCode)
      } else {
        ext035Expression = ext035Expression.and(ext035Expression.eq("EXCUNO", customerCode))
      }
      countExpression++
    }

    if (documentCode.length() > 0) {
      if (countExpression == 0) {
        ext035Expression = ext035Expression.eq("EXDOID", documentCode)
      } else {
        ext035Expression = ext035Expression.and(ext035Expression.eq("EXDOID", documentCode))
      }
      countExpression++
    }

    DBAction ext035Query = database.table("EXT035").index("00")
      .matching(ext035Expression)
      .selection("EXCONO", "EXZCOD", "EXCSCD", "EXCUNO", "EXDOID", "EXADS1", "EXRGDT", "EXRGTM", "EXLMDT", "EXCHNO", "EXCHID").build()
    DBContainer ext035Request = ext035Query.getContainer()
    ext035Request.set("EXCONO", currentCompany)
    if (!ext035Query.readAll(ext035Request, 1, maxSel, ext035Reader)) {
      return
    }
  }

  Closure<?> ext035Reader = { DBContainer ext035Result ->
    countLine++
    if (countLine > nbli && countLine <= maxSel) {
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

    if (countLine > maxSel) {
      return
    }
  }
}
