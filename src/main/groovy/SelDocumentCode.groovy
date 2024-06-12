/**
 * README
 * This extension is used by Mashup
 *
 * Name : EXT035MI.SelDocumentCode
 * Description : select records from the EXT035 table.
 * Date         Changed By   Description
 * 20210125     SEAR         QUAX01 - Constraints matrix
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit

public class SelDocumentCode extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program
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
    Integer currentCompany
    if (mi.in.get("CONO") == null) {
      currentCompany = (Integer)program.getLDAZD().CONO
    } else {
      currentCompany = mi.in.get("CONO")
    }
    nbli = (mi.in.get("NBLI") != null ? (Integer)mi.in.get("NBLI") : 0)
    nbsl = (mi.in.get("NBSL") != null ? (Integer)mi.in.get("NBSL") : 50)

    // maxsel to return
    maxSel = nbli + nbsl

    String constraintCode =  (String)(mi.in.get("ZCOD") != null ? mi.in.get("ZCOD") : "")
    String countryCode =  (String)(mi.in.get("CSCD") != null ? mi.in.get("CSCD") : "")
    String customerCode = (String)(mi.in.get("CUNO") != null ? mi.in.get("CUNO") : "")
    String documentCode = (String)(mi.in.get("DOID") != null ? mi.in.get("DOID") : "")

    ExpressionFactory expression = database.getExpressionFactory("EXT035")

    int countExpression = 0

    if (constraintCode.length() > 0) {
      expression = expression.eq("EXZCOD", constraintCode)
      countExpression++
    }

    if (countryCode.length() > 0) {
      if (countExpression == 0) {
        expression = expression.eq("EXCSCD", countryCode)
      } else {
        expression = expression.and(expression.eq("EXCSCD", countryCode))
      }
      countExpression++
    }

    if (customerCode.length() > 0) {
      if (countExpression == 0) {
        expression = expression.eq("EXCUNO", customerCode)
      } else {
        expression = expression.and(expression.eq("EXCUNO", customerCode))
      }
      countExpression++
    }

    if (documentCode.length() > 0) {
      if (countExpression == 0) {
        expression = expression.eq("EXDOID", documentCode)
      } else {
        expression = expression.and(expression.eq("EXDOID", documentCode))
      }
      countExpression++
    }

    DBAction query = database.table("EXT035").index("00")
        .matching(expression)
        .selection("EXCONO", "EXZCOD", "EXCSCD", "EXCUNO", "EXDOID", "EXADS1", "EXRGDT", "EXRGTM", "EXLMDT", "EXCHNO", "EXCHID").build()
    DBContainer EXT035 = query.getContainer()
    EXT035.set("EXCONO", currentCompany)
    if(!query.readAll(EXT035, 1, outData)){
      // mi.error("L'enregistrement n'existe pas")
      return
    }
  }

  Closure<?> outData = { DBContainer EXT035 ->
    countLine++
    if (countLine > nbli && countLine <= maxSel) {
      String constraintCode = EXT035.get("EXZCOD")
      String countryCode = EXT035.get("EXCSCD")
      String customerCode = EXT035.get("EXCUNO")
      String documentCode = EXT035.get("EXDOID")
      String documentCodeType = EXT035.get("EXADS1")
      String entryDate = EXT035.get("EXRGDT")
      String entryTime = EXT035.get("EXRGTM")
      String changeDate = EXT035.get("EXLMDT")
      String changeNumber = EXT035.get("EXCHNO")
      String changedBy = EXT035.get("EXCHID")
      String company = EXT035.get("EXCONO")
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