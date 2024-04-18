/**
 * README
 * This extension is used by Mashup
 *
 * Name : EXT035MI.LstDocumentCode
 * Description : List records from the EXT035 table.
 * Date         Changed By   Description
 * 20210125     SEAR         QUAX01 - Constraints matrix
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit

public class LstDocumentCode extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program

  public LstDocumentCode(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program) {
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
    if (mi.in.get("ZCOD") == null) {
      DBAction query = database.table("EXT035").index("00").selection("EXCONO", "EXZCOD", "EXCSCD", "EXCUNO", "EXDOID", "EXADS1", "EXRGDT", "EXRGTM", "EXLMDT", "EXCHNO", "EXCHID").build()
      DBContainer EXT035 = query.getContainer()
      EXT035.set("EXCONO", currentCompany)
      if(!query.readAll(EXT035, 1, outData)){
        mi.error("L'enregistrement n'existe pas")
        return
      }
    } else {
      String constraintCode =  (String)(mi.in.get("ZCOD") != null ? mi.in.get("ZCOD") : "")
      String countryCode =  (String)(mi.in.get("CSCD") != null ? mi.in.get("CSCD") : "")
      String customerCode = (String)(mi.in.get("CUNO") != null ? mi.in.get("CUNO") : "")
      String documentCode = (String)(mi.in.get("DOID") != null ? mi.in.get("DOID") : "")
      logger.debug("constraintcode=${constraintCode}, countryCode=${countryCode}, customerCode=${customerCode}, documentCode=${documentCode}")


      ExpressionFactory expression = database.getExpressionFactory("EXT035")
      expression = expression.ge("EXZCOD", constraintCode)
          .and(expression.ge("EXCSCD", countryCode))
          .and(expression.ge("EXCUNO", customerCode))
          .and(expression.ge("EXDOID", documentCode))
      DBAction query = database.table("EXT035").index("00")
          .matching(expression)
          .selection("EXCONO", "EXZCOD", "EXCSCD", "EXCUNO", "EXDOID", "EXADS1", "EXRGDT", "EXRGTM", "EXLMDT", "EXCHNO", "EXCHID").build()
      DBContainer EXT035 = query.getContainer()
      EXT035.set("EXCONO", currentCompany)
      if(!query.readAll(EXT035, 1, outData)){
        mi.error("L'enregistrement n'existe pas")
        return
      }
    }
  }

  Closure<?> outData = { DBContainer EXT035 ->
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
}
