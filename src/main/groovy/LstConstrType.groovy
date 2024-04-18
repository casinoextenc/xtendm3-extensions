/**
 * README
 * This extension is used by Mashup
 * 
 * Name : EXT031MI.LstConstrType
 * Description : List records from the EXT031 table.
 * Date         Changed By   Description
 * 20210125     SEAR         QUAX01 - Constraints matrix 
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit

public class LstConstrType extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program

  public LstConstrType(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program) {
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
    if (mi.in.get("ZCTY") == null) {
      DBAction query = database.table("EXT031").index("00").selection("EXZCTY", "EXZTYP", "EXRGDT", "EXRGTM", "EXLMDT", "EXCHNO", "EXCHID").build()
      DBContainer EXT031 = query.getContainer()
      EXT031.set("EXCONO", currentCompany)
      if(!query.readAll(EXT031, 1, outData)){
        mi.error("L'enregistrement n'existe pas")
        return
      }
    } else {
      String constraintType = mi.in.get("ZCTY")
      ExpressionFactory expression = database.getExpressionFactory("EXT031")
      expression = expression.ge("EXZCTY", constraintType)
      DBAction query = database.table("EXT031").index("00").matching(expression).selection("EXZCTY", "EXZTYP", "EXRGDT", "EXRGTM", "EXLMDT", "EXCHNO", "EXCHID").build()
      DBContainer EXT031 = query.getContainer()
      EXT031.set("EXCONO", currentCompany)
      if(!query.readAll(EXT031, 1, outData)){
        mi.error("L'enregistrement n'existe pas")
        return
      }
    }
  }

  Closure<?> outData = { DBContainer EXT031 ->
    String constraintType = EXT031.get("EXZCTY")
    String description = EXT031.get("EXZTYP")
    String entryDate = EXT031.get("EXRGDT")
    String entryTime = EXT031.get("EXRGTM")
    String changeDate = EXT031.get("EXLMDT")
    String changeNumber = EXT031.get("EXCHNO")
    String changedBy = EXT031.get("EXCHID")
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
