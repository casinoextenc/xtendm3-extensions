/**
* README
* This extension is used by Mashup
* 
* Name : EXT033MI.LstConstrFeat
* Description : List records from the EXT033 table.
* Date         Changed By   Description
* 20210125     SEAR         QUAX01 - Constraints matrix 
*/
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit

public class LstConstrFeat extends ExtendM3Transaction {
  private final MIAPI mi;
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program

  public LstConstrFeat(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program) {
    this.mi = mi;
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
    if (mi.in.get("ZCAR") == null) {
      DBAction query = database.table("EXT033").index("00").selection("EXZCAR", "EXZDES", "EXRGDT", "EXRGTM", "EXLMDT", "EXCHNO", "EXCHID").build()
      DBContainer EXT033 = query.getContainer()
      EXT033.set("EXCONO", currentCompany)
      if(!query.readAll(EXT033, 1, outData)){
        mi.error("L'enregistrement n'existe pas")
        return
      }
    } else {
      String constraintType = mi.in.get("ZCAR")
      ExpressionFactory expression = database.getExpressionFactory("EXT033")
      expression = expression.ge("EXZCAR", constraintType)
      DBAction query = database.table("EXT033").index("00").matching(expression).selection("EXZCAR", "EXZDES", "EXRGDT", "EXRGTM", "EXLMDT", "EXCHNO", "EXCHID").build()
      DBContainer EXT033 = query.getContainer()
      EXT033.set("EXCONO", currentCompany)
      if(!query.readAll(EXT033, 1, outData)){
        mi.error("L'enregistrement n'existe pas")
        return
      }
    }
  }

  Closure<?> outData = { DBContainer EXT033 ->
    String constraintType = EXT033.get("EXZCAR")
    String description = EXT033.get("EXZDES")
    String entryDate = EXT033.get("EXRGDT")
    String entryTime = EXT033.get("EXRGTM")
    String changeDate = EXT033.get("EXLMDT")
    String changeNumber = EXT033.get("EXCHNO")
    String changedBy = EXT033.get("EXCHID")
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
