/**
* README
* This extension is used by Mashup
* 
* Name : EXT034MI.LstCodification
* Description : List records from the EXT034 table.
* Date         Changed By   Description
* 20210125     SEAR         QUAX01 - Constraints matrix 
* 20230620     FLEBARS      QUAX01 - evol contrainte 
*/
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit

public class LstCodification extends ExtendM3Transaction {
  private final MIAPI mi;
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program

  public LstCodification(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program) {
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
    if (mi.in.get("ZCOD") == null) {
      DBAction query = database.table("EXT034").index("00").selection(
        "EXZCOD", 
        "EXZDES", 
        "EXZCTY", 
        "EXRGDT", 
        "EXRGTM", 
        "EXLMDT", 
        "EXCHNO", 
        "EXZSTY", 
        "EXCHID").build()
      DBContainer EXT034 = query.getContainer()
      EXT034.set("EXCONO", currentCompany)
      if(!query.readAll(EXT034, 1, outData)){
        mi.error("L'enregistrement n'existe pas")
        return
      }
    } else {
      String codification = mi.in.get("ZCOD")
      ExpressionFactory expression = database.getExpressionFactory("EXT034")
      expression = expression.ge("EXZCOD", codification)
      DBAction query = database.table("EXT034").index("00").matching(expression).selection(
        "EXZCOD", 
        "EXZDES", 
        "EXZCTY", 
        "EXRGDT", 
        "EXRGTM", 
        "EXLMDT", 
        "EXCHNO", 
        "EXZSTY", 
        "EXCHID").build()
      DBContainer EXT034 = query.getContainer()
      EXT034.set("EXCONO", currentCompany)
      if(!query.readAll(EXT034, 1, outData)){
        mi.error("L'enregistrement n'existe pas")
        return
      }
    }
  }

  Closure<?> outData = { DBContainer EXT034 ->
    String codification = EXT034.get("EXZCOD")
    String description = EXT034.get("EXZDES")
    String constraintType = EXT034.get("EXZCTY")
    String entryDate = EXT034.get("EXRGDT")
    String entryTime = EXT034.get("EXRGTM")
    String changeDate = EXT034.get("EXLMDT")
    String changeNumber = EXT034.get("EXCHNO")
    String changedBy = EXT034.get("EXCHID")
    mi.outData.put("ZCOD", codification)
    mi.outData.put("ZDES", description)
    mi.outData.put("ZCTY", constraintType)
    mi.outData.put("RGDT", entryDate)
    mi.outData.put("RGTM", entryTime)
    mi.outData.put("LMDT", changeDate)
    mi.outData.put("CHNO", changeNumber)
    mi.outData.put("CHID", changedBy)
    mi.outData.put("ZSTY", EXT034.get("EXZSTY") as String)
    mi.write()
  }
}
