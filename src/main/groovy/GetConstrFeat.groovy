/**
* README
* This extension is used by Mashup
* 
* Name : EXT033MI.GetConstrFeat
* Description : Retrieve records from the EXT033 table.
* Date         Changed By   Description
* 20210125     SEAR         QUAX01 - Constraints matrix 
*/
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit

public class GetConstrFeat extends ExtendM3Transaction {
  private final MIAPI mi;
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program

  public GetConstrFeat(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program) {
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
    DBAction query = database.table("EXT033").index("00").selection("EXZCAR", "EXZDES", "EXRGDT", "EXRGTM", "EXLMDT", "EXCHNO", "EXCHID").build()
    DBContainer EXT033 = query.getContainer()
    EXT033.set("EXCONO", currentCompany)
    EXT033.set("EXZCAR",  mi.in.get("ZCAR"))
    if(!query.readAll(EXT033, 2, outData)){
      mi.error("L'enregistrement n'existe pas")
      return
    }
  }

  Closure<?> outData = { DBContainer EXT033 ->
    String constraintFeat = EXT033.get("EXZCAR")
    String description = EXT033.get("EXZDES")
    String entryDate = EXT033.get("EXRGDT")
    String entryTime = EXT033.get("EXRGTM")
    String changeDate = EXT033.get("EXLMDT")
    String changeNumber = EXT033.get("EXCHNO")
    String changedBy = EXT033.get("EXCHID")
    mi.outData.put("ZCAR", constraintFeat)
    mi.outData.put("ZDES", description)
    mi.outData.put("RGDT", entryDate)
    mi.outData.put("RGTM", entryTime)
    mi.outData.put("LMDT", changeDate)
    mi.outData.put("CHNO", changeNumber)
    mi.outData.put("CHID", changedBy)
    mi.write()
  }
}

