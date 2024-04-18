/**
* README
* This extension is used by Mashup
* 
* Name : EXT033MI.UpdConstrFeat
* Description : Update records from the EXT033 table.
* Date         Changed By   Description
* 20210125     SEAR         QUAX01 - Constraints matrix 
*/
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class UpdConstrFeat extends ExtendM3Transaction {
  private final MIAPI mi;
  private final LoggerAPI logger;
  private final ProgramAPI program
  private final DatabaseAPI database;
  private final SessionAPI session;
  private final TransactionAPI transaction

  public UpdConstrFeat(MIAPI mi, DatabaseAPI database, ProgramAPI program) {
    this.mi = mi;
    this.database = database
    this.program = program
  }

  public void main() {
    Integer currentCompany
    if (mi.in.get("CONO") == null) {
      currentCompany = (Integer)program.getLDAZD().CONO
    } else {
      currentCompany = mi.in.get("CONO")
    }
    DBAction query = database.table("EXT033").index("00").selection("EXCONO", "EXZCAR", "EXZDES").build()
    DBContainer EXT033 = query.getContainer()
    EXT033.set("EXCONO", currentCompany)
    EXT033.set("EXZCAR", mi.in.get("ZCAR"))
    if(!query.readLock(EXT033, updateCallBack)){
      mi.error("L'enregistrement n'existe pas")
      return
    }
  }
  Closure<?> updateCallBack = { LockedResult lockedResult ->
    LocalDateTime timeOfCreation = LocalDateTime.now()
    int changeNumber = lockedResult.get("EXCHNO")
    if (mi.in.get("ZDES") != null)
      lockedResult.set("EXZDES", mi.in.get("ZDES"))
    lockedResult.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
    lockedResult.setInt("EXCHNO", changeNumber + 1)
    lockedResult.set("EXCHID", program.getUser())
    lockedResult.update()
  }
}
