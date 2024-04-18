/**
* README
* This extension is used by Mashup
* 
* Name : EXT033MI.DelConstrFeat
* Description : Delete records from the EXT033 table.
* Date         Changed By   Description
* 20230201     SEAR	        QUAX01 - Constraints matrix 
*/
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit

public class DelConstrFeat extends ExtendM3Transaction {
  private final MIAPI mi;
  private final DatabaseAPI database
  private final ProgramAPI program

  public DelConstrFeat(MIAPI mi, DatabaseAPI database, ProgramAPI program) {
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
    DBAction query = database.table("EXT033").index("00").build()
    DBContainer EXT033 = query.getContainer()
    EXT033.set("EXCONO", currentCompany)
    EXT033.set("EXZCAR", mi.in.get("ZCAR"))
    if(!query.readLock(EXT033, updateCallBack)){
      mi.error("L'enregistrement n'existe pas")
      return
    }
  }
  Closure<?> updateCallBack = { LockedResult lockedResult ->
    lockedResult.delete()
  }
}

