/**
* README
* This extension is used by Mashup
* 
* Name : EXT034MI.DelCodification
* Description : Delete records from the EXT034 table.
* Date         Changed By   Description
* 20230201     SEAR	        QUAX01 - Constraints matrix 
*/
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit

public class DelCodification extends ExtendM3Transaction {
  private final MIAPI mi;
  private final DatabaseAPI database
  private final ProgramAPI program

  public DelCodification(MIAPI mi, DatabaseAPI database, ProgramAPI program) {
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
    DBAction query = database.table("EXT034").index("00").build()
    DBContainer EXT034 = query.getContainer()
    EXT034.set("EXCONO", currentCompany)
    EXT034.set("EXZCOD", mi.in.get("ZCOD"))
    if(!query.readLock(EXT034, updateCallBack)){
      mi.error("L'enregistrement n'existe pas")
      return
    }
  }
  Closure<?> updateCallBack = { LockedResult lockedResult ->
    lockedResult.delete()
  }
}

