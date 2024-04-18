/**
 * README
 * This extension is used by Mashup
 * 
 * Name : EXT031MI.DelConstrType
 * Description : Delete records from the EXT031 table.
 * Date         Changed By   Description
 * 20230201     SEAR         QUAX01 - Constraints matrix 
 */
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit

public class DelConstrType extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final ProgramAPI program

  public DelConstrType(MIAPI mi, DatabaseAPI database, ProgramAPI program) {
    this.mi = mi
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
    DBAction query = database.table("EXT031").index("00").build()
    DBContainer EXT031 = query.getContainer()
    EXT031.set("EXCONO", currentCompany)
    EXT031.set("EXZCTY", mi.in.get("ZCTY"))
    if(!query.readLock(EXT031, updateCallBack)){
      mi.error("L'enregistrement n'existe pas")
      return
    }
  }
  Closure<?> updateCallBack = { LockedResult lockedResult ->
    lockedResult.delete()
  }
}

