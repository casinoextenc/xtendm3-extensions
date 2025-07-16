/**
 * README
 * This extension is used by Mashup
 *
 * Name : EXT820MI.DelSchedSetting
 * Description : Delete records from the EXT820 table.
 * Date         Changed By   Description
 * 20211130     RENARN       INTX99 Batch submission
 */
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit

public class DelSchedSetting extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final ProgramAPI program

  public DelSchedSetting(MIAPI mi, DatabaseAPI database, ProgramAPI program) {
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
    DBAction query = database.table("EXT820").index("00").build()
    DBContainer EXT820 = query.getContainer()
    EXT820.set("EXCONO", currentCompany)
    EXT820.set("EXJOID", mi.in.get("JOID"))
    EXT820.set("EXUSID", mi.in.get("USID"))
    EXT820.set("EXXVSN", mi.in.get("XVSN"))
    if(!query.readLock(EXT820, updateCallBack)){
      mi.error("L'enregistrement n'existe pas")
      return
    }
  }
  // Delete EXT820
  Closure<?> updateCallBack = { LockedResult lockedResult ->
    lockedResult.delete()
  }
}
