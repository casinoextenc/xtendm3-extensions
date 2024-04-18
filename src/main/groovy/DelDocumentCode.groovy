/**
 * README
 * This extension is used by Mashup
 * 
 * Name : EXT035MI.DelDocumentCode
 * Description : Delete records from the EXT035 table.
 * Date         Changed By   Description
 * 20230201     SEAR         QUAX01 - Constraints matrix 
 */
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit

public class DelDocumentCode extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final ProgramAPI program

  public DelDocumentCode(MIAPI mi, DatabaseAPI database, ProgramAPI program) {
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
    DBAction query = database.table("EXT035").index("00").build()
    DBContainer EXT035 = query.getContainer()
    EXT035.set("EXCONO", currentCompany)
    EXT035.set("EXZCOD",  mi.in.get("ZCOD"))
    EXT035.set("EXCUNO", mi.in.get("CUNO"))
    EXT035.set("EXCSCD", mi.in.get("CSCD"))
    EXT035.set("EXDOID", mi.in.get("DOID"))
    if(!query.readLock(EXT035, updateCallBack)){
      mi.error("L'enregistrement n'existe pas")
      return
    }
  }
  Closure<?> updateCallBack = { LockedResult lockedResult ->
    lockedResult.delete()
  }
}

