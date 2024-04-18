/**
 * README
 * This extension is used by Mashup
 *
 * Name : EXT030MI.DelConstraint
 * Description : Delete records from the EXT030 table.
 * Date         Changed By   Description
 * 20230210     SEAR         QUAX01 - Constraints matrix
 */

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class DelConstraint extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final ProgramAPI program

  public DelConstraint(MIAPI mi, DatabaseAPI database, ProgramAPI program) {
    this.mi = mi
    this.database = database
    this.program = program
  }

  public void main() {
    Integer currentCompany
    int zcid = (mi.in.get("ZCID") != null ? (Integer)mi.in.get("ZCID") : 0)
    if (mi.in.get("CONO") == null) {
      currentCompany = (Integer)program.getLDAZD().CONO
    } else {
      currentCompany = mi.in.get("CONO")
    }
    DBAction query = database.table("EXT030").index("00").build()
    DBContainer EXT030 = query.getContainer()
    EXT030.set("EXCONO", currentCompany)
    EXT030.set("EXZCID", zcid)
    if(!query.readLock(EXT030, updateCallBack)){
      mi.error("L'enregistrement n'existe pas")
      return
    }
  }
  Closure<?> updateCallBack = { LockedResult lockedResult ->
    lockedResult.delete()
  }
}