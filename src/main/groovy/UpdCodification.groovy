/**
 * README
 * This extension is used by Mashup
 * 
 * Name : EXT034MI.UpdCodification
 * Description : Update records from the EXT034 table.
 * Date         Changed By   Description
 * 20210125     SEAR         QUAX01 - Constraints matrix 
 * 20230620     FLEBARS      QUAX01 - evol contrainte 
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class UpdCodification extends ExtendM3Transaction {
  private final MIAPI mi
  private final LoggerAPI logger
  private final ProgramAPI program
  private final DatabaseAPI database
  private final SessionAPI session
  private final TransactionAPI transaction

  public UpdCodification(MIAPI mi, DatabaseAPI database, ProgramAPI program) {
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

    //Check if record exists in Constraint Type Table (EXT031)
    if (mi.in.get("ZCTY") != null) {
      DBAction query = database.table("EXT031").index("00").build()
      DBContainer EXT031 = query.getContainer()
      EXT031.set("EXCONO", currentCompany)
      EXT031.set("EXZCTY", mi.in.get("ZCTY"))
      if (!query.read(EXT031)) {
        mi.error("Type de contrainte " + mi.in.get("ZCTY") + " n'existe pas")
        return
      }
    }

    DBAction query = database.table("EXT034").index("00").selection("EXCONO", "EXZCOD").build()
    DBContainer EXT034 = query.getContainer()
    EXT034.set("EXCONO", currentCompany)
    EXT034.set("EXZCOD", mi.in.get("ZCOD"))
    if(!query.readLock(EXT034, updateCallBack)){
      mi.error("L'enregistrement n'existe pas")
      return
    }
  }
  Closure<?> updateCallBack = { LockedResult lockedResult ->
    LocalDateTime timeOfCreation = LocalDateTime.now()
    int changeNumber = lockedResult.get("EXCHNO")
    if (mi.in.get("ZDES") != null)
      lockedResult.set("EXZDES", mi.in.get("ZDES"))
    lockedResult.set("EXZCTY", mi.in.get("ZCTY"))
    lockedResult.set("EXZSTY", mi.in.get("ZSTY"))
    lockedResult.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
    lockedResult.setInt("EXCHNO", changeNumber + 1)
    lockedResult.set("EXCHID", program.getUser())
    lockedResult.update()
  }
}
