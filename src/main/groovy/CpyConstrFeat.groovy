/**
 * README
 * This extension is used by Mashup
 * 
 * Name : EXT033MI.CpyConstrFeat
 * Description : Copy records to the EXT033 table.
 * Date         Changed By   Description
 * 20230125     SEAR         QUAX01 - Constraints matrix 
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class CpyConstrFeat extends ExtendM3Transaction {
  private final MIAPI mi
  private final LoggerAPI logger
  private final ProgramAPI program
  private final DatabaseAPI database
  private final SessionAPI session
  private final TransactionAPI transaction

  public CpyConstrFeat(MIAPI mi, DatabaseAPI database, ProgramAPI program) {
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
    LocalDateTime timeOfCreation = LocalDateTime.now()
    DBAction query = database.table("EXT033").index("00").selection("EXZDES").build()
    DBContainer EXT033 = query.getContainer()
    EXT033.set("EXCONO", currentCompany)
    EXT033.set("EXZCAR", mi.in.get("ZCAR"))
    if(query.read(EXT033)){
      EXT033.set("EXZCAR", mi.in.get("CZCA"))
      if (!query.read(EXT033)) {
        EXT033.setInt("EXRGDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
        EXT033.setInt("EXRGTM", timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")) as Integer)
        EXT033.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
        EXT033.setInt("EXCHNO", 1)
        EXT033.set("EXCHID", program.getUser())
        query.insert(EXT033)
      } else {
        mi.error("L'enregistrement existe déjà")
      }
    } else {
      mi.error("L'enregistrement n'existe pas")
      return
    }
  }
}
