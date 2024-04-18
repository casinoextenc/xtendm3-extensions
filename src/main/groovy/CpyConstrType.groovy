/**
 * README
 * This extension is used by Mashup
 * 
 * Name : EXT031MI.CpyConstrType
 * Description : Copy records to the EXT031 table.
 * Date         Changed By   Description
 * 20230125     SEAR         QUAX01 - Constraints matrix 
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class CpyConstrType extends ExtendM3Transaction {
  private final MIAPI mi
  private final LoggerAPI logger
  private final ProgramAPI program
  private final DatabaseAPI database
  private final SessionAPI session
  private final TransactionAPI transaction

  public CpyConstrType(MIAPI mi, DatabaseAPI database, ProgramAPI program) {
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
    DBAction query = database.table("EXT031").index("00").selection("EXZTYP").build()
    DBContainer EXT031 = query.getContainer()
    EXT031.set("EXCONO", currentCompany)
    EXT031.set("EXZCTY", mi.in.get("ZCTY"))
    if(query.read(EXT031)){
      EXT031.set("EXZCTY", mi.in.get("CZCT"))
      if (!query.read(EXT031)) {
        EXT031.setInt("EXRGDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
        EXT031.setInt("EXRGTM", timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")) as Integer)
        EXT031.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
        EXT031.setInt("EXCHNO", 1)
        EXT031.set("EXCHID", program.getUser())
        query.insert(EXT031)
      } else {
        mi.error("L'enregistrement existe déjà")
      }
    } else {
      mi.error("L'enregistrement n'existe pas")
      return
    }
  }
}
