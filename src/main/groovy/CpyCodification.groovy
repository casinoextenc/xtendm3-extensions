/**
 * README
 * This extension is used by Mashup
 * 
 * Name : EXT034MI.CpyCodification
 * Description : Copy records to the EXT034 table.
 * Date         Changed By   Description
 * 20230125     SEAR         QUAX01 - Constraints matrix 
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class CpyCodification extends ExtendM3Transaction {
  private final MIAPI mi
  private final LoggerAPI logger
  private final ProgramAPI program
  private final DatabaseAPI database
  private final SessionAPI session
  private final TransactionAPI transaction

  public CpyCodification(MIAPI mi, DatabaseAPI database, ProgramAPI program) {
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
    DBAction query = database.table("EXT034").index("00").selection("EXZDES", "EXZCTY").build()
    DBContainer EXT034 = query.getContainer()
    EXT034.set("EXCONO", currentCompany)
    EXT034.set("EXZCOD", mi.in.get("ZCOD"))
    if(query.read(EXT034)){
      EXT034.set("EXZCOD", mi.in.get("CZCO"))
      if (!query.read(EXT034)) {
        EXT034.setInt("EXRGDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
        EXT034.setInt("EXRGTM", timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")) as Integer)
        EXT034.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
        EXT034.setInt("EXCHNO", 1)
        EXT034.set("EXCHID", program.getUser())
        query.insert(EXT034)
      } else {
        mi.error("L'enregistrement existe déjà")
      }
    } else {
      mi.error("L'enregistrement n'existe pas")
      return
    }
  }
}
