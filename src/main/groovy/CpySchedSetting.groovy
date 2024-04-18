/**
 * README
 * This extension is used by Mashup
 *
 * Name : EXT820MI.CpySchedSetting
 * Description : Copy records from the EXT820 table.
 * Date         Changed By   Description
 * 20211130     RENARN       INTX99 Batch submission
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class CpySchedSetting extends ExtendM3Transaction {
  private final MIAPI mi;
  private final LoggerAPI logger;
  private final ProgramAPI program
  private final DatabaseAPI database;
  private final SessionAPI session;
  private final TransactionAPI transaction

  public CpySchedSetting(MIAPI mi, DatabaseAPI database, ProgramAPI program) {
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
    LocalDateTime timeOfCreation = LocalDateTime.now()
    DBAction query = database.table("EXT820").index("00").selection("EXJOID","EXUSID","EXXVSN","EXTX30","EXTX60","EXXCAT","EXSCTY","EXXNOW","EXXTOD","EXXNMO","EXXNTU","EXXNWE","EXXNTH","EXXNFR","EXXNSA","EXXNSU","EXXEMO","EXXETU","EXXEWE","EXXETH","EXXEFR","EXXESA","EXXESU","EXXEMT","EXXRDY","EXXJDT","EXXJTM","EXXSTI","EXFDAT","EXTDAT","EXJSCA","EXPETP","EXTIZO","EXRGDT","EXRGTM","EXLMDT","EXCHNO","EXCHID").build()
    DBContainer EXT820 = query.getContainer()
    EXT820.set("EXCONO", currentCompany)
    EXT820.set("EXJOID", mi.in.get("JOID"))
    EXT820.set("EXUSID", mi.in.get("USID"))
    EXT820.set("EXXVSN", mi.in.get("XVSN"))
    if(query.read(EXT820)){
      EXT820.set("EXJOID", mi.in.get("CJOI"))
      EXT820.set("EXUSID", mi.in.get("CUSI"))
      EXT820.set("EXXVSN", mi.in.get("CXVS"))
      if (!query.read(EXT820)) {
        EXT820.setInt("EXRGDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
        EXT820.setInt("EXRGTM", timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")) as Integer)
        EXT820.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
        EXT820.setInt("EXCHNO", 1)
        EXT820.set("EXCHID", program.getUser())
        query.insert(EXT820)
      } else {
        mi.error("L'enregistrement existe déjà")
      }
    } else {
      mi.error("L'enregistrement n'existe pas")
      return
    }
  }
}
