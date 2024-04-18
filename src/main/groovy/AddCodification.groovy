/**
 * README
 * This extension is used by Mashup
 * 
 * Name : EXT034MI.AddCodification
 * Description : Add records to the EXT034 table.
 * Date         Changed By   Description
 * 20230201     SEAR         QUAX01 - Constraints matrix 
 * 20230620     FLEBARS      QUAX01 - evol contrainte 
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class AddCodification extends ExtendM3Transaction {
  private final MIAPI mi
  private final LoggerAPI logger
  private final ProgramAPI program
  private final DatabaseAPI database
  private final SessionAPI session
  private final TransactionAPI transaction

  public AddCodification(MIAPI mi, DatabaseAPI database, ProgramAPI program) {
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

    LocalDateTime timeOfCreation = LocalDateTime.now()
    DBAction query = database.table("EXT034").index("00").build()
    DBContainer EXT034 = query.getContainer()
    EXT034.set("EXCONO", currentCompany)
    EXT034.set("EXZCOD",  mi.in.get("ZCOD"))
    if (!query.read(EXT034)) {
      EXT034.set("EXZDES", mi.in.get("ZDES"))
      EXT034.set("EXZCTY", mi.in.get("ZCTY"))
      EXT034.set("EXZSTY", mi.in.get("ZSTY"))
      EXT034.setInt("EXRGDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      EXT034.setInt("EXRGTM", timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")) as Integer)
      EXT034.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      EXT034.setInt("EXCHNO", 1)
      EXT034.set("EXCHID", program.getUser())
      query.insert(EXT034)
    } else {
      mi.error("L'enregistrement existe déjà")
      return
    }
  }
}