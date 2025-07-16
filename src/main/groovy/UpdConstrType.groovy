/**
 * README
 * This extension is used by Mashup
 * QUAX01 Gestion du référentiel qualité
 * Name : EXT031MI.UpdConstrType
 * Description : Update records from the EXT031 table.
 * Date         Changed By   Description
 * 20210125     SEAR         QUAX01 - Constraints matrix
 * 20240605     FLEBARS      QUAX01 - Controle code pour validation Infor
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class UpdConstrType extends ExtendM3Transaction {
  private final MIAPI mi
  private final LoggerAPI logger
  private final ProgramAPI program
  private final DatabaseAPI database

  private int currentCompany

  public UpdConstrType(MIAPI mi, DatabaseAPI database, ProgramAPI program, LoggerAPI logger) {
    this.mi = mi
    this.database = database
    this.program = program
    this.logger = logger
  }

  public void main() {
    if (mi.in.get("CONO") == null) {
      currentCompany = (Integer) program.getLDAZD().CONO
    } else {
      currentCompany = mi.in.get("CONO")
    }
    DBAction ext031Query = database.table("EXT031").index("00").selection("EXCONO", "EXZCTY", "EXZTYP").build()
    DBContainer ext031Request = ext031Query.getContainer()
    ext031Request.set("EXCONO", currentCompany)
    ext031Request.set("EXZCTY", mi.in.get("ZCTY"))
    if (!ext031Query.readLock(ext031Request, ext031Updater)) {
      mi.error("L'enregistrement n'existe pas")
    }
  }
  Closure<?> ext031Updater = { LockedResult ext031LockedResult ->
    LocalDateTime timeOfCreation = LocalDateTime.now()
    int changeNumber = ext031LockedResult.get("EXCHNO")
    if (mi.in.get("ZTYP") != null)
      ext031LockedResult.set("EXZTYP", mi.in.get("ZTYP"))
    ext031LockedResult.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
    ext031LockedResult.setInt("EXCHNO", changeNumber + 1)
    ext031LockedResult.set("EXCHID", program.getUser())
    ext031LockedResult.update()
  }
}
