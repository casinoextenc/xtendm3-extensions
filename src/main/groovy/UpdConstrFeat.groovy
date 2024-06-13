/**
 * README
 * This extension is used by Mashup
 * QUAX01 Gestion du référentiel qualité
 * Name : EXT033MI.UpdConstrFeat
 * Description : Update records from the EXT033 table.
 * Date         Changed By   Description
 * 20210125     SEAR         QUAX01 - Constraints matrix
 * 20240605     FLEBARS      QUAX01 - Controle code pour validation Infor
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class UpdConstrFeat extends ExtendM3Transaction {
  private final MIAPI mi
  private final LoggerAPI logger
  private final ProgramAPI program
  private final DatabaseAPI database

  private int currentCompany

  public UpdConstrFeat(MIAPI mi, DatabaseAPI database, ProgramAPI program, LoggerAPI logger) {
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
    DBAction ext033Query = database.table("EXT033").index("00").selection("EXCONO", "EXZCAR", "EXZDES").build()
    DBContainer ext033Request = ext033Query.getContainer()
    ext033Request.set("EXCONO", currentCompany)
    ext033Request.set("EXZCAR", mi.in.get("ZCAR"))
    if (!ext033Query.readLock(ext033Request, ext033Updater)) {
      mi.error("L'enregistrement n'existe pas")
    }
  }
  Closure<?> ext033Updater = { LockedResult ext033LockedResult ->
    LocalDateTime timeOfCreation = LocalDateTime.now()
    int changeNumber = ext033LockedResult.get("EXCHNO")
    if (mi.in.get("ZDES") != null)
      ext033LockedResult.set("EXZDES", mi.in.get("ZDES"))
    ext033LockedResult.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
    ext033LockedResult.setInt("EXCHNO", changeNumber + 1)
    ext033LockedResult.set("EXCHID", program.getUser())
    ext033LockedResult.update()
  }
}
