/**
 * README
 * This extension is used by Mashup
 *
 * Name : EXT875MI.DelLog
 * Description : Delete records from the EXT875 table.
 * Date         Changed By   Description
 * 20241122     FLEBARS      Log handling
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class DelLog extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program
  private final UtilityAPI utility
  private Integer currentCompany

  public DelLog(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility) {
    this.mi = mi
    this.database = database
    this.logger = logger
    this.program = program
    this.utility = utility
  }

  public void main() {
    String rfid = ""
    Long lmts = 0
    Integer levl = 0

    LocalDateTime timeOfCreation = LocalDateTime.now()

    if (mi.in.get("CONO") != null) {
      currentCompany = (Integer) mi.in.get("CONO")
    } else {
      currentCompany = (Integer) program.getLDAZD().CONO
    }

    if (mi.in.get("RFID") != null) {
      rfid = mi.in.get("RFID")
    } else {
      mi.error(" RFID mandatory")
      return
    }

    if (mi.in.get("LMTS") != null) {
      lmts = mi.in.get("LMTS") as Long
    } else {
      mi.error(" LMTS mandatory")
      return
    }

    DBAction ext875Query = database.table("EXT875").index("00").build()
    DBContainer ext875Request = ext875Query.getContainer()
    ext875Request.set("EXCONO", currentCompany)
    ext875Request.set("EXRFID", rfid)
    ext875Request.set("EXLMTS", lmts)


    Closure<?> ext875Updater = { LockedResult ext875LockedResult ->

      ext875LockedResult.delete()
    }

    if (!ext875Query.readLock(ext875Request, ext875Updater)){
      mi.error("L'enregistrement n'existe pas")
      return

    }
  }
}
