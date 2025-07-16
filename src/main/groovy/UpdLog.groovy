/**
 * README
 * This extension is used by Mashup
 *
 * Name : EXT875MI.UpdLog
 * Description : Update records from the EXT875 table.
 * Date         Changed By   Description
 * 20241122     FLEBARS      Log handling
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class UpdLog extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program
  private final UtilityAPI utility
  private Integer currentCompany

  public UpdLog(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility) {
    this.mi = mi
    this.database = database
    this.logger = logger
    this.program = program
    this.utility = utility
  }

  public void main() {
    String rfid = ""
    String jbnm = ""
    String tmsg = ""
    Long lmts = 0
    Integer levl = 0

    LocalDateTime timeOfCreation = LocalDateTime.now()

    Integer rgdt = timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer
    Integer rgtm = timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")) as Integer
    Integer lmdt = timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer
    Integer chno = 1
    String chid = program.getUser()


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

    if (mi.in.get("LEVL") != null) {
      levl = (Integer) mi.in.get("LEVL")
      if (levl <1 || levl >9 ) {
        mi.error(" Level invalide valeur possible 1 à 9")
        return
      }
    }

    if (mi.in.get("JBNM") != null) {
      jbnm = mi.in.get("JBNM")
    }

    if (mi.in.get("TMSG") != null) {
      tmsg = mi.in.get("TMSG")
    }


    if (mi.in.get("JBNM") == null && mi.in.get("TMSG") == null && mi.in.get("LEVL") == null ) {
      mi.error(" Pas de donnees à mettre à jour")
      return
    }

    DBAction ext875Query = database.table("EXT875").index("00").build()
    DBContainer ext875Request = ext875Query.getContainer()
    ext875Request.set("EXCONO", currentCompany)
    ext875Request.set("EXRFID", rfid)
    ext875Request.set("EXLMTS", lmts)

    if (!ext875Query.read(ext875Request)) {
      mi.error("L'enregistrement n'existe pas")
      return
    }

    Closure<?> ext875Updater = { LockedResult ext875LockedResult ->
      if (mi.in.get("LEVL") != null) {
        ext875LockedResult.set("EXLEVL", levl)
      }

      if (mi.in.get("JBNM") != null) {
        ext875LockedResult.set("EXJBNM", jbnm)
      }
      if (mi.in.get("TMSG") != null) {
        ext875LockedResult.set("EXTMSG", tmsg)
      }
      ext875LockedResult.set("EXCHNO", ((Integer)  ext875LockedResult.get("EXCHNO") + 1))
      ext875LockedResult.set("EXCHID", chid)
      ext875LockedResult.set("EXLMDT", lmdt)
      ext875LockedResult.update()
    }

    ext875Query.readLock(ext875Request, ext875Updater)
  }
}
