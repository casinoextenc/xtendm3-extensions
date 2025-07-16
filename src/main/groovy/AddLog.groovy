/**
 * README
 * This extension is used by Mashup
 *
 * Name : EXT875MI.AddLog
 * Description : Add records to the EXT875 table.
 * Date         Changed By   Description
 * 20241122     FLEBARS       Log handling
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.ZoneOffset

public class AddLog extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program
  private final UtilityAPI utility
  private Integer currentCompany

  public AddLog(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility) {
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
    Integer levl = 1

    LocalDateTime timeOfCreation = LocalDateTime.now()
    Long lmts = timeOfCreation.toInstant(ZoneOffset.UTC).toEpochMilli()
    Integer rgdt = timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer
    Integer rgtm = timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")) as Integer
    Integer chno = 1
    String chid = program.getUser()


    if (mi.in.get("CONO") != null) {
      currentCompany = (Integer) mi.in.get("CONO")
    } else {
      currentCompany = (Integer) program.getLDAZD().CONO
    }

    if (mi.in.get("LEVL") != null) {
      levl = (Integer) mi.in.get("LEVL")
      if (levl <0 || levl >9 ) {
        mi.error(" Level invalide valeur possible 1 à 9")
        return
      }
    }

    if (mi.in.get("RFID") != null) {
      rfid = mi.in.get("RFID")
    } else {
      mi.error(" RFID mandatory")
      return
    }

    if (mi.in.get("JBNM") != null) {
      jbnm = mi.in.get("JBNM")
    } else {
      mi.error(" Job Name mandatory")
      return
    }

    if (mi.in.get("TMSG") != null) {
      tmsg = mi.in.get("TMSG")
    } else {
      mi.error(" Message mandatory")
      return
    }

    DBAction ext875Query = database.table("EXT875").index("00").build()
    DBContainer ext875Request = ext875Query.getContainer()
    ext875Request.set("EXCONO", currentCompany)
    ext875Request.set("EXRFID", rfid)

    if (!ext875Query.read(ext875Request)) {
      ext875Request.set("EXJBNM", jbnm)
      ext875Request.set("EXLEVL", levl)
      ext875Request.set("EXTMSG", tmsg)
      ext875Request.setInt("EXRGDT", rgdt)
      ext875Request.setInt("EXLMDT", rgdt)
      ext875Request.setInt("EXRGTM", rgtm)
      ext875Request.setInt("EXCHNO", chno)
      ext875Request.set("EXCHID", chid)
      ext875Query.insert(ext875Request)
    } else {
      mi.error("Record already exist")
    }
  }
}
