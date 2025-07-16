/**
 * README
 * This extension is used by Mashup
 *
 * Name : EXT875MI.GetLog
 * Description : Get records from the EXT875 table.
 * Date         Changed By   Description
 * 20241122     FLEBARS      Log handling
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class GetLog extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program
  private final UtilityAPI utility
  private Integer currentCompany

  public GetLog(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility) {
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
    Integer rgdt = 0
    Integer rgtm = 0

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

    DBAction ext875Query = database.table("EXT875").index("00").selection("EXJBNM", "EXLEVL", "EXTMSG", "EXRGDT", "EXRGTM").build()
    DBContainer ext875Request = ext875Query.getContainer()
    ext875Request.set("EXCONO", currentCompany)
    ext875Request.set("EXRFID", rfid)
    ext875Request.set("EXLMTS", lmts)

    if (!ext875Query.read(ext875Request)) {
      mi.error("L'enregistrement n'existe pas")
      return
    } else {
      mi.outData.put("CONO", (String) ext875Request.get("EXCONO"))
      mi.outData.put("RFID", (String) ext875Request.get("EXRFID"))
      mi.outData.put("LMTS", (String) ext875Request.get("EXLMTS"))
      mi.outData.put("JBNM", (String) ext875Request.get("EXJBNM"))
      mi.outData.put("LEVL", (String) ext875Request.get("EXLEVL"))
      mi.outData.put("TMSG", (String) ext875Request.get("EXTMSG"))
      mi.outData.put("RGDT", (String) ext875Request.get("EXRGDT"))
      mi.outData.put("RGTM", (String) ext875Request.get("EXRGTM"))
    }
  }
}
