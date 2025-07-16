/**
 * README
 * This extension is used by Mashup
 *
 * Name : EXT875MI.PurLog
 * Description : Purge records from the EXT875 table.
 * Date         Changed By   Description
 * 20241122     FLEBARS      Log handling
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class PurLog extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program
  private final UtilityAPI utility
  private Integer currentCompany

  public PurLog(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility) {
    this.mi = mi
    this.database = database
    this.logger = logger
    this.program = program
    this.utility = utility
  }

  public void main() {
    String rfid = ""
    String jbnm = ""
    Integer levl = 0
    Integer fvdt = 0
    Integer fvtm = 0
    Integer tvdt = 0
    Integer tvtm = 0

    if (mi.in.get("CONO") != null) {
      currentCompany = (Integer) mi.in.get("CONO")
    } else {
      currentCompany = 0
    }

    if (mi.in.get("RFID") != null) {
      rfid = mi.in.get("RFID")
    }

    if (mi.in.get("FVDT") != null) {
      fvdt = mi.in.get("FVDT") as Integer
    }

    if (mi.in.get("TVDT") != null) {
      tvdt = mi.in.get("TVDT") as Integer
    }

    if (mi.in.get("FVTM") != null) {
      fvtm = mi.in.get("FVTM") as Integer
    }

    if (mi.in.get("TVTM") != null) {
      tvtm = mi.in.get("TVTM") as Integer
    }

    if (mi.in.get("LEVL") != null) {
      levl = mi.in.get("LEVL") as Integer
    }

    if (mi.in.get("JBNM") != null) {
      jbnm = mi.in.get("JBNM")
    }

    //Create Expression
    ExpressionFactory ext875Expression = database.getExpressionFactory("EXT875")
    ext875Expression = ext875Expression.eq("EXCONO", currentCompany.toString())

    if (rfid != "") {
      ext875Expression = ext875Expression.and(ext875Expression.eq("EXRFID", rfid))
    }
    if (fvdt >0) {
      ext875Expression = ext875Expression.and(ext875Expression.ge("EXRGDT", fvdt.toString()))
    }
    if (tvdt > 0) {
      ext875Expression = ext875Expression.and(ext875Expression.le("EXRGDT", tvdt.toString()))
    }

    if (fvtm > 0) {
      ext875Expression = ext875Expression.and(ext875Expression.ge("EXRGTM", fvtm.toString()))
    }
    if (tvtm > 0) {
      ext875Expression = ext875Expression.and(ext875Expression.le("EXRGTM", tvtm.toString()))
    }

    if (jbnm != "") {
      ext875Expression = ext875Expression.and(ext875Expression.eq("EXJBNM", jbnm))
    }
    if (levl >0) {
      ext875Expression = ext875Expression.and(ext875Expression.eq("EXLEVL", levl.toString()))
    }


    DBAction ext875Query = database.table("EXT875").index("00").matching(ext875Expression).selection("EXRFID").build()
    DBContainer ext875Request = ext875Query.getContainer()
    ext875Request.set("EXCONO", currentCompany )

    //Update closure
    Closure<?> ext875Updater = { LockedResult ext875LockedResult ->
      ext875LockedResult.delete()
    }
    //Read closure
    Closure<?> ext875Reader = { DBContainer ext875Result ->
      ext875Query.readLock(ext875Result, ext875Updater)
    }

    //Loop on records
    if (!ext875Query.readAll(ext875Request, 1, 10000,ext875Reader)) {
      mi.error("L'enregistrement n'existe pas")
      return
    }
  }
}
