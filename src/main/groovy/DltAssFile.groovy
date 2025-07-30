import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/**
 * Name : EXT105MI.DltAssFile
 *
 * Description :
 * Delete record in EXT105
 *
 * Date         Changed By    Version   Description
 * 20250729     FLEBARS       1.0       Creation
 */

public class DltAssFile extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program
  private final UtilityAPI utility

  private int currentCompany
  private String currentDate


  public DltAssFile(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility) {
    this.mi = mi
    this.database = database
    this.logger = logger
    this.program = program
    this.utility = utility
  }

  public void main() {
    currentCompany = (int) program.getLDAZD().CONO
    LocalDateTime timeOfCreation = LocalDateTime.now()
    currentDate = timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd"))
    boolean errorFlag = false
    String errorMessage = ""

    String cuno = (String) mi.in.get("CUNO")
    String trdt = (String) mi.in.get("TRDT")
    String file = (String) mi.in.get("FILE")


    DBAction ext105Query = database.table("EXT105").index("00").build()
    DBContainer ext105Request = ext105Query.getContainer()
    ext105Request.set("EXCONO", currentCompany)
    ext105Request.set("EXCUNO", cuno)
    ext105Request.set("EXTRDT", trdt as Integer)
    ext105Request.set("EXFILE", file)
    if (!ext105Query.readLock(ext105Request, { LockedResult ext105LockedRecord ->
      ext105LockedRecord.delete()
    })) {
      mi.error("L'enregistrement n'existe pas")
      return
    }

  }


}
