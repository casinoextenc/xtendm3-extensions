import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/**
 * Name : EXT105MI.UpdAssFile
 *
 * Description :
 * Update existing record in EXT105
 *
 * Date         Changed By    Version   Description
 * 20250729     FLEBARS       1.0       Creation
 */
public class UpdAssFile extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program
  private final UtilityAPI utility

  private int currentCompany
  private String currentDate

  public UpdAssFile(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility) {
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

    String cuno = (String) mi.in.get("CUNO")
    String trdt = (String) mi.in.get("TRDT")
    String file = (String) mi.in.get("FILE")

    long mlid = mi.in.get("MLID") == null ? 0l : mi.in.get("MLID") as Long


    DBAction ext105Query = database.table("EXT105").index("00").build()
    DBContainer ext105Request = ext105Query.getContainer()
    ext105Request.set("EXCONO", currentCompany)
    ext105Request.set("EXCUNO", cuno)
    ext105Request.set("EXTRDT", trdt as Integer)
    ext105Request.set("EXFILE", file)
    if (!ext105Query.read(ext105Request)) {
      mi.error("Enregistrement n'existe pas")
      return
    }

    if (mlid != 0l) {
      DBAction cmailbQuery = database.table("CMAILB").index("00").build()
      DBContainer cmailbRequest = cmailbQuery.getContainer()
      cmailbRequest.set("CBCONO", currentCompany)
      cmailbRequest.set("CBMLID", mlid)
      if (!cmailbQuery.read(cmailbRequest)) {
        mi.error("Enregistrement mail n'existe pas dans CRS420")
        return
      }
    }

    if (ext105Query.readLock(ext105Request, { LockedResult ext105LockedRecord ->
      if (mi.in.get("NBLN") != null)
        ext105LockedRecord.setInt("EXNBLN", mi.in.get("NBLN") as Integer)
      if (mi.in.get("NBAN") != null)
        ext105LockedRecord.setInt("EXNBAN", mi.in.get("NBAN") as Integer)
      if (mi.in.get("NBSU") != null)
        ext105LockedRecord.setInt("EXNBSU", mi.in.get("NBSU") as Integer)
      if (mi.in.get("NBAS") != null)
        ext105LockedRecord.setInt("EXNBAS", mi.in.get("NBAS") as Integer)
      if (mi.in.get("NBCA") != null)
        ext105LockedRecord.setInt("EXNBCA", mi.in.get("NBCA") as Integer)
      if (mi.in.get("STAT") != null)
        ext105LockedRecord.set("EXSTAT", mi.in.get("STAT") as String)
      if (mi.in.get("TXER") != null)
        ext105LockedRecord.set("EXTXER", mi.in.get("TXER") as String)
      if (mi.in.get("MLID") != null)
        ext105LockedRecord.setLong("EXMLID", mi.in.get("MLID") as Long)


      ext105LockedRecord.setInt("EXCHNO", ((Integer) ext105LockedRecord.get("EXCHNO") + 1))
      ext105Request.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      ext105LockedRecord.set("EXCHID", program.getUser())
      ext105LockedRecord.update()
    })) {

    }


  }
}
