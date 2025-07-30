import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/**
 * Name : EXT105MI.LetAssFile
 *
 * Description :
 * List record in EXT105
 *
 * Date         Changed By    Version   Description
 * 20250729     FLEBARS       1.0       Creation
 */

public class LstAssFile extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program
  private final UtilityAPI utility

  private int currentCompany
  private String currentDate


  public LstAssFile(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility) {
    this.mi = mi
    this.database = database
    this.logger = logger
    this.program = program
    this.utility = utility
  }

  public void main() {
    int nrOfRecords = mi.getMaxRecords() <= 0 || mi.getMaxRecords() >= 10000 ? 10000 : mi.getMaxRecords()
    int nrKeyFields = 2

    currentCompany = (int) program.getLDAZD().CONO
    LocalDateTime timeOfCreation = LocalDateTime.now()
    currentDate = timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd"))
    boolean errorFlag = false
    String errorMessage = ""

    String cuno = (String) mi.in.get("CUNO")
    String trdt = mi.in.get("TRDT") == null ? "" : (String) mi.in.get("TRDT")


    DBAction ext105Query = database.table("EXT105")
      .index("00")
      .selection("EXNBLN", "EXNBLN",
        "EXNBAN",
        "EXNBSU",
        "EXNBAS",
        "EXNBCA",
        "EXSTAT",
        "EXTXER",
        "EXMLID")
      .build()
    DBContainer ext105Request = ext105Query.getContainer()
    ext105Request.set("EXCONO", currentCompany)
    ext105Request.set("EXCUNO", cuno)
    if (!trdt.isEmpty()) {
      ext105Request.set("EXTRDT", trdt as Integer)
      nrKeyFields++
    }

    if (!ext105Query.readAll(ext105Request, nrKeyFields, nrOfRecords, { DBContainer ext105Result ->
      mi.outData.put("FILE", ext105Result.get("EXFILE") as String)
      mi.outData.put("TRDT", ext105Result.get("EXTRDT") as String)
      mi.outData.put("NBLN", ext105Result.get("EXNBLN") as String)
      mi.outData.put("NBAN", ext105Result.get("EXNBAN") as String)
      mi.outData.put("NBSU", ext105Result.get("EXNBSU") as String)
      mi.outData.put("NBAS", ext105Result.get("EXNBAS") as String)
      mi.outData.put("NBCA", ext105Result.get("EXNBCA") as String)
      mi.outData.put("STAT", ext105Result.get("EXSTAT") as String)
      mi.outData.put("TXER", ext105Result.get("EXTXER") as String)
      mi.outData.put("MLID", ext105Result.get("EXMLID") as String)
      mi.write()
    })) {
      mi.error("Aucun enregistrement pour la sélection")
      return
    }
  }
}
