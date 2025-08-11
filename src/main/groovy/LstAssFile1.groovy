import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/**
 * Name : EXT105MI.LetAssFile
 *
 * Description :
 * List count files by CUNO and TRDT in EXT105
 *
 * Date         Changed By    Version   Description
 * 20250729     FLEBARS       1.0       Creation
 */

public class LstAssFile1 extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program
  private final UtilityAPI utility

  private final String KEY_SEPARATOR = "#"

  private int currentCompany
  private String currentDate


  public LstAssFile1(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility) {
    this.mi = mi
    this.database = database
    this.logger = logger
    this.program = program
    this.utility = utility
  }

  public void main() {
    int nrOfRecords = mi.getMaxRecords() <= 0 || mi.getMaxRecords() >= 10000 ? 10000 : mi.getMaxRecords()

    currentCompany = (int) program.getLDAZD().CONO
    LocalDateTime timeOfCreation = LocalDateTime.now()
    currentDate = timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd"))

    String trdt = mi.in.get("TRDT") == null ? "" : (String) mi.in.get("TRDT")

    boolean doLoop = true
    String tmpCuno = ""
    Map<String, Integer> countMap = new TreeMap<String, Integer>()

    while (doLoop) {
      String cuno = ""


      ExpressionFactory ext105Expression = database.getExpressionFactory("EXT105")
      ext105Expression = ext105Expression.eq("EXCONO", currentCompany.toString())
      if (!tmpCuno.isEmpty())
        ext105Expression = ext105Expression.and(ext105Expression.gt("EXCUNO", tmpCuno))

      DBAction ext105Query = database.table("EXT105")
        .index("01")
        .matching(ext105Expression)
        .selection("EXSTAT")
        .build()
      DBContainer ext105Request = ext105Query.getContainer()
      ext105Request.set("EXCONO", currentCompany)
      ext105Request.set("EXTRDT", trdt as Integer)

      // Read the first record
      if (!ext105Query.readAll(ext105Request, 2, 1, { DBContainer ext105Result ->
        cuno = ext105Result.getString("EXCUNO")
      })) {
        doLoop = false
      }
      tmpCuno = cuno

      ext105Query = database.table("EXT105")
        .index("01")
        .selection("EXSTAT")
        .build()
      ext105Request = ext105Query.getContainer()
      ext105Request.set("EXCONO", currentCompany)
      ext105Request.set("EXTRDT", trdt as Integer)
      ext105Request.set("EXCUNO", cuno)

      if (ext105Query.readAll(ext105Request, 2, nrOfRecords, { DBContainer ext105Result ->
        String stat = ext105Result.getString("EXSTAT")
        logger.debug("Read CUNO: ${cuno}, tmpcuno: ${tmpCuno}, STAT: ${stat} " + ext105Result.getString("EXFILE"))

        String key = cuno + KEY_SEPARATOR + stat
        logger.debug("OK CUNO: ${cuno}, STAT: ${stat}, Key: ${key}")
        if (countMap.containsKey(key)) {
          countMap.put(key, countMap.get(key) + 1)
        } else {
          countMap.put(key, 1)
        }
        return // Stop reading if we have moved to a new CUNO
      })) {
      }
    }
    // IF no records found, return an error
    if (countMap.isEmpty()) {
      mi.error("Aucun enregistrement trouvé pour TRDT: ${trdt}")
      return
    }

    // Loop on count map to send results to MI
    countMap.each { key, value ->
      String[] tb = key.split(KEY_SEPARATOR)
      logger.debug("Reading, Key: ${key} ${tb}")
      mi.outData.put("CUNO", tb[0])
      mi.outData.put("STAT", tb[1])
      mi.outData.put("NBFL", value.toString())
      mi.write()
    }

  }
}
