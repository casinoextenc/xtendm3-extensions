

/**
 * README
 * This extension is used by Mashup
 * Name : EXT041MI.DelByAscd
 * Description : Delete records from the EXT041 table.
 * Date         Changed By   Description
 * 20230201     SEAR	       COMX02 - Cadencier
 * 20240605     FLEBARS      COMX02 - Cadencier
 * 20250416     ARENARD      The code has been checked
 * 20250610     FLEBARS      Apply xtendm3 remarks
 */
public class DelByAscd extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final ProgramAPI program
  private final LoggerAPI logger

  private int currentCompany

  public DelByAscd(MIAPI mi, DatabaseAPI database, ProgramAPI program, LoggerAPI logger) {
    this.mi = mi
    this.database = database
    this.program = program
    this.logger = logger
  }

  public void main() {
    String cuno =""
    String ascd =""
    if (mi.in.get("CONO") == null) {
      currentCompany = (Integer) program.getLDAZD().CONO
    } else {
      currentCompany = mi.in.get("CONO")
    }

    if (mi.in.get("CUNO") != null) {
      cuno = mi.in.get("CUNO")
    } else {
      mi.error("Code Client obligatoire")
      return
    }

    if (mi.in.get("ASCD") != null) {
      ascd = mi.in.get("ASCD")
    } else {
      ascd =""
    }


    DBAction ext041Query = database.table("EXT041").index("20").build()
    DBContainer ext041Request = ext041Query.getContainer()
    ext041Request.set("EXCONO", currentCompany)
    ext041Request.set("EXCUNO", cuno)
    ext041Request.set("EXASCD", ascd)
    Closure<?> ext041Updater = { LockedResult ext041LockedResult ->
      ext041LockedResult.delete()
    }
    //Read closure
    Closure<?> ext041Reader = { DBContainer ext041Result ->
      ext041Query.readLock(ext041Result, ext041Updater)
    }

    //Loop on records
    if (!ext041Query.readAll(ext041Request, 3, 10000,ext041Reader)) {
      mi.error("L'enregistrement n'existe pas")
      return
    }
  }
}

