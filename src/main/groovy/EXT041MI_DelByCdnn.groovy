

/**
 * README
 * This extension is used by Mashup
 * Name : EXT041MI.DelByCdnn
 * Description : Delete records from the EXT041 table.
 * Date         Changed By   Description
 * 20240605     FLEBARS      COMX02 - Cadencier
 * 20250416     ARENARD      The code has been checked

 */
public class DelByCdnn extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final ProgramAPI program
  private final LoggerAPI logger

  private int currentCompany

  public DelByCdnn(MIAPI mi, DatabaseAPI database, ProgramAPI program, LoggerAPI logger) {
    this.mi = mi
    this.database = database
    this.program = program
    this.logger = logger
  }

  public void main() {
    String cuno =""
    String cdnn =""
    if (mi.in.get("CONO") == null) {
      currentCompany = (Integer) program.getLDAZD().CONO
    } else {
      currentCompany = mi.in.get("CONO")
    }

    if (mi.in.get("CUNO") != null) {
      cuno = mi.in.get("CUNO")
    } else {
      mi.error("Code Client obligatoire")
    }

    if (mi.in.get("CDNN") != null) {
      cdnn = mi.in.get("CDNN")
    } else {
      mi.error("Code Cadencier obligatoire")
    }


    DBAction ext041Query = database.table("EXT041").index("00").build()
    DBContainer ext041Request = ext041Query.getContainer()
    ext041Request.set("EXCONO", currentCompany)
    ext041Request.set("EXCUNO", cuno)
    ext041Request.set("EXCDNN", cdnn)
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
    }
  }
}
