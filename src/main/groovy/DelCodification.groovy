/**
 * README
 * This extension is used by Mashup
 * QUAX01 Gestion du référentiel qualité
 * Name : EXT034MI.DelCodification
 * Description : Delete records from the EXT034 table.
 * Date         Changed By   Description
 * 20230201     SEAR	        QUAX01 - Constraints matrix
 * 20240605     FLEBARS      QUAX01 - Controle code pour validation Infor
 */
public class DelCodification extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final ProgramAPI program
  private final LoggerAPI logger

  private int currentCompany

  public DelCodification(MIAPI mi, DatabaseAPI database, ProgramAPI program, LoggerAPI logger) {
    this.mi = mi
    this.database = database
    this.program = program
    this.logger = logger
  }

  public void main() {
    if (mi.in.get("CONO") == null) {
      currentCompany = (Integer) program.getLDAZD().CONO
    } else {
      currentCompany = mi.in.get("CONO")
    }
    DBAction ext034Query = database.table("EXT034").index("00").build()
    DBContainer ext034Request = ext034Query.getContainer()
    ext034Request.set("EXCONO", currentCompany)
    ext034Request.set("EXZCOD", mi.in.get("ZCOD"))
    Closure<?> ext034Updater = { LockedResult ext034LockedResult ->
      ext034LockedResult.delete()
    }
    if (!ext034Query.readLock(ext034Request, ext034Updater)) {
      mi.error("L'enregistrement n'existe pas")
    }
  }
}

