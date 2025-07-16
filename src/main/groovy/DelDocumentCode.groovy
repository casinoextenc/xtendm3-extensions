/**
 * README
 * This extension is used by Mashup
 * QUAX01 Gestion du référentiel qualité
 * Name : EXT035MI.DelDocumentCode
 * Description : Delete records from the EXT035 table.
 * Date         Changed By   Description
 * 20230201     SEAR         QUAX01 - Constraints matrix
 * 20240605     FLEBARS      QUAX01 - Controle code pour validation Infor
 */


public class DelDocumentCode extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final ProgramAPI program
  private final LoggerAPI logger

  private int currentCompany


  public DelDocumentCode(MIAPI mi, DatabaseAPI database, ProgramAPI program, LoggerAPI logger) {
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
    DBAction ext035Query = database.table("EXT035").index("00").build()
    DBContainer ext035Request = ext035Query.getContainer()
    ext035Request.set("EXCONO", currentCompany)
    ext035Request.set("EXZCOD", mi.in.get("ZCOD"))
    ext035Request.set("EXCUNO", mi.in.get("CUNO"))
    ext035Request.set("EXCSCD", mi.in.get("CSCD"))
    ext035Request.set("EXDOID", mi.in.get("DOID"))
    Closure<?> ext035Updater = { LockedResult ext035LockedResult ->
      ext035LockedResult.delete()
    }
    if (!ext035Query.readLock(ext035Request, ext035Updater)) {
      mi.error("L'enregistrement n'existe pas")
    }
  }
}

