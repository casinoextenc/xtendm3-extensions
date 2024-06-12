/**
 * README
 * This extension is used by Mashup
 * QUAX01 Gestion du référentiel qualité
 * Name : EXT031MI.DelConstrType
 * Description : Delete records from the EXT031 table.
 * Date         Changed By   Description
 * 20230201     SEAR         QUAX01 - Constraints matrix 
 * 20240605     FLEBARS      QUAX01 - Controle code pour validation Infor
 */
public class DelConstrType extends ExtendM3Transaction {
  private final MIAPI mi
  private final LoggerAPI logger
  private final DatabaseAPI database
  private final ProgramAPI program

  private int currentCompany

  public DelConstrType(MIAPI mi, DatabaseAPI database, ProgramAPI program, LoggerAPI logger) {
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
    DBAction ext031Query = database.table("EXT031").index("00").build()
    DBContainer ext031Request = ext031Query.getContainer()
    ext031Request.set("EXCONO", currentCompany)
    ext031Request.set("EXZCTY", mi.in.get("ZCTY"))
    if (!ext031Query.readLock(ext031Request, ext031Updater)) {
      mi.error("L'enregistrement n'existe pas")
      return
    }
  }
  Closure<?> ext031Updater = { LockedResult ext031LockedResult ->
    ext031LockedResult.delete()
  }
}

