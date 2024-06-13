/**
 * README
 * This extension is used by Mashup
 * QUAX01 Gestion du référentiel qualité
 * Name : EXT032MI.DelQualityRef
 * Description : delete records to the EXT032 table.
 * Date         Changed By   Description
 * 20230210     SEAR         QUAX01 - Constraints matrix
 * 20240605     FLEBARS      QUAX01 - Controle code pour validation Infor
 */
public class DelQualityRef extends ExtendM3Transaction {
  private final MIAPI mi
  private final LoggerAPI logger
  private final ProgramAPI program
  private final DatabaseAPI database
  private final MICallerAPI miCaller
  private final UtilityAPI utility

  private int currentCompany

  public DelQualityRef(MIAPI mi, DatabaseAPI database, ProgramAPI program, MICallerAPI miCaller, UtilityAPI utility, LoggerAPI logger) {
    this.mi = mi
    this.database = database
    this.program = program
    this.miCaller = miCaller
    this.utility = utility
    this.logger = logger
  }

  public void main() {
    if (mi.in.get("CONO") == null) {
      currentCompany = (Integer)program.getLDAZD().CONO
    } else {
      currentCompany = mi.in.get("CONO")
    }
    DBAction ext032Query = database.table("EXT032").index("00").build()
    DBContainer ext032Request = ext032Query.getContainer()
    ext032Request.set("EXCONO", currentCompany)
    ext032Request.set("EXPOPN",  mi.in.get("POPN"))
    ext032Request.set("EXSUNO", mi.in.get("SUNO"))
    ext032Request.set("EXORCO", mi.in.get("ORCO"))

    Closure<?> ext032Updater = { LockedResult ext032LockedResult ->
      ext032LockedResult.delete()
    }
    if(!ext032Query.readLock(ext032Request, ext032Updater)){
      mi.error("L'enregistrement n'existe pas")
    }
  }
}
