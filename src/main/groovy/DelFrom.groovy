/**
 * Name : EXT875MI.DelFrom
 * Version 1.0
 *
 * Description :

 *
 * Date         Changed By    Description
 * 20241122     FLEBARS       Creation
 */
public class DelFrom extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program
  private final UtilityAPI utility
  private final MICallerAPI miCaller

  private final int maxCountReadAll = 10000

  private int currentCompany

  public DelFrom(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility, MICallerAPI miCaller) {
    this.mi = mi
    this.database = database
    this.logger = logger
    this.program = program
    this.utility = utility
    this.miCaller = miCaller
  }

  public void main() {
    currentCompany = (int) program.getLDAZD().CONO
    int nbkeys = 1
    String lmts = (String) mi.in.get("LMTS")
    String pgnm = (String) mi.in.get("PGNM")

    logger.debug("DelFrom")

    ExpressionFactory ext875Expression = database.getExpressionFactory("EXT875")
    ext875Expression = ext875Expression.ge("EXLMTS", lmts)
    if (pgnm != null && !pgnm.isEmpty()) {
      ext875Expression = ext875Expression.and(ext875Expression.eq("EXPGNM", pgnm))
    }

    //READLOCK EXT875
    DBAction ext875Query = database.table("EXT875")
      .index("20")
      .matching(ext875Expression)
      .build()

    DBContainer ext875Request = ext875Query.getContainer()
    ext875Request.set("EXCONO", currentCompany)

    Closure<?> ext875Updater = { LockedResult ext875LockedResult ->
      ext875LockedResult.delete()
    }
    Closure<?> ext875Reader = { DBContainer ext875Result ->
      ext875Query.readAllLock(ext875Result, 4, ext875Updater)
    }

    if (!ext875Query.readAll(ext875Request, 1,110000, ext875Reader)){
      mi.error("No records found")
      return
    }

  }
}
