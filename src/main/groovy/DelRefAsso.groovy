/**
 * Name : EXT010MI.DelRefAsso
 * Description :
 * This API method to delete records in specific table EXT010 Customer Assortment
 * COMX01 Gestion des assortiments clients
 * Date         Changed By    Description
 * 20221122     FLEBARS       COMX01 - Creation
 * 20240620     FLEBARS       COMX01 - Controle code pour validation Infor
 */
public class DelRefAsso extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program
  private final UtilityAPI utility

  private int currentCompany


  public DelRefAsso(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility) {
    this.mi = mi
    this.database = database
    this.logger = logger
    this.program = program
    this.utility = utility
  }

  /**
   * Get mi inputs
   * Check input values
   * Check if record not exists in EXT010
   * Serialize in EXT010
   */
  public void main() {
    currentCompany = (int) program.getLDAZD().CONO

    //Get mi inputs
    String asgd = (String) (mi.in.get("ASGD") != null ? mi.in.get("ASGD") : "")
    String cuno = (String) (mi.in.get("CUNO") != null ? mi.in.get("CUNO") : "")
    String itno = (String) (mi.in.get("ITNO") != null ? mi.in.get("ITNO") : "")
    int cdat = (Integer) (mi.in.get("CDAT") != null ? mi.in.get("CDAT") : 0)

    //Check if record exists
    DBAction ext010Query = database.table("EXT010")
      .index("00")
      .selection(
        "EXCONO",
        "EXASGD",
        "EXCUNO",
        "EXITNO",
        "EXCDAT"
      )
      .build();

    DBContainer ext010Request = ext010Query.getContainer()
    ext010Request.set("EXCONO", currentCompany)
    ext010Request.set("EXASGD", asgd)
    ext010Request.set("EXCUNO", cuno)
    ext010Request.set("EXITNO", itno)
    ext010Request.set("EXCDAT", cdat)

    //Record exists
    if (!ext010Query.read(ext010Request)) {
      mi.error("L'enregistrement n'existe pas")
      return
    }

    Closure<?> ext010Updater = { LockedResult ext010LockedResult ->
      ext010LockedResult.delete()
    }

    ext010Query.readLock(ext010Request, ext010Updater)

  }
}
