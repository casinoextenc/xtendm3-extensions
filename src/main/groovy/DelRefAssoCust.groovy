/**
 * Name : EXT010MI.DelRefAsso
 * Description :
 * This API method to delete records in specific table EXT010 Customer Assortment
 * COMX01 Gestion des assortiments clients
 * Date         Changed By    Description
 * 20221122     FLEBARS       COMX01 - Creation
 * 20240620     FLEBARS       COMX01 - Controle code pour validation Infor
 */
public class DelRefAssoCust extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program
  private final UtilityAPI utility

  private int currentCompany


  public DelRefAssoCust(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility) {
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
    String cuno = (String) (mi.in.get("CUNO") != null ? mi.in.get("CUNO") : "")


    //Check if record exists
    DBAction ext010Query = database.table("EXT010")
      .index("02")
      .selection(
        "EXCONO",
        "EXASGD",
        "EXCUNO",
        "EXITNO",
        "EXCDAT"
      )
      .build();

    DBContainer ext010Request = ext010Query.getContainer()
    ext010Request.set("EXCONO", 100)
    ext010Request.set("EXCUNO", cuno)


    //Record exists


    Closure<?> ext010Updater = { LockedResult ext010LockedResult ->
      ext010LockedResult.delete()
    }

    ext010Query.readAllLock(ext010Request,2, ext010Updater)

  }
}
