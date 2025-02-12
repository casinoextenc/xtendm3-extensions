/**
 * Name : EXT012MI.DelPlanningGDA
 *
 * Description :
 * APP02 Planning GDA This API method to delete records in specific table EXT012
 *
 *
 * Date         Changed By    Description
 * 20230308     SEAR       Creation
 * 20240911     FLEBARS       Revue code pour validation
 */
public class DelPlanningGDA extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program
  private final UtilityAPI utility

  private int currentCompany


  public DelPlanningGDA(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility) {
    this.mi = mi
    this.database = database
    this.logger = logger
    this.program = program
    this.utility=utility
  }

  /**
   * Get mi inputs
   * Check input values
   * Check if record not exists in EXT012
   * Serialize in EXT012
   */
  public void main() {
    currentCompany = (int)program.getLDAZD().CONO

    //Get mi inputs
    String asgd = (String)(mi.in.get("ASGD") != null ? mi.in.get("ASGD") : "")
    String cuno = (String)(mi.in.get("CUNO") != null ? mi.in.get("CUNO") : "")
    String suno = (String)(mi.in.get("SUNO") != null ? mi.in.get("SUNO") : "")
    int drgd = (Integer)(mi.in.get("DRGD") != null ? mi.in.get("DRGD") : 0)
    int hrgd = (Integer)(mi.in.get("HRGD") != null ? mi.in.get("HRGD") : 0)


    DBAction ext012Query = database.table("EXT012").index("00").build()
    DBContainer ext012Request = ext012Query.getContainer()
    ext012Request.set("EXCONO", currentCompany)
    ext012Request.set("EXCUNO", cuno)
    ext012Request.set("EXSUNO", suno)
    ext012Request.set("EXASGD", asgd)
    ext012Request.set("EXDRGD", drgd)
    ext012Request.set("EXHRGD", hrgd)
    if(!ext012Query.readLock(ext012Request, ext012Updater)){
      mi.error("L'enregistrement n'existe pas")
      return
    }
  }
  Closure<?> ext012Updater = { LockedResult ext012LockedResult ->
    ext012LockedResult.delete()
  }

}
