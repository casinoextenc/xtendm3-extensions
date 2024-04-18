/**
 * Name : EXT012MI.DelPlanningGDA
 * 
 * Description : 
 * This API method to add records in specific table EXT012 Planning GDA
 * 
 * 
 * Date         Changed By    Description
 * 20230308     SEAR       Creation
 */
public class DelPlanningGDA extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program
  private final UtilityAPI utility

  private int currentCompany
  private String errorMessage


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
    

    DBAction query = database.table("EXT012").index("00").build()
    DBContainer containerEXT012 = query.getContainer()
    containerEXT012.set("EXCONO", currentCompany)
    containerEXT012.set("EXCUNO", cuno)
    containerEXT012.set("EXSUNO", suno)
    containerEXT012.set("EXASGD", asgd)
    containerEXT012.set("EXDRGD", drgd)
    containerEXT012.set("EXHRGD", hrgd)
    if(!query.readLock(containerEXT012, updateCallBack)){
      mi.error("L'enregistrement n'existe pas")
      return
    }
  }
  Closure<?> updateCallBack = { LockedResult lockedResult ->
    lockedResult.delete()
  }

}