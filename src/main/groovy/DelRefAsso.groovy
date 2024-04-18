/**
 * Name : EXT010MI.DelRefAsso
 * 
 * Description : 
 * This API method to delete records in specific table EXT010 Customer Assortment
 * 
 * 
 * Date         Changed By    Description
 * 20221122     FLEBARS       COMX01 - Creation
 */
public class DelRefAsso extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program
  private final UtilityAPI utility

  private int currentCompany
  private String errorMessage

  
  public DelRefAsso(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility) {
    this.mi = mi
    this.database = database
    this.logger = logger
    this.program = program
    this.utility=utility
  }
  
    /**
   * Get mi inputs
   * Check input values
   * Check if record not exists in EXT010
   * Serialize in EXT010
   */
  public void main() {
    currentCompany = (int)program.getLDAZD().CONO
    
    //Get mi inputs
    String asgd = (String)(mi.in.get("ASGD") != null ? mi.in.get("ASGD") : "")
    String cuno = (String)(mi.in.get("CUNO") != null ? mi.in.get("CUNO") : "")
    String itno = (String)(mi.in.get("ITNO") != null ? mi.in.get("ITNO") : "")
    int cdat = (Integer)(mi.in.get("CDAT") != null ? mi.in.get("CDAT") : 0)
    
        //Check if record exists
    DBAction queryEXT010 = database.table("EXT010")
      .index("00")
      .selection(
        "EXCONO",
        "EXASGD",
        "EXCUNO",
        "EXITNO",
        "EXCDAT"
      )
      .build();
    
    DBContainer containerEXT010 = queryEXT010.getContainer()
    containerEXT010.set("EXCONO", currentCompany)
    containerEXT010.set("EXASGD", asgd)
    containerEXT010.set("EXCUNO", cuno)
    containerEXT010.set("EXITNO", itno)
    containerEXT010.set("EXCDAT", cdat)
    
    //Record exists
    if (!queryEXT010.read(containerEXT010)) {
      mi.error("L'enregistrement n'existe pas")
      return
    }
    
    Closure<?> deleteEXT010 = { LockedResult lockedResultEXT010 ->
      lockedResultEXT010.delete()
    }
    
    queryEXT010.readLock(containerEXT010, deleteEXT010)

  }
}