/**
 * Name : EXT012MI.GetGDADateByDly
 * 
 * Description : 
 * This API method to add records in specific table EXT012 Planning GDA
 * 
 * 
 * Date         Changed By    Description
 * 20230308     SEAR       Creation
 */
public class GetGDADateByDly extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program
  private final UtilityAPI utility

  private int currentCompany
  private String errorMessage


  public GetGDADateByDly(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility) {
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
    String suno = (String)(mi.in.get("SUNO") != null ? mi.in.get("SUNO") : "")
    int dlgd = (Integer)(mi.in.get("DLGD") != null ? mi.in.get("DLGD") : 0)
    
    String deliveryDate = (mi.in.get("DLGD") != null ? (Integer)mi.in.get("DLGD") : 0)
    
    ExpressionFactory expression = database.getExpressionFactory("EXT012")
    expression = expression.eq("EXDLGD", deliveryDate)
    
    DBAction queryEXT012 = database.table("EXT012")
        .index("10")
        .matching(expression)
        .selection(
        "EXCONO",
        "EXCUNO",
        "EXSUNO",
        "EXASGD",
        "EXDRGD",
        "EXHRGD",
        "EXDLGD",
        "EXRGDT",
        "EXRGTM",
        "EXLMDT",
        "EXCHNO",
        "EXCHID"
        )
        .build()
        
    DBContainer containerEXT012 = queryEXT012.getContainer()
    containerEXT012.set("EXCONO", currentCompany)
    containerEXT012.set("EXCUNO", cuno)
    containerEXT012.set("EXSUNO", suno)
    containerEXT012.set("EXASGD", asgd)
    containerEXT012.set("EXDLGD", dlgd)
    
    //Record exists
    if (!queryEXT012.readAll(containerEXT012, 4, outData)){
      mi.error("L'enregistrement n'existe pas")
      return
    }
  }
  Closure<?> outData = { DBContainer containerEXT012 ->
    String customerCode = containerEXT012.get("EXCUNO")
    String supplierCode = containerEXT012.get("EXSUNO")
    String Assortment = containerEXT012.get("EXASGD")
    String pickupDate = containerEXT012.get("EXDRGD")
    String pickuptHour = containerEXT012.get("EXHRGD")
    String deliveryDate = containerEXT012.get("EXDLGD")
    String entryDate = containerEXT012.get("EXRGDT")
    String entryTime = containerEXT012.get("EXRGTM")
    String changeDate = containerEXT012.get("EXLMDT")
    String changeNumber = containerEXT012.get("EXCHNO")
    String changedBy = containerEXT012.get("EXCHID")
    mi.outData.put("CUNO", customerCode)
    mi.outData.put("SUNO", supplierCode)
    mi.outData.put("ASGD", Assortment)
    mi.outData.put("DRGD", pickupDate)
    mi.outData.put("HRGD", pickuptHour)
    mi.outData.put("DLGD", deliveryDate)
    mi.outData.put("RGDT", entryDate)
    mi.outData.put("RGTM", entryTime)
    mi.outData.put("LMDT", changeDate)
    mi.outData.put("CHNO", changeNumber)
    mi.outData.put("CHID", changedBy)
    mi.write()
  }


}