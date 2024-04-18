public class LstDLIXbyORNO extends ExtendM3Transaction {
  private final MIAPI mi;
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final MICallerAPI miCaller
  private final ProgramAPI program
  private final UtilityAPI utility
  private Integer currentCompany

  
  
  public LstDLIXbyORNO(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program,UtilityAPI utility,MICallerAPI miCaller) {
    this.mi = mi
    this.database = database
    this.logger = logger
    this.program = program
    this.utility = utility
    this.miCaller = miCaller
  }
  
  public void main() {
    currentCompany = (Integer)program.getLDAZD().CONO
    // Check job number
    String orno = mi.in.get("ORNO")
    List<String> dlixes = new ArrayList<String>()
    
    //TODO controle existence ORNO in OOHEAD
    
    DBAction MHDISL_query = database.table("MHDISL").index("10").build()
    DBContainer MHDISL_request = MHDISL_query.getContainer()
    MHDISL_request.set("URCONO", currentCompany)
    MHDISL_request.set("URRORC", 3)
    MHDISL_request.set("URRIDN", orno)
    
    
    /**
     * Store DLIX in blockedIndex list
     */
    Closure<?> closure_MHDISL = { DBContainer MHDISL_result ->
      String blop = ""
      String dlix = MHDISL_result.get("URDLIX") as String
      long l_dlix = MHDISL_result.get("URDLIX")
      DBAction query_MHDISH = database.table("MHDISH").index("00").selection("OQBLOP").build()
      DBContainer MHDISH = query_MHDISH.getContainer()
      MHDISH.set("OQCONO", currentCompany)
      MHDISH.set("OQINOU",  1)
      MHDISH.set("OQDLIX", l_dlix)
      if(query_MHDISH.read(MHDISH)) {
        blop = MHDISH.get("OQBLOP") as String
      }
      if (!dlixes.contains(dlix) && blop != "1") {
        dlixes.add(dlix)
      }
    }
    if (!MHDISL_query.readAll(MHDISL_request, 3, closure_MHDISL)) {
      mi.error("L'enregistrement n'existe pas")
      return
    }
    
    Collections.sort(dlixes);
    
    for (dlix in dlixes) {
      mi.outData.put("DLIX", dlix)
      mi.write()
    }
    
    
    
    
  }
}