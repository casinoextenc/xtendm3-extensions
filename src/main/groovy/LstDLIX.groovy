public class LstDLIX extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final MICallerAPI miCaller
  private final ProgramAPI program
  private final UtilityAPI utility
  private Integer currentCompany



  public LstDLIX(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program,UtilityAPI utility,MICallerAPI miCaller) {
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
    String dlix = mi.in.get("DLIX")
    int rorc = 0
    String whlo = ""
    String dpol = ""
    String modl = ""
    String tedl = ""
    String srot = ""
    String cona = ""
    String coaa = ""
    String pcka = ""
    String fwno = ""
    String agky = ""
    String dcc1 = ""

    List<String> dlixes = new ArrayList<String>()

    
    //GET MHDISH DATA For input DLIX
    DBAction MHDISH_query = database.table("MHDISH")
        .selection(
        "OQCONO"
        ,"OQINOU"
        ,"OQDLIX"
        ,"OQRORC"
        ,"OQWHLO"
        ,"OQDPOL"
        ,"OQMODL"
        ,"OQTEDL"
        ,"OQSROT"
        ,"OQCONA"
        ,"OQCOAA"
        ,"OQPCKA"
        ,"OQFWNO"
        ,"OQAGKY"
        ,"OQDCC1"
        )
        .index("00")
        .build()


    DBContainer MHDISH_request = MHDISH_query.getContainer()
    MHDISH_request.set("OQCONO", currentCompany)
    MHDISH_request.set("OQINOU", 1)
    MHDISH_request.set("OQDLIX", dlix as Long)
    if (MHDISH_query.read(MHDISH_request)) {
      rorc = MHDISH_request.get("OQRORC") as Integer
      whlo = MHDISH_request.get("OQWHLO") as String
      dpol = MHDISH_request.get("OQDPOL") as String
      modl = MHDISH_request.get("OQMODL") as String
      tedl = MHDISH_request.get("OQTEDL") as String
      srot = MHDISH_request.get("OQSROT") as String
      cona = MHDISH_request.get("OQCONA") as String
      coaa = MHDISH_request.get("OQCOAA") as String
      pcka = MHDISH_request.get("OQPCKA") as String
      fwno = MHDISH_request.get("OQFWNO") as String
      agky = MHDISH_request.get("OQAGKY") as String
      dcc1 = MHDISH_request.get("OQDCC1") as String
    }
    
    
    //
    //  Load relative indexes
    //
    ExpressionFactory MHDISH2_expression = database.getExpressionFactory("MHDISH")
    MHDISH2_expression = MHDISH2_expression.lt("OQPGRS", '50')
    MHDISH2_expression = MHDISH2_expression.and (MHDISH2_expression.eq("OQRORC", "" + rorc))
    MHDISH2_expression = MHDISH2_expression.and (MHDISH2_expression.eq("OQWHLO", whlo))
    MHDISH2_expression = MHDISH2_expression.and (MHDISH2_expression.eq("OQDPOL", dpol))
    MHDISH2_expression = MHDISH2_expression.and (MHDISH2_expression.eq("OQMODL", modl))
    MHDISH2_expression = MHDISH2_expression.and (MHDISH2_expression.eq("OQTEDL", tedl))
    MHDISH2_expression = MHDISH2_expression.and (MHDISH2_expression.eq("OQSROT", srot))
    MHDISH2_expression = MHDISH2_expression.and (MHDISH2_expression.eq("OQCOAA", coaa))
    MHDISH2_expression = MHDISH2_expression.and (MHDISH2_expression.eq("OQPCKA", pcka))
    MHDISH2_expression = MHDISH2_expression.and (MHDISH2_expression.eq("OQFWNO", fwno))
    MHDISH2_expression = MHDISH2_expression.and (MHDISH2_expression.eq("OQAGKY", agky))
    MHDISH2_expression = MHDISH2_expression.and (MHDISH2_expression.eq("OQDCC1", dcc1))
    
    

    DBAction MHDISH2_query = database.table("MHDISH")
        .matching(MHDISH2_expression)
        .index("50")
        .build()

    Closure<?> MHDISH2_reader = { DBContainer MHDISH2_result ->
      String tmp = MHDISH2_result.get("OQDLIX") as String
      if (!tmp.equals(dlix))
        dlixes.add(tmp)
    }


    DBContainer MHDISH2_request = MHDISH2_query.getContainer()
    MHDISH2_request.set("OQCONO", currentCompany)
    MHDISH2_request.set("OQINOU", 1)
    MHDISH2_request.set("OQCONA", cona)
    
    if (MHDISH2_query.readAll(MHDISH2_request, 3, MHDISH2_reader)) {
    }
    
    Collections.sort(dlixes);
    
    for (dlix1 in dlixes) {
      mi.outData.put("DLIX", dlix1)
      mi.write()
    }

    
  }
}