/**
 * README
 * This extension is used by Mashup
 *
 * Name : EXT050MI.LstDLIX
 * Description : List delivery indexes
 * Date         Changed By   Description
 * 20230828     FLEBARS      LOG28 - Creation of files and containers
 */
public class LstDLIX extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final MICallerAPI miCaller
  private final ProgramAPI program
  private final UtilityAPI utility
  private Integer currentCompany
  private Integer nbMaxRecord = 10000

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
    DBAction mhdishQuery = database.table("MHDISH")
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

    DBContainer mhdishRequest = mhdishQuery.getContainer()
    mhdishRequest.set("OQCONO", currentCompany)
    mhdishRequest.set("OQINOU", 1)
    mhdishRequest.set("OQDLIX", dlix as Long)
    if (mhdishQuery.read(mhdishRequest)) {
      rorc = mhdishRequest.get("OQRORC") as Integer
      whlo = mhdishRequest.get("OQWHLO") as String
      dpol = mhdishRequest.get("OQDPOL") as String
      modl = mhdishRequest.get("OQMODL") as String
      tedl = mhdishRequest.get("OQTEDL") as String
      srot = mhdishRequest.get("OQSROT") as String
      cona = mhdishRequest.get("OQCONA") as String
      coaa = mhdishRequest.get("OQCOAA") as String
      pcka = mhdishRequest.get("OQPCKA") as String
      fwno = mhdishRequest.get("OQFWNO") as String
      agky = mhdishRequest.get("OQAGKY") as String
      dcc1 = mhdishRequest.get("OQDCC1") as String
    }

    //
    //  Load relative indexes
    //
    ExpressionFactory mhdish2Expression = database.getExpressionFactory("MHDISH")
    mhdish2Expression = mhdish2Expression.lt("OQPGRS", '50')
    mhdish2Expression = mhdish2Expression.and (mhdish2Expression.eq("OQRORC", "" + rorc))
    mhdish2Expression = mhdish2Expression.and (mhdish2Expression.eq("OQWHLO", whlo))
    mhdish2Expression = mhdish2Expression.and (mhdish2Expression.eq("OQDPOL", dpol))
    mhdish2Expression = mhdish2Expression.and (mhdish2Expression.eq("OQMODL", modl))
    mhdish2Expression = mhdish2Expression.and (mhdish2Expression.eq("OQTEDL", tedl))
    mhdish2Expression = mhdish2Expression.and (mhdish2Expression.eq("OQSROT", srot))
    mhdish2Expression = mhdish2Expression.and (mhdish2Expression.eq("OQCOAA", coaa))
    mhdish2Expression = mhdish2Expression.and (mhdish2Expression.eq("OQPCKA", pcka))
    mhdish2Expression = mhdish2Expression.and (mhdish2Expression.eq("OQFWNO", fwno))
    mhdish2Expression = mhdish2Expression.and (mhdish2Expression.eq("OQAGKY", agky))
    mhdish2Expression = mhdish2Expression.and (mhdish2Expression.eq("OQDCC1", dcc1))

    DBAction mhdish2Query = database.table("MHDISH")
      .matching(mhdish2Expression)
      .index("50")
      .build()

    Closure<?> mhdish2Reader = { DBContainer mhdish2Result ->
      String tmp = mhdish2Result.get("OQDLIX") as String
      if (!tmp.equals(dlix))
        dlixes.add(tmp)
    }

    DBContainer mhdish2Request = mhdish2Query.getContainer()
    mhdish2Request.set("OQCONO", currentCompany)
    mhdish2Request.set("OQINOU", 1)
    mhdish2Request.set("OQCONA", cona)

    if (mhdish2Query.readAll(mhdish2Request, 3, nbMaxRecord, mhdish2Reader)) {
    }

    Collections.sort(dlixes)

    for (dlix1 in dlixes) {
      mi.outData.put("DLIX", dlix1)
      mi.write()
    }
  }
}
