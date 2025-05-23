/**
 * README
 * This extension is used by Mashup
 *
 * Name : EXT050MI.LstDLIXbyORNO
 * Description : List delivery indexes by order
 * Date         Changed By   Description
 * 20230828     FLEBARS      LOG28 - Creation of files and containers
 */
public class LstDLIXbyORNO extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final MICallerAPI miCaller
  private final ProgramAPI program
  private final UtilityAPI utility
  private Integer currentCompany
  private Integer nbMaxRecord = 10000

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

    // Check order
    if(mi.in.get("ORNO") != null){
      DBAction rechercheOOHEAD = database.table("OOHEAD").index("00").build();
      DBContainer OOHEAD = rechercheOOHEAD.getContainer();
      OOHEAD.set("OACONO", currentCompany);
      OOHEAD.set("OAORNO", mi.in.get("ORNO"));
      if(!rechercheOOHEAD.read(OOHEAD)){
        mi.error("La commande n'existe pas")
        return;
      }
    }else{
      mi.error("Le N° de commande est obligatoire")
      return;
    }

    DBAction mhdislQuery = database.table("MHDISL").index("10").build()
    DBContainer mhdislRequest = mhdislQuery.getContainer()
    mhdislRequest.set("URCONO", currentCompany)
    mhdislRequest.set("URRORC", 3)
    mhdislRequest.set("URRIDN", orno)

    /**
     * Store DLIX in blockedIndex list
     */
    Closure<?> closureMhdisl = { DBContainer mhdislResult ->
      String blop = ""
      String dlix = mhdislResult.get("URDLIX") as String
      long lDlix = mhdislResult.get("URDLIX")
      DBAction queryMhdish = database.table("MHDISH").index("00").selection("OQBLOP").build()
      DBContainer MHDISH = queryMhdish.getContainer()
      MHDISH.set("OQCONO", currentCompany)
      MHDISH.set("OQINOU",  1)
      MHDISH.set("OQDLIX", lDlix)
      if(queryMhdish.read(MHDISH)) {
        blop = MHDISH.get("OQBLOP") as String
      }
      if (!dlixes.contains(dlix) && blop != "1") {
        dlixes.add(dlix)
      }
    }
    if (!mhdislQuery.readAll(mhdislRequest, 3, nbMaxRecord, closureMhdisl)) {
      mi.error("L'enregistrement n'existe pas")
      return
    }

    Collections.sort(dlixes)

    for (dlix in dlixes) {
      mi.outData.put("DLIX", dlix)
      mi.write()
    }
  }
}
