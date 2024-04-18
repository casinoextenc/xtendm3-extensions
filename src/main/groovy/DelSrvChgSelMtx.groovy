/**
 * Name : EXT061MI.DelSrvChgSelMtx
 * Version 1.0
 * Add records in EXT061
 *
 * Description :
 *
 * Date         Changed By    Description
 * 20230808     FLEBARS       CMD03 - Calculation of service charges
 * 20240202     MLECLERCQ     CMD03 - Support PREX6
 */
public class DelSrvChgSelMtx extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program
  private final UtilityAPI utility
  private final MICallerAPI miCaller

  private int currentCompany

  public DelSrvChgSelMtx(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility, MICallerAPI miCaller) {
    this.mi = mi
    this.database = database
    this.logger = logger
    this.program = program
    this.utility = utility
    this.miCaller = miCaller
  }

  public void main() {
    //**********************************
    // INZ Intialize
    //**********************************
    currentCompany = (int)program.getLDAZD().CONO

    //Get API INPUTS
    String prex = (String)mi.in.get("PREX")
    String obv1 = (String)mi.in.get("OBV1")
    String obv2 = (String)mi.in.get("OBV2")
    String obv3 = (String)mi.in.get("OBV3")
    String vfdt = (String)mi.in.get("VFDT")


    //**********************************
    // CHECK API INPUT PARAMETERS
    //**********************************
    if (!["1", "2", "3", "4", "5", "6"].contains(prex)) {
      mi.error("Priorité ${prex} est invalide")
      return
    }

    prex = " " + prex

    //**********************************
    // UPD UPDATE DB
    //**********************************
    //Check if record exists
    DBAction queryEXT06100 = database.table("EXT061").index("00").selection(
      "EXCONO"
      ,"EXPREX"
      ,"EXOBV1"
      ,"EXOBV2"
      ,"EXOBV3"
      ,"EXVFDT"
      ,"EXCRID"
      ,"EXCRD0"
      ,"EXCMRE"
      ,"EXCRFA"
      ,"EXCUCD"
      ,"EXLVDT"
      ,"EXRGDT"
      ,"EXRGTM"
      ,"EXLMDT"
      ,"EXCHNO"
      ,"EXCHID"
      ,"EXLMTS"
    ).build()

    DBContainer containerEXT061 = queryEXT06100.getContainer()
    containerEXT061.set("EXCONO",currentCompany)
    containerEXT061.set("EXPREX", prex)
    containerEXT061.set("EXOBV1", obv1)
    containerEXT061.set("EXOBV2", obv2)
    containerEXT061.set("EXOBV3", obv3)
    containerEXT061.set("EXVFDT", Integer.parseInt(vfdt))
    Closure<?> updaterEXT061 = { LockedResult lockedResultEXT061 ->
      lockedResultEXT061.delete()
    }
    if (!queryEXT06100.readLock(containerEXT061,updaterEXT061 )) {
      mi.error("L'enregistrement n'existe pas")
      return
    }
  }
}
