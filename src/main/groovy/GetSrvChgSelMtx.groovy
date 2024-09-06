/**
 * Name : EXT061MI.LstSrvChgSelMtx
 * Version 1.0
 * Add records in EXT061
 *
 * Description :
 *
 * Date         Changed By    Description
 * 20230808     FLEBARS       CMD03 - Calculation of service charges
<<<<<<< HEAD
=======
 * 20240809     YBLUTEAU      CMD03 - Prio 7
>>>>>>> origin/development
 */
public class GetSrvChgSelMtx extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program
  private final UtilityAPI utility
  private final MICallerAPI miCaller

  private int currentCompany

  public GetSrvChgSelMtx(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility, MICallerAPI miCaller) {
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

    int nbfields = 2

    //**********************************
    // CHK
    //**********************************
<<<<<<< HEAD
    if (!["1", "2", "3", "4", "5"].contains(prex)) {
=======
    if (!["1", "2", "3", "4", "5", "6", "7"].contains(prex)) {
>>>>>>> origin/development
      mi.error("Priorité ${prex} est invalide")
      return
    }
    if (!(vfdt == null || vfdt.isEmpty())){
      boolean checkDate = (Boolean)utility.call("DateUtil", "isDateValid", "" + vfdt, "yyyyMMdd")
      if (!checkDate){
        mi.error("Date de début ${vfdt} est invalide")
        return
      }
    }

    //**********************************
    // UPD
    //**********************************
    DBAction queryEXT06100 = database.table("EXT061")
<<<<<<< HEAD
        .index("00")
        .selection(
=======
      .index("00")
      .selection(
>>>>>>> origin/development
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
<<<<<<< HEAD
        ).build()
=======
      ).build()
>>>>>>> origin/development

    DBContainer containerEXT061 = queryEXT06100.getContainer()
    containerEXT061.set("EXCONO", currentCompany)
    containerEXT061.set("EXPREX", prex)
    containerEXT061.set("EXOBV1", obv1)
    containerEXT061.set("EXOBV2", obv2)
    containerEXT061.set("EXOBV3", obv3)
    containerEXT061.set("EXVFDT", Integer.parseInt(vfdt))


    if (!queryEXT06100.read(containerEXT061)){
      mi.error("L'enregistrement n'existe pas")
      return
    } else {
      mi.outData.put("CRID", containerEXT061.get("EXCRID") as String)
      mi.outData.put("CRD0", containerEXT061.get("EXCRD0") as String)
      mi.outData.put("CMRE", containerEXT061.get("EXCMRE") as String)
      mi.outData.put("CRFA", containerEXT061.get("EXCRFA") as String)
      mi.outData.put("CUCD", containerEXT061.get("EXCUCD") as String)
      mi.outData.put("LVDT", containerEXT061.get("EXLVDT") as String)
      mi.outData.put("RGDT", containerEXT061.get("EXRGDT") as String)
      mi.outData.put("RGTM", containerEXT061.get("EXRGTM") as String)
      mi.outData.put("LMDT", containerEXT061.get("EXLMDT") as String)
      mi.outData.put("CHNO", containerEXT061.get("EXCHNO") as String)
      mi.outData.put("CHID", containerEXT061.get("EXCHID") as String)
      mi.write()
    }
  }
<<<<<<< HEAD
}
=======
}
>>>>>>> origin/development
