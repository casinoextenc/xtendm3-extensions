/**
 * Name : EXT061MI.LstSrvChgSelMtx
 * Version 1.0
 * Add records in EXT061
 *
 * Description :
 *
 * Date         Changed By    Description
 * 20230808     FLEBARS       CMD03 - Calculation of service charges
 * 20240202     MLECLERCQ     CMD03 - Support PREX6
<<<<<<< HEAD
=======
 * 20240809     YBLUTEAU      CMD03 - Prio 7
>>>>>>> origin/development
 */
public class LstSrvChgSelMtx extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program
  private final UtilityAPI utility
  private final MICallerAPI miCaller

  private int currentCompany

  public LstSrvChgSelMtx(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility, MICallerAPI miCaller) {
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
    if (!["1", "2", "3", "4", "5", "6"].contains(prex)) {
=======
    if (!["1", "2", "3", "4", "5", "6", "7"].contains(prex)) {
>>>>>>> origin/development
      mi.error("Priorité ${prex} est invalide")
      return
    }

    int hlvl = 0
    try {
      hlvl = 6 - Integer.parseInt(prex)
    } catch (NumberFormatException e) {
      logger.debug("AIE")
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
      .index("00")
      .selection(
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
    containerEXT061.set("EXCONO", currentCompany)
    containerEXT061.set("EXPREX", prex)

    if (!(obv1== null || obv1.isEmpty())) {
      containerEXT061.set("EXOBV1", obv1)
      nbfields++
    }

    if ((obv1 == null || obv1.isEmpty()) && !(obv2 == null || obv2.isEmpty())) {
      mi.error("Critères incorrectes")
      return
    } else if (obv2 == null || obv2.isEmpty()) {
      containerEXT061.set("EXOBV2", obv2)
      nbfields++
    }

    if ((obv2 == null || obv2.isEmpty()) && !(obv3 == null || obv3.isEmpty())) {
      mi.error("Critères incorrectes")
      return
    } else if (!(obv3 == null || obv3.isEmpty())){
      containerEXT061.set("EXOBV3", obv3)
      nbfields++
    }

    if ((obv3 == null || obv3.isEmpty()) && !(vfdt == null || vfdt.isEmpty())) {
      if(hlvl > 0){
        mi.error("Critères incorrectes")
<<<<<<< HEAD
        return  
=======
        return
>>>>>>> origin/development
      }

    } else if (!(vfdt == null || vfdt.isEmpty())){
      containerEXT061.set("EXVFDT", Integer.parseInt(vfdt))
      nbfields++
    }
<<<<<<< HEAD
    
    
=======


>>>>>>> origin/development
    Closure<?> outDataEXT061 = { DBContainer responseEXT061 ->
      mi.outData.put("PREX", responseEXT061.get("EXPREX") as String)
      mi.outData.put("OBV1", responseEXT061.get("EXOBV1") as String)
      mi.outData.put("OBV2", responseEXT061.get("EXOBV2") as String)
      mi.outData.put("OBV3", responseEXT061.get("EXOBV3") as String)
      mi.outData.put("VFDT", responseEXT061.get("EXVFDT") as String)
      mi.outData.put("CRID", responseEXT061.get("EXCRID") as String)
      mi.outData.put("CRD0", responseEXT061.get("EXCRD0") as String)
      mi.outData.put("CMRE", responseEXT061.get("EXCMRE") as String)
      mi.outData.put("CRFA", responseEXT061.get("EXCRFA") as String)
      mi.outData.put("CUCD", responseEXT061.get("EXCUCD") as String)
      mi.outData.put("LVDT", responseEXT061.get("EXLVDT") as String)
      mi.outData.put("RGDT", responseEXT061.get("EXRGDT") as String)
      mi.outData.put("RGTM", responseEXT061.get("EXRGTM") as String)
      mi.outData.put("LMDT", responseEXT061.get("EXLMDT") as String)
      mi.outData.put("CHNO", responseEXT061.get("EXCHNO") as String)
      mi.outData.put("CHID", responseEXT061.get("EXCHID") as String)
<<<<<<< HEAD
      
=======

>>>>>>> origin/development
      mi.write()
    }


    if (!queryEXT06100.readAll(containerEXT061, 1, outDataEXT061)){
      mi.error("L'enregistrement n'existe pas")
      return
    }

  }
}
