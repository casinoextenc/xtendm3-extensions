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
 * 20240809     YBLUTEAU      CMD03 - Prio 7
 * 20241211     YJANNIN       CMD03 2.5 - Prio 6
 */
public class LstSrvChgSelMtx extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program
  private final UtilityAPI utility
  private final MICallerAPI miCaller

  private int currentCompany
  private Integer nbMaxRecord = 10000

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
    String prex = mi.in.get("PREX") == null ? "" : mi.in.get("PREX") as String
    String obv1 = mi.in.get("OBV1") == null ? "" : mi.in.get("OBV1") as String
    String obv2 = mi.in.get("OBV2") == null ? "" : mi.in.get("OBV2") as String
    String obv3 = mi.in.get("OBV3") == null ? "" : mi.in.get("OBV3") as String
    String obv4 = mi.in.get("OBV4") == null ? "" : mi.in.get("OBV4") as String
    String vfdt = mi.in.get("VFDT") == null ? "" : mi.in.get("VFDT") as String

    int nbfields = 2

    //**********************************
    // CHK
    //**********************************
    if (!["1", "2", "3", "4", "5", "6"].contains(prex)) {
      mi.error("Priorité ${prex} est invalide")
      return
    }

    int hlvl = 0
    try {
      hlvl = 4 - Integer.parseInt(prex)
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
    DBAction queryExt06100 = database.table("EXT061")
      .index("00")
      .selection(
        "EXCONO"
        ,"EXPREX"
        ,"EXOBV1"
        ,"EXOBV2"
        ,"EXOBV3"
        ,"EXOBV4"
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
      ).build()

    DBContainer containerExt061 = queryExt06100.getContainer()
    containerExt061.set("EXCONO", currentCompany)
    containerExt061.set("EXPREX", prex)

    if (!(obv1== null || obv1.isEmpty())) {
      containerExt061.set("EXOBV1", obv1)
      nbfields++
    }

    if ((obv1 == null || obv1.isEmpty()) && !(obv2 == null || obv2.isEmpty())) {
      mi.error("Critères incorrectes")
      return
    } else if (obv2 == null || obv2.isEmpty()) {
      containerExt061.set("EXOBV2", obv2)
      nbfields++
    }

    if ((obv2 == null || obv2.isEmpty()) && !(obv3 == null || obv3.isEmpty())) {
      mi.error("Critères incorrectes")
      return
    } else if (!(obv3 == null || obv3.isEmpty())){
      containerExt061.set("EXOBV3", obv3)
      nbfields++
    }

    if ((obv3 == null || obv3.isEmpty()) && !(obv4 == null || obv4.isEmpty())) {
      mi.error("Critères incorrectes")
      return
    } else if (!(obv4 == null || obv4.isEmpty())){
      containerExt061.set("EXOBV4", obv3)
      nbfields++
    }

    if ((obv4 == null || obv4.isEmpty()) && !(vfdt == null || vfdt.isEmpty())) {
      if(hlvl > 0){
        mi.error("Critères incorrectes")
        return
      }
    } else if (!(vfdt == null || vfdt.isEmpty())){
      containerExt061.set("EXVFDT", Integer.parseInt(vfdt))
      nbfields++
    }


    Closure<?> outDataExt061 = { DBContainer responseExt061 ->
      mi.outData.put("PREX", responseExt061.get("EXPREX") as String)
      mi.outData.put("OBV1", responseExt061.get("EXOBV1") as String)
      mi.outData.put("OBV2", responseExt061.get("EXOBV2") as String)
      mi.outData.put("OBV3", responseExt061.get("EXOBV3") as String)
      mi.outData.put("OBV4", responseExt061.get("EXOBV4") as String)
      mi.outData.put("VFDT", responseExt061.get("EXVFDT") as String)
      mi.outData.put("CRID", responseExt061.get("EXCRID") as String)
      mi.outData.put("CRD0", responseExt061.get("EXCRD0") as String)
      mi.outData.put("CMRE", responseExt061.get("EXCMRE") as String)
      mi.outData.put("CRFA", responseExt061.get("EXCRFA") as String)
      mi.outData.put("CUCD", responseExt061.get("EXCUCD") as String)
      mi.outData.put("LVDT", responseExt061.get("EXLVDT") as String)
      mi.outData.put("RGDT", responseExt061.get("EXRGDT") as String)
      mi.outData.put("RGTM", responseExt061.get("EXRGTM") as String)
      mi.outData.put("LMDT", responseExt061.get("EXLMDT") as String)
      mi.outData.put("CHNO", responseExt061.get("EXCHNO") as String)
      mi.outData.put("CHID", responseExt061.get("EXCHID") as String)

      mi.write()
    }


    if (!queryExt06100.readAll(containerExt061, nbfields, nbMaxRecord, outDataExt061)){
      mi.error("L'enregistrement n'existe pas")
      return
    }

  }
}
