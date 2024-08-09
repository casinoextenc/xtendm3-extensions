/**
 * Name: EXT061MI.UpdSrvChgSelMtx
 * Migration projet GIT
 * old file = EXT061MI_UpdSrvChgSelMtx.groovy
 */

/**
 * Name : EXT061MI.UpdSrvChgSelMtx
 * Version 1.0
 * Add records in EXT061
 *
 * Description :
 *
 * Date         Changed By    Description
 * 20230808     FLEBARS       Creation EXT061
 * 20240130     MLECLERCQ     Support PREX 6 & LVDT
 * 20240809     YBLUTEAU      CMD03 - Prio 7
 */
public class UpdSrvChgSelMtx extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program
  private final UtilityAPI utility
  private final MICallerAPI miCaller

  private int currentCompany

  public UpdSrvChgSelMtx(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility, MICallerAPI miCaller) {
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
    String crid = (String)mi.in.get("CRID")
    String crfa = (String)mi.in.get("CRFA")
    String cucd = (String)mi.in.get("CUCD")
    String lvdt = (String)mi.in.get("LVDT")

    int crme = 0
    String crd0 = ""

    //**********************************
    // CHECK API INPUT PARAMETERS
    //**********************************
    if (!["1", "2", "3", "4", "5", "6", "7"].contains(prex)) {
      mi.error("Priorité ${prex} est invalide")
      return
    }

    int hlvl = 0
    try {
      hlvl = 6 - Integer.parseInt(prex)
    } catch (NumberFormatException e) {
      logger.debug("AIE")
    }

    prex = " " + prex

    //CHECK if obv1 exists in OCUSMA
    DBAction queryOCUSMA00 = database.table("OCUSMA").index("00").selection(
      "OKCONO",
      "OKCUNO",
      "OKSTAT"
    ).build()

    DBContainer containerOCUSMA = queryOCUSMA00.getContainer()
    containerOCUSMA.set("OKCONO",currentCompany)
    containerOCUSMA.set("OKCUNO", obv1)
    if (!queryOCUSMA00.read(containerOCUSMA)) {
      mi.error("Client ${obv1} inexistant")
      return
    }

    if(hlvl > 0){
      //CHECK if obv2 exists in MITHRY
      DBAction queryMITHRY00 = database.table("MITHRY").index("00").selection(
        "HICONO",
        "HIHLVL",
        "HIHIE0"
      ).build()

      DBContainer containerMITHRY = queryMITHRY00.getContainer()
      containerMITHRY.set("HICONO",currentCompany)
      containerMITHRY.set("HIHLVL", hlvl)
      containerMITHRY.set("HIHIE0", obv2)
      if (!queryMITHRY00.read(containerMITHRY)) {
        mi.error("Hierarchie article niveau ${hlvl} ${obv2} inexistante")
        return
      }

      //CHECK if obv3 exists in CIDMAS
      DBAction queryCIDMAS00 = database.table("CIDMAS").index("00").selection(
        "IDCONO",
        "IDSUNO"
      ).build()

      DBContainer containerCIDMAS = queryCIDMAS00.getContainer()
      containerCIDMAS.set("IDCONO",currentCompany)
      if(prex ==  " 6"){
        containerCIDMAS.set("IDSUNO", obv2)
        if (!queryCIDMAS00.read(containerCIDMAS)) {
          mi.error("Fournisseur ${obv3} inexistant")
          return
        }
      }
      containerCIDMAS.set("IDSUNO", obv3)
      if (!queryCIDMAS00.read(containerCIDMAS)) {
        mi.error("Fournisseur ${obv3} inexistant")
        return
      }
    }



    boolean checkDateDebut = (Boolean)utility.call("DateUtil", "isDateValid", "" + vfdt, "yyyyMMdd")
    if (!checkDateDebut){
      mi.error("Date de début ${vfdt} est invalide")
      return
    }

    boolean checkDateFin = (Boolean)utility.call("DateUtil", "isDateValid", "" + lvdt, "yyyyMMdd")
    if (!checkDateFin){
      mi.error("Date de fin ${lvdt} est invalide")
      return
    }
    if(checkDateDebut > checkDateFin){
      mi.error("Date de fin ${lvdt} ne peut être inférieure à date début ${vfdt}")
      return
    }

    //CHECK if crid exists in OLICHA
    DBAction queryOLICHA00 = database.table("OLICHA").index("00").selection(
      "MJCONO"
      ,"MJCRID"
      ,"MJCRD0"
      ,"MJCRME"
    ).build()

    DBContainer containerOLICHA = queryOLICHA00.getContainer()
    containerOLICHA.set("MJCONO",currentCompany)
    containerOLICHA.set("MJCRID", crid)
    if (!queryOLICHA00.read(containerOLICHA)) {
      mi.error("Frais ${crid} inexistant")
      return
    } else {
      crd0 = containerOLICHA.get("MJCRD0") as String
      crme = containerOLICHA.get("MJCRME") as Integer
    }
    //CHECK if crid exists in CUGEX1
    DBAction queryCUGEX100 = database.table("CUGEX1").index("00").selection(
      "F1CONO"
      ,"F1FILE"
      ,"F1PK01"
      ,"F1PK02"
      ,"F1PK03"
      ,"F1PK04"
      ,"F1PK05"
      ,"F1PK06"
      ,"F1PK07"
      ,"F1PK08"
      ,"F1CHB1"
    ).build()

    //CHECK if crid flagged for extension
    DBContainer containerCUGEX1 = queryCUGEX100.getContainer()
    containerCUGEX1.set("F1CONO",currentCompany)
    containerCUGEX1.set("F1FILE", "OLICHA")
    containerCUGEX1.set("F1PK01", crid)
    boolean chkcrid = false
    if (queryCUGEX100.read(containerCUGEX1)) {
      String chb1 = containerCUGEX1.get("F1CHB1") as String
      chkcrid = "1".equals(chb1)
    }
    if (!chkcrid){
      mi.error("Frais ${crid} pas activé frais auto")
      return
    }

    //CHECK if crid exists in CSYTAB
    DBAction queryCSYTAB00 = database.table("CSYTAB").index("00").selection(
      "CTCONO"
      ,"CTDIVI"
      ,"CTSTCO"
      ,"CTSTKY"
      ,"CTLNCD"
    ).build()

    DBContainer containerCSYTAB = queryCSYTAB00.getContainer()
    containerCSYTAB.set("CTCONO",currentCompany)
    containerCSYTAB.set("CTDIVI", "")
    containerCSYTAB.set("CTSTCO", "CUCD")
    containerCSYTAB.set("CTSTKY", cucd)
    containerCSYTAB.set("CTLNCD", "")
    if (!queryCSYTAB00.read(containerCSYTAB)) {
      mi.error("Devise ${cucd} inexistante")
      return
    }

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
      int changeNumber = lockedResultEXT061.get("EXCHNO")
      lockedResultEXT061.set("EXCRID", crid)
      lockedResultEXT061.set("EXCRD0", crd0)
      lockedResultEXT061.set("EXCMRE", crme)
      lockedResultEXT061.set("EXCRFA", Double.parseDouble(crfa))
      lockedResultEXT061.set("EXCUCD", cucd)
      lockedResultEXT061.set("EXLVDT", Integer.parseInt(lvdt))
      lockedResultEXT061.set("EXLMDT", utility.call("DateUtil", "currentDateY8AsInt"))
      lockedResultEXT061.set("EXCHNO", changeNumber + 1)
      lockedResultEXT061.set("EXCHID", program.getUser())
      lockedResultEXT061.set("EXLMTS", utility.call("DateUtil", "currentEpochMilliseconds"))
      lockedResultEXT061.update()
    }
    if (!queryEXT06100.readLock(containerEXT061,updaterEXT061 )) {
      mi.error("L'enregistrement n'existe pas")
      return
    }
  }
}
