/****************************************************************************************
 Extension Name: EXT050MI.LstMastFilShip1
 Type: ExtendM3Transaction
 Script Author: SEAR
 Date: 2023-05-11
 Description:
 * List master file shipment

 Revision History:
 Name                    Date             Version          Description of Changes
 SEAR                    2023-05-11       1.0              LOG28 - Creation of files and containers
 MLECLERCQ               2023-08-18       1.1              LOG28 - Correction ZNBC filter for 0 <> null
 ARENARD                 2025-04-28       1.2              Extension has been fixed
 MLECLERCQ               2025-05-13       1.3              Added OBORST > 20 filter on OOLINE
 MLECLERCQ               2025-11-14       1.4              Changed read on OOLINE to MITPLO due to volume
 ******************************************************************************************/

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class LstMastFilShip1 extends ExtendM3Transaction {
  private final MIAPI mi
  private final LoggerAPI logger
  private final ProgramAPI program
  private final DatabaseAPI database
  private final MICallerAPI miCaller
  private final UtilityAPI utility

  int currentCompany

  private String whloInput
  private String uca4Input
  private String uca5Input
  private String uca6Input
  private Long znbcInput
  private Long znbcDradtr

  private String jobNumber
  private Integer nbMaxRecord = 10000

  private ArrayList<String> allowedOrders

  public LstMastFilShip1(LoggerAPI logger, MIAPI mi, DatabaseAPI database, ProgramAPI program, MICallerAPI miCaller, UtilityAPI utility) {
    this.logger = logger
    this.mi = mi
    this.database = database
    this.program = program
    this.miCaller = miCaller
    this.utility = utility
  }

  public void main() {
    LocalDateTime timeOfCreation = LocalDateTime.now()
    jobNumber = program.getJobNumber() + timeOfCreation.format(DateTimeFormatter.ofPattern("yyMMdd")) + timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss"))

    if (mi.in.get("CONO") == null) {
      currentCompany = (Integer)program.getLDAZD().CONO
    } else {
      currentCompany = mi.in.get("CONO")
    }

    //Get mi inputs
    whloInput = (mi.in.get("WHLO") != null ? (String)mi.in.get("WHLO") : "")
    uca4Input = (mi.in.get("UCA4") != null ? (String)mi.in.get("UCA4") : "")
    uca5Input = (mi.in.get("UCA5") != null ? (String)mi.in.get("UCA5") : "")
    uca6Input = (mi.in.get("UCA6") != null ? (String)mi.in.get("UCA6") : "")
    znbcInput = (Long)(mi.in.get("ZNBC") != null ? mi.in.get("ZNBC") : -1)


    // check warehouse
    DBAction queryMitwhl = database.table("MITWHL").index("00").selection("MWWHLO").build()
    DBContainer MITWHL = queryMitwhl.getContainer()
    MITWHL.set("MWCONO", currentCompany)
    MITWHL.set("MWWHLO", whloInput)
    if(!queryMitwhl.read(MITWHL)){
      mi.error("Le dépôt " + whloInput + " n'existe pas")
      return
    }

    // Get OOLINE
    allowedOrders = new ArrayList<>()
    ExpressionFactory mitploExp = database.getExpressionFactory("MITPLO")
    mitploExp = mitploExp.lt("MOSTS2", "44")
    mitploExp = mitploExp.and(mitploExp.gt("MOSTS2", "20"))
    mitploExp = mitploExp.and(mitploExp.eq("MOORCA","311"))

    DBAction mitploQuery = database.table("MITPLO").index("00").matching(mitploExp).selection("MORIDN","MOWHLO").build()
    DBContainer mitploRequest = mitploQuery.getContainer()
    mitploRequest.set("MOCONO", currentCompany)
    mitploRequest.set("MOWHLO", whloInput)

    Closure<?> mitploReader = { DBContainer mitploResult ->
      String orno = mitploResult.get("MORIDN")

      if(allowedOrders.size() > 0){
        boolean  found = allowedOrders.find { it -> it == orno}
        if(!found){
          allowedOrders.add(orno)
          getDatas(orno)
        }
      }else{
        allowedOrders.add(orno)
        getDatas(orno)
      }
    }

    if (!mitploQuery.readAll(mitploRequest, 2, nbMaxRecord, mitploReader)){
    }

    // list out data
    DBAction listQueryEXT050 = database.table("EXT050")
      .index("00")
      .selection(
        "EXUCA4",
        "EXUCA5",
        "EXUCA6",
        "EXZNBC"
      )
      .build()

    DBContainer listContainerEXT050 = listQueryEXT050.getContainer()
    listContainerEXT050.set("EXBJNO", jobNumber)

    Closure<?> outData = { DBContainer containerExt050 ->
      String dossierEXT050 = containerExt050.get("EXUCA4")
      String semaineEXT050 = containerExt050.get("EXUCA5")
      String anneeEXT050 = containerExt050.get("EXUCA6")
      String nbConteneurs = containerExt050.get("EXZNBC")
      mi.outData.put("UCA4", dossierEXT050)
      mi.outData.put("UCA5", semaineEXT050)
      mi.outData.put("UCA6", anneeEXT050)
      mi.outData.put("ZNBC", nbConteneurs)
      mi.write()
    }

    //Record exists
    if (!listQueryEXT050.readAll(listContainerEXT050, 1, nbMaxRecord, outData)){
    }

    // delete workfile
    DBAction delQuery = database.table("EXT050").index("00").build()
    DBContainer delContainerEXT050 = delQuery.getContainer()
    delContainerEXT050.set("EXBJNO", jobNumber)

    Closure<?> deleteCallBack = { LockedResult lockedResult ->
      lockedResult.delete()
    }

    if(!delQuery.readAllLock(delContainerEXT050, 1, deleteCallBack)){
    }
  }

  public void getDatas(String orno){
    Map<String, String> ooheadData = getOohead(orno)
    if(ooheadData){
      String ooheadUca4 = ooheadData["OAUCA4"] as String
      String ooheadUca5 = ooheadData["OAUCA5"] as String
      String ooheadUca6 = ooheadData["OAUCA6"] as String

      if(uca6Input != ""){
        if (((ooheadUca6 == uca6Input) && ooheadUca6 != "") && ((ooheadUca5 == uca5Input || "" == uca5Input) && ooheadUca5 != "") && ((ooheadUca4 == uca4Input || "" == uca4Input) && ooheadUca4 != "") ) {
          int znbcT = getDradtr(ooheadUca4, ooheadData["OAUCA5"] as String, ooheadData["OAUCA6"] as String)
          if (znbcT == znbcInput || znbcInput == -1l) {
            addExt050(ooheadUca4, ooheadData["OAUCA5"] as String, ooheadData["OAUCA6"] as String)
          }
        }
      }else if(uca5Input != ""){
        if (((ooheadUca5 == uca5Input || "" == uca5Input) && ooheadUca5 != "") && ((ooheadUca4 == uca4Input || "" == uca4Input) && ooheadUca4 != "") ) {
          int znbcT = getDradtr(ooheadUca4, ooheadData["OAUCA5"] as String, ooheadData["OAUCA6"] as String)
          if (znbcT == znbcInput || znbcInput == -1l) {
            addExt050(ooheadUca4, ooheadData["OAUCA5"] as String, ooheadData["OAUCA6"] as String)
          }
        }
      }else{
        if ((ooheadUca4 == uca4Input || "" == uca4Input) && ooheadUca4 != "") {
          int znbcT = getDradtr(ooheadUca4, ooheadData["OAUCA5"] as String, ooheadData["OAUCA6"] as String)
          if (znbcT == znbcInput || znbcInput == -1l) {
            addExt050(ooheadUca4, ooheadData["OAUCA5"] as String, ooheadData["OAUCA6"] as String)
          }
        }
      }
    }
  }

  /**
   * Read OOHEAD data
   * @param orno
   * @return
   */
  public Map<String, String> getOohead(String orno) {
    Map<String, String> returnValue = [
      "OAUCA4" : ""
      ,"OAUCA5": ""
      ,"OAUCA6": ""
      ,"OAUDN1": ""
    ]

    ExpressionFactory ooheadExp = database.getExpressionFactory("OOHEAD")
    ooheadExp = ooheadExp.lt("OAUDN1", "1")
    DBAction ooheadQuery = database.table("OOHEAD").matching(ooheadExp).index("00").selection(
      "OACONO"
      ,"OAORNO"
      ,"OAUCA4"
      ,"OAUCA5"
      ,"OAUCA6"
      ,"OAUDN1"
    ).build()
    DBContainer ooheadRequest = ooheadQuery.getContainer()
    ooheadRequest.set("OACONO", currentCompany)
    ooheadRequest.set("OAORNO", orno)
    if (ooheadQuery.read(ooheadRequest)) {
      if(ooheadRequest){
        String udn1 = ooheadRequest.get("OAUDN1") as String
        String uca4 = ooheadRequest.get("OAUCA4") as String
        String uca5 = ooheadRequest.get("OAUCA5") as String
        String uca6 = ooheadRequest.get("OAUCA6") as String

        returnValue["OAUCA4"] = uca4.trim()
        returnValue["OAUCA5"] = uca5.trim()
        returnValue["OAUCA6"] = uca6.trim()
        returnValue["OAUDN1"] = udn1.trim()
        return returnValue
      }
    }
  }

  /**
   * Get DRADTR data
   * @param dossier
   * @param semaine
   * @param annee
   * @return
   */
  public int getDradtr(String dossier, String semaine, String annee) {

    List listConn = new LinkedList()
    znbcDradtr = 0
    ExpressionFactory expressionDradtr = database.getExpressionFactory("DRADTR")
    expressionDradtr = expressionDradtr.eq("DRUDE1", dossier)
    expressionDradtr = expressionDradtr.and(expressionDradtr.eq("DRUDE2", semaine))
    expressionDradtr = expressionDradtr.and(expressionDradtr.eq("DRUDE3", annee))

    DBAction queryDradtr = database.table("DRADTR").index("00").matching(expressionDradtr).selection("DRCONN").build()
    DBContainer DRADTR = queryDradtr.getContainer()
    DRADTR.set("DRCONO", currentCompany)
    DRADTR.set("DRTLVL", 1)

    Closure<?> dradtrData = { DBContainer containerDradtr ->
      String conn = containerDradtr.get("DRCONN") as String
      if (!listConn.contains(conn)) {
        listConn.add(conn)
        znbcDradtr++
      }
    }
    if(queryDradtr.readAll(DRADTR, 2, nbMaxRecord, dradtrData)){
    }
    return znbcDradtr
  }

  /**
   * Add EXT050 data
   * @param dossier
   * @param semaine
   * @param annee
   */
  public void addExt050(String dossier, String semaine, String annee) {

    //Check if record exists
    DBAction queryExt050 = database.table("EXT050")
      .index("00")
      .selection(
        "EXBJNO",
        "EXCONO",
        "EXUCA4",
        "EXUCA5",
        "EXUCA6",
        "EXZNBC",
        "EXRGDT",
        "EXRGTM",
        "EXLMDT",
        "EXCHNO",
        "EXCHID"
      )
      .build()

    DBContainer containerExt050 = queryExt050.getContainer()
    containerExt050.set("EXBJNO", jobNumber)
    containerExt050.set("EXCONO", currentCompany)
    containerExt050.set("EXUCA4", dossier)
    containerExt050.set("EXUCA5", semaine)
    containerExt050.set("EXUCA6", annee)

    //Record exists
    if (!queryExt050.read(containerExt050)) {
      containerExt050.set("EXBJNO", jobNumber)
      containerExt050.set("EXCONO", currentCompany)
      containerExt050.set("EXUCA4", dossier)
      containerExt050.set("EXUCA5", semaine)
      containerExt050.set("EXUCA6", annee)
      containerExt050.set("EXZNBC", znbcDradtr)
      containerExt050.set("EXRGDT", utility.call("DateUtil", "currentDateY8AsInt"))
      containerExt050.set("EXRGTM", utility.call("DateUtil", "currentTimeAsInt"))
      containerExt050.set("EXLMDT", utility.call("DateUtil", "currentDateY8AsInt"))
      containerExt050.set("EXCHNO", 1)
      containerExt050.set("EXCHID", program.getUser())
      queryExt050.insert(containerExt050)
    }
  }
}
