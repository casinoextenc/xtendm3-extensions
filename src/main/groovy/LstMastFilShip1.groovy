/**
 * README
 * This extension is used by Mashup
 *
 * Name : EXT050MI.LstMastFilShip1
 * Description : List master file shipment
 * Date         Changed By   Description
 * 20230511     SEAR         LOG28 - Creation of files and containers
 * 20230818     MLECLERCQ    LOG28 - Correction ZNBC filter for 0 <> null
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class LstMastFilShip1 extends ExtendM3Transaction {
  private final MIAPI mi
  private final LoggerAPI logger
  private final ProgramAPI program
  private final DatabaseAPI database
  private final SessionAPI session
  private final TransactionAPI transaction
  private final MICallerAPI miCaller
  private final UtilityAPI utility

  int currentCompany

  private String parm
  private boolean validOrder
  private boolean sameWarehouse
  private boolean sameZNBC
  private String whloOoline
  private String whloInput
  private String uca4Input
  private String uca5Input
  private String uca6Input
  private Long znbcInput
  private Long znbcDradtr

  private String jobNumber
  private Integer nbMaxRecord = 10000

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

    logger.debug("UCA5 : ${uca5Input}, UCA6 : ${uca6Input}")

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
    String tmpORNO = ""
    // TODO AMELIORER FILTRE
    logger.debug("ameliorer filtre")
    ExpressionFactory oolineExp = database.getExpressionFactory("OOLINE")
    oolineExp = oolineExp.eq("OBWHLO", whloInput)
    oolineExp = oolineExp.and(oolineExp.lt("OBORST", "44"))

    DBAction oolineQuery = database.table("OOLINE").index("00").matching(oolineExp).selection("OBWHLO").build()
    DBContainer oolineRequest = oolineQuery.getContainer()
    oolineRequest.set("OBCONO", currentCompany)

    Closure<?> oolineReader = { DBContainer oolineResult ->
      String orno = oolineResult.get("OBORNO")
      if (orno != tmpORNO){
        Map<String, String> ooheadData = getOOHEAD(orno)
        if(ooheadData){
          String ooheadUca4 = ooheadData["OAUCA4"] as String
          String ooheadUca5 = ooheadData["OAUCA5"] as String
          String ooheadUca6 = ooheadData["OAUCA6"] as String
          logger.debug("orno ${orno} uca4 ${ooheadUca4}")

          if(uca6Input != ""){
            if (((ooheadUca6 == uca6Input) && ooheadUca6 != "") && ((ooheadUca5 == uca5Input || "" == uca5Input) && ooheadUca5 != "") && ((ooheadUca4 == uca4Input || "" == uca4Input) && ooheadUca4 != "") ) {
              int znbcT = getDRADTR(ooheadUca4, ooheadData["OAUCA5"] as String, ooheadData["OAUCA6"] as String)
              if (znbcT == znbcInput || znbcInput == -1l) {
                addEXT050(ooheadUca4, ooheadData["OAUCA5"] as String, ooheadData["OAUCA6"] as String)
              }
            }
          }else if(uca5Input != ""){
            if (((ooheadUca5 == uca5Input || "" == uca5Input) && ooheadUca5 != "") && ((ooheadUca4 == uca4Input || "" == uca4Input) && ooheadUca4 != "") ) {
              int znbcT = getDRADTR(ooheadUca4, ooheadData["OAUCA5"] as String, ooheadData["OAUCA6"] as String)
              if (znbcT == znbcInput || znbcInput == -1l) {
                addEXT050(ooheadUca4, ooheadData["OAUCA5"] as String, ooheadData["OAUCA6"] as String)
              }
            }
          }else{
            if ((ooheadUca4 == uca4Input || "" == uca4Input) && ooheadUca4 != "") {
              int znbcT = getDRADTR(ooheadUca4, ooheadData["OAUCA5"] as String, ooheadData["OAUCA6"] as String)
              if (znbcT == znbcInput || znbcInput == -1l) {
                addEXT050(ooheadUca4, ooheadData["OAUCA5"] as String, ooheadData["OAUCA6"] as String)
              }
            }
          }
        }

        tmpORNO = orno
      }
    }

    if (!oolineQuery.readAll(oolineRequest, 1, nbMaxRecord, oolineReader)){
    }

    // list out data
    DBAction ListqueryEXT050 = database.table("EXT050")
      .index("00")
      .selection(
        "EXUCA4",
        "EXUCA5",
        "EXUCA6",
        "EXZNBC"
      )
      .build()

    DBContainer ListContainerEXT050 = ListqueryEXT050.getContainer()
    ListContainerEXT050.set("EXBJNO", jobNumber)

    Closure<?> outData = { DBContainer containerEXT050 ->
      String dossierEXT050 = containerEXT050.get("EXUCA4")
      String semaineEXT050 = containerEXT050.get("EXUCA5")
      String anneeEXT050 = containerEXT050.get("EXUCA6")
      String nbConteneurs = containerEXT050.get("EXZNBC")
      mi.outData.put("UCA4", dossierEXT050)
      mi.outData.put("UCA5", semaineEXT050)
      mi.outData.put("UCA6", anneeEXT050)
      mi.outData.put("ZNBC", nbConteneurs)
      mi.write()
    }

    //Record exists
    if (!ListqueryEXT050.readAll(ListContainerEXT050, 1, nbMaxRecord, outData)){
    }

    // delete workfile
    DBAction DelQuery = database.table("EXT050").index("00").build()
    DBContainer DelcontainerEXT050 = DelQuery.getContainer()
    DelcontainerEXT050.set("EXBJNO", jobNumber)

    Closure<?> deleteCallBack = { LockedResult lockedResult ->
      lockedResult.delete()
    }

    if(!DelQuery.readAllLock(DelcontainerEXT050, 1, deleteCallBack)){
    }
  }

  /**
   * Read OOHEAD data
   * @param orno
   * @return
   */
  public Map<String, String> getOOHEAD(String orno) {
    logger.debug("getoohead ${orno}")

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

        logger.debug("UCA4 : ${uca4}, UDN1: ${udn1}")
        returnValue["OAUCA4"] = uca4.trim()
        returnValue["OAUCA5"] = uca5.trim()
        returnValue["OAUCA6"] = uca6.trim()
        returnValue["OAUDN1"] = udn1.trim()
        logger.debug("ret " + returnValue)
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
  public int getDRADTR(String dossier, String semaine, String annee) {
    logger.debug("getDRADTR ${dossier} ${semaine} ${annee}")

    List listCONN = new LinkedList()
    znbcDradtr = 0
    ExpressionFactory expressionDradtr = database.getExpressionFactory("DRADTR")
    expressionDradtr = expressionDradtr.eq("DRUDE1", dossier)
    expressionDradtr = expressionDradtr.and(expressionDradtr.eq("DRUDE2", semaine))
    expressionDradtr = expressionDradtr.and(expressionDradtr.eq("DRUDE3", annee))

    DBAction queryDradtr = database.table("DRADTR").index("00").matching(expressionDradtr).selection("DRCONN").build()
    DBContainer DRADTR = queryDradtr.getContainer()
    DRADTR.set("DRCONO", currentCompany)
    DRADTR.set("DRTLVL", 1)

    Closure<?> DRADTRData = { DBContainer ContainerDRADTR ->
      String conn = ContainerDRADTR.get("DRCONN") as String
      if (!listCONN.contains(conn)) {
        listCONN.add(conn)
        znbcDradtr++
      }
    }
    if(queryDradtr.readAll(DRADTR, 2, nbMaxRecord, DRADTRData)){
    }
    return znbcDradtr
  }

  /**
   * Add EXT050 data
   * @param dossier
   * @param semaine
   * @param annee
   */
  public void addEXT050(String dossier, String semaine, String annee) {
    logger.debug("EXT050 ${dossier} ${semaine} ${annee}")

    //Check if record exists
    DBAction queryEXT050 = database.table("EXT050")
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

    DBContainer containerEXT050 = queryEXT050.getContainer()
    containerEXT050.set("EXBJNO", jobNumber)
    containerEXT050.set("EXCONO", currentCompany)
    containerEXT050.set("EXUCA4", dossier)
    containerEXT050.set("EXUCA5", semaine)
    containerEXT050.set("EXUCA6", annee)

    //Record exists
    if (!queryEXT050.read(containerEXT050)) {
      containerEXT050.set("EXBJNO", jobNumber)
      containerEXT050.set("EXCONO", currentCompany)
      containerEXT050.set("EXUCA4", dossier)
      containerEXT050.set("EXUCA5", semaine)
      containerEXT050.set("EXUCA6", annee)
      containerEXT050.set("EXZNBC", znbcDradtr)
      containerEXT050.set("EXRGDT", utility.call("DateUtil", "currentDateY8AsInt"))
      containerEXT050.set("EXRGTM", utility.call("DateUtil", "currentTimeAsInt"))
      containerEXT050.set("EXLMDT", utility.call("DateUtil", "currentDateY8AsInt"))
      containerEXT050.set("EXCHNO", 1)
      containerEXT050.set("EXCHID", program.getUser())
      queryEXT050.insert(containerEXT050)
    }
  }
}
