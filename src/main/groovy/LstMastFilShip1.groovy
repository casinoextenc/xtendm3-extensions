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
  private String whlo_OOLINE
  private String whlo_input
  private String uca4_input
  private Long znbc_input
  private Long znbc_DRADTR

  private String jobNumber

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
    whlo_input = (mi.in.get("WHLO") != null ? (String)mi.in.get("WHLO") : "")
    uca4_input = (mi.in.get("UCA4") != null ? (String)mi.in.get("UCA4") : "")
    znbc_input = (Long)(mi.in.get("ZNBC") != null ? mi.in.get("ZNBC") : -1)

    // check warehouse
    DBAction query_MITWHL = database.table("MITWHL").index("00").selection("MWWHLO").build()
    DBContainer MITWHL = query_MITWHL.getContainer()
    MITWHL.set("MWCONO", currentCompany)
    MITWHL.set("MWWHLO", whlo_input)
    if(!query_MITWHL.read(MITWHL)){
      mi.error("Le dépôt " + whlo_input + " n'existe pas")
      return
    }

    // Get OOLINE
    String tmpORNO = ""
    // TODO AMELIORER FILTRE
    logger.debug("ameliorer filtre")
    ExpressionFactory OOLINE_exp = database.getExpressionFactory("OOLINE")
    OOLINE_exp = OOLINE_exp.eq("OBWHLO", whlo_input)
    OOLINE_exp = OOLINE_exp.and(OOLINE_exp.lt("OBORST", "44"))

    DBAction OOLINE_query = database.table("OOLINE").index("00").matching(OOLINE_exp).selection("OBWHLO").build()
    DBContainer OOLINE_request = OOLINE_query.getContainer()
    OOLINE_request.set("OBCONO", currentCompany)
    
    
    Closure<?> OOLINE_reader = { DBContainer OOLINE_result ->
      String orno = OOLINE_result.get("OBORNO")
      if (orno != tmpORNO){
        def oohead_data = getOOHEAD(orno)
        String oohead_uca4 = oohead_data["OAUCA4"] as String
        logger.debug("orno ${orno} uca4 ${oohead_uca4}")
        if ((oohead_uca4 == uca4_input || "" == uca4_input) && oohead_uca4 != "") {
          int znbc_t = getDRADTR(oohead_uca4, oohead_data["OAUCA5"] as String, oohead_data["OAUCA6"] as String)
          if (znbc_t == znbc_input || znbc_input == -1l) {
            addEXT050(oohead_uca4, oohead_data["OAUCA5"] as String, oohead_data["OAUCA6"] as String)
          }
        }
        tmpORNO = orno
      }
    }


    if (!OOLINE_query.readAll(OOLINE_request, 1, OOLINE_reader)){
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
    if (!ListqueryEXT050.readAll(ListContainerEXT050, 1, outData)){
    }

    // delete workfile
    DBAction DelQuery = database.table("EXT050").index("00").build()
    DBContainer DelcontainerEXT050 = DelQuery.getContainer()
    DelcontainerEXT050.set("EXBJNO", jobNumber)

    Closure<?> deleteCallBack = { LockedResult lockedResult ->
      lockedResult.delete()
    }


    if(!DelQuery.readAllLock(DelcontainerEXT050, 1, deleteCallBack)){
      //mi.error("L'enregistrement n'existe pas")
      //return
    }
  }


  /**
   * Read OOHEAD data
   * @param orno
   * @return
   */
  public def getOOHEAD(String orno) {
    logger.debug("getoohead ${orno}")

    def return_value = [
      "UCA4" : ""
      ,"UCA5": ""
      ,"UCA6": ""
    ]


    DBAction OOHEAD_query = database.table("OOHEAD").index("00").selection(
        "OACONO"
        ,"OAORNO"
        ,"OAUCA4"
        ,"OAUCA5"
        ,"OAUCA6"
        ).build()
    DBContainer OOHEAD_request = OOHEAD_query.getContainer()
    OOHEAD_request.set("OACONO", currentCompany)
    OOHEAD_request.set("OAORNO", orno)
    if (OOHEAD_query.read(OOHEAD_request)) {
      String uca4 = OOHEAD_request.get("OAUCA4") as String
      String uca5 = OOHEAD_request.get("OAUCA5") as String
      String uca6 = OOHEAD_request.get("OAUCA6") as String
      
      
      return_value["OAUCA4"] = uca4.trim()
      return_value["OAUCA5"] = uca5.trim()
      return_value["OAUCA6"] = uca6.trim()
      logger.debug("ret " + return_value)
      return return_value
    }
  }

  /**
   * @param dossier
   * @param semaine
   * @param annee
   * @return
   */
  public int getDRADTR(String dossier, String semaine, String annee) {
    logger.debug("getDRADTR ${dossier} ${semaine} ${annee}")
    
    List listCONN = new LinkedList()
    znbc_DRADTR = 0
    ExpressionFactory expression_DRADTR = database.getExpressionFactory("DRADTR")
    expression_DRADTR = expression_DRADTR.eq("DRUDE1", dossier)
    expression_DRADTR = expression_DRADTR.and(expression_DRADTR.eq("DRUDE2", semaine))
    expression_DRADTR = expression_DRADTR.and(expression_DRADTR.eq("DRUDE3", annee))

    DBAction query_DRADTR = database.table("DRADTR").index("00").matching(expression_DRADTR).selection("DRCONN").build()
    DBContainer DRADTR = query_DRADTR.getContainer()
    DRADTR.set("DRCONO", currentCompany)
    DRADTR.set("DRTLVL", 1)

    Closure<?> DRADTRData = { DBContainer ContainerDRADTR ->
      String conn = ContainerDRADTR.get("DRCONN") as String
      if (!listCONN.contains(conn)) {
        listCONN.add(conn)
        znbc_DRADTR++
      }
    }
    if(query_DRADTR.readAll(DRADTR, 2, DRADTRData)){
    }
    return znbc_DRADTR
  }

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
      containerEXT050.set("EXZNBC", znbc_DRADTR)
      containerEXT050.set("EXRGDT", utility.call("DateUtil", "currentDateY8AsInt"))
      containerEXT050.set("EXRGTM", utility.call("DateUtil", "currentTimeAsInt"))
      containerEXT050.set("EXLMDT", utility.call("DateUtil", "currentDateY8AsInt"))
      containerEXT050.set("EXCHNO", 1)
      containerEXT050.set("EXCHID", program.getUser())
      queryEXT050.insert(containerEXT050)
    }
  }
}

