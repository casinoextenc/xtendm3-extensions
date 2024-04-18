/**
 * README
 * This extension is submitted by an API
 *
 * Name : EXT014
 * Description : batch template
 * Date         Changed By   Description
 * 20231010     PBEAUDOUIN     Constraint engine
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class EXT014 extends ExtendM3Batch {
  private final LoggerAPI logger
  private final DatabaseAPI database
  private final ProgramAPI program
  private final BatchAPI batch
  private final MICallerAPI miCaller
  private final TextFilesAPI textFiles
  private final UtilityAPI utility
  private int currentCompany
  private Map<String, String> documents_EXT014
  //Objects to store informations
  //Used to store order infos
  private def datasORDER
  //Used to store order line infos
  private def datasLINE
  //Used to store list of documents for order line infos
  private Map<String, String> documents


  public EXT014(LoggerAPI logger, DatabaseAPI database, ProgramAPI program, BatchAPI batch, MICallerAPI miCaller, TextFilesAPI textFiles, UtilityAPI utility) {
    this.logger = logger
    this.database = database
    this.program = program
    this.batch = batch
    this.miCaller = miCaller
    this.textFiles = textFiles
    this.utility = utility
  }

  /**
   * Main method
   * retrieve job datas then call performActualJob
   */
  public void main() {

    // Get job number
    LocalDateTime timeOfCreation = LocalDateTime.now()
    String jobNumber = program.getJobNumber() + timeOfCreation.format(DateTimeFormatter.ofPattern("yyMMdd")) + timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss"))
    if (batch.getReferenceId().isPresent()) {
      Optional<String> data = getJobData(batch.getReferenceId().get())
      performActualJob(data)
    } else {
    }

    deleteEXTJOB()
  }
  /**
   * Get job datas in EXTJOB table
   * @param referenceId
   * @return
   */
  private Optional<String> getJobData(String referenceId) {
    def query = database.table("EXTJOB").index("00").selection("EXDATA").build()
    def container = query.createContainer()
    container.set("EXRFID", referenceId)
    if (query.read(container)) {
      return Optional.of(container.getString("EXDATA"))
    } else {
      //logger.debug("EXTJOB not found")
    }
    return Optional.empty()
  }
  /**
   * @param data
   * @return
   */
  private performActualJob(Optional<String> data) {
    currentCompany = (Integer) program.getLDAZD().CONO
    //Read job inputs
    if (!data.isPresent()) {
      return
    }
    String rawData = data.get()
    String[] rowParms = rawData.split(";")
    String ponr = ""
    String posx = ""
    int intPONR = 0
    int intPOSX = 0

    String inORNO = rowParms[0]

    logger.debug("Perform job orno=${inORNO}")

    //Read OOHEAD
    ExpressionFactory OOHEAD_expression = database.getExpressionFactory("OOHEAD")
    OOHEAD_expression = OOHEAD_expression.lt("OAORST", "99")

    DBAction OOHEAD_query = database.table("OOHEAD")
      .index("00")
      .matching(OOHEAD_expression)
      .selection("OAORNO"
        , "OACUNO"
        , "OACUOR"
        , "OAORDT"
        , "OAWHLO"
        , "OAUCA4"
        , "OAUCA5"
        , "OAUCA6"
        , "OAORTP"
        , "OATEDL"
        , "OAVOL3"
        , "OAGRWE"
        , "OANEWE"
        , "OANTAM")
      .build()
    DBContainer OOHEAD_request = OOHEAD_query.getContainer()
    OOHEAD_request.set("OAORNO", inORNO)
    OOHEAD_request.set("OACONO", currentCompany)


    //nb keys for OOHEAD read all
    int nbk = 2

    if (!OOHEAD_query.readAll(OOHEAD_request, nbk, performOOHEADJob)) {

    }
  }
  Closure<?> EXT013_reader = { DBContainer EXT13_result ->
    String lino = EXT13_result.get("EXLINO")
    String fitn = EXT13_result.get("EXFITN")
    String mscd = EXT13_result.get("EXMSCD")
    String remk = EXT13_result.get("EXREMK")
    datasLINE["POSX"] = posx
    datasLINE["EXLINO"] = lino
    datasLINE["EXFITN"] = fitn
    datasLINE["EXMSCD"] = mscd
    datasLINE["EXREMK"] = remk
    logger.debug("performEXT013 lino=${lino} mscd=${mscd} remk=${remk}")





  }
    /**
     * Perform treatment per OOHEAD
     */
    Closure<?> performOOHEADJob = { DBContainer OOHEAD_result ->
      String orno = OOHEAD_result.get("OAORNO")


      String cuno = OOHEAD_result.get("OACUNO")
      String cuor = OOHEAD_result.get("OACUOR")
      String ordt = OOHEAD_result.get("OAORDT")
      String whlo = OOHEAD_result.get("OAWHLO")
      String uca4 = OOHEAD_result.get("OAUCA4")
      String uca5 = OOHEAD_result.get("OAUCA5")
      String uca6 = OOHEAD_result.get("OAUCA6")
      String ortp = OOHEAD_result.get("OAORTP")
      String tedl = OOHEAD_result.get("OATEDL")
      String vol3 = OOHEAD_result.get("OAVOL3")
      String grwe = OOHEAD_result.get("OAGRWE")
      String newe = OOHEAD_result.get("OANEWE")
      String ntam = OOHEAD_result.get("OANTAM")
      logger.debug("Closure orno=${orno} whlo=${whlo} cuno=${cuno}")

      documents = new LinkedHashMap<String, String>()
      documents_EXT014 = new LinkedHashMap<String, String>()

      datasORDER = [
        "ORNO"  : ""
        , "CUNO": ""
        , "CUOR": ""
        , "ORDT": ""
        , "WHLO": ""
        , "UCA4": ""
        , "UCA5": ""
        , "UCA6": ""
        , "ORTP": ""
        , "TEDL": ""
        , "VOL3": ""
        , "GRWE": ""
        , "NEWE": ""
        , "NTAM": ""

      ]

      datasORDER["CUNO"] = cuno
      datasORDER["CUOR"] = cuor
      datasORDER["ORDT"] = ordt
      datasORDER["WHLO"] = whlo
      datasORDER["UCA4"] = uca4
      datasORDER["UCA5"] = uca5
      datasORDER["UCA6"] = uca6
      datasORDER["ORTP"] = ortp
      datasORDER["TEDL"] = tedl
      datasORDER["VOL3"] = vol3
      datasORDER["GRWE"] = grwe
      datasORDER["NEWE"] = newe
      datasORDER["NTAM"] = ntam

      // Read OOline

      ExpressionFactory OOLINE_expression = database.getExpressionFactory("OOLINE")
      OOLINE_expression = OOLINE_expression.lt("OBORST", "99")

      DBAction OOLINE_query = database.table("OOLINE")
        .index("00")
        .matching(OOLINE_expression)
        .selection("OBORNO"
          , "OBPONR"
          , "OBPOSX"
          , "OBORST"
          , "OBRSC1"
          , "OBRSCD"
          , "OBREPI"
          , "OBITNO"
          , "OBORQT"
          , "OBNEPR"
          , "OBUDN6"
          , "OBLNAM")
        .build()


      DBContainer OOLINE_ = OOLINE_query.getContainer()
      OOLINE_.set("OBCONO", currentCompany)
      OOLINE_.set("OBORNO", orno)
      if (!OOLINE_query.readAll(OOLINE_, 2, OOLINE_reader)) {

      }


    }
  Closure<?> OOLINE_reader = { DBContainer OOLINE_result ->
    ponr = OOLINE_result.get("OBPONR")
    posx = OOLINE_result.get("OBPOSX")

    try {
      intPONR = Integer.parseInt(ponr)
    } catch (NumberFormatException e) {
      intPONR = 0
    }
    try {
      intPOSX = Integer.parseInt(posx)
    } catch (NumberFormatException e) {
      intPOSX = 0

    }
    String orst = OOLINE_result.get("OBORST")
    String rsc1 = OOLINE_result.get("OBRSC1")
    String rscd = OOLINE_result.get("OBRSCD")
    String repi = OOLINE_result.get("OBREPI")
    String itno = OOLINE_result.get("OBITNO")
    String orqt = OOLINE_result.get("OBORQT")
    String nepr = OOLINE_result.get("OBNEPR")
    String udn6 = OOLINE_result.get("OBUDN6")
    String lnam = OOLINE_result.get("OBLNAM")
    logger.debug("performOOHEADJob orno=${orno} ponr=${ponr} itno=${itno}")
    if (ponr != 0) {

      DBAction query_EXT013 = database.table("EXT013").index("00")
        .selection(
          "EXCONO"
          , "EXORNO"
          , "EXPONR"
          , "EXPOSX"
          , "EXLINO"
          , "EXFITN"
          , "EXMSCD"
          , "EXREMK"

        ).build()

      DBContainer EXT013 = query_EXT013.getContainer()
      EXT013.set("EXCONO", currentCompany)
      EXT013.set("EXORNO", orno)
      EXT013.set("EXPONR", intPONR)
      EXT013.set("EXPOSX", intPOSX)
      if (!query_EXT013.readAll(EXT013, 4, EXT013_reader)) {

      }

    }



  }

  /**
   * Delete job informations in EXTJOB
   * @param data
   * @return
   */

  public void deleteEXTJOB() {
    LocalDateTime timeOfCreation = LocalDateTime.now()
    DBAction query = database.table("EXTJOB").index("00").build()
    DBContainer EXTJOB = query.getContainer()
    EXTJOB.set("EXRFID", batch.getReferenceId().get())

    Closure<?> updateCallBack_EXTJOB = { LockedResult lockedResult ->
      lockedResult.delete()
    }

    if (!query.readAllLock(EXTJOB, 1, updateCallBack_EXTJOB)) {
    }
  }
}


