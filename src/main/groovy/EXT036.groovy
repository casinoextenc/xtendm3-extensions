/**
 * README
 * This extension is submitted by an API
 *
 * Name : EXT036
 * Description : batch template
 * Date         Changed By   Description
 * 20230210     ARENARD      QUAX02 - Constraint engine
 * 20230411     ARENARD      For following criterias, 2 is considered as an empty value : EXHAZI, EXZALC, EXZSAN, EXZALI, EXZORI, EXZPHY
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class EXT036 extends ExtendM3Batch {
  private final LoggerAPI logger
  private final DatabaseAPI database
  private final ProgramAPI program
  private final BatchAPI batch
  private final MICallerAPI miCaller
  private final TextFilesAPI textFiles
  private final UtilityAPI utility

  private int currentCompany

  //Objects to store informations
  //Used to store order infos
  private def datasORDER
  //Used to store order line infos
  private def datasLINE
  //Used to store item infos
  private def datasITEM
  //Used to store list of documents for order line infos
  private Map<String, String> documents
  private Map<String, String> documents_EXT036

  public EXT036(LoggerAPI logger, DatabaseAPI database, ProgramAPI program, BatchAPI batch, MICallerAPI miCaller, TextFilesAPI textFiles, UtilityAPI utility) {
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


    String inORNO = rowParms[0]
    String inAPONR = rowParms.length > 1 ? rowParms[1] : ""
    String inAPOSX = rowParms.length > 2 ? rowParms[2] : ""
    int inPONR = 0
    int inPOSX = 0
    try {
      inPONR = Integer.parseInt(inAPONR)
    } catch (NumberFormatException e) {
      inPONR = 0
    }
    try {
      inPOSX = Integer.parseInt(inAPOSX)
    } catch (NumberFormatException e) {
      inPOSX = 0
    }
    logger.debug("Perform job orno=${inORNO} ponr=${inAPONR} posx=${inAPOSX}")

    //Read OOLINE
    ExpressionFactory OOLINE_expression = database.getExpressionFactory("OOLINE")
    OOLINE_expression = OOLINE_expression.le("OBORST", "77")
	OOLINE_expression = OOLINE_expression.or(OOLINE_expression.eq("OBORST", "99"))

    DBAction OOLINE_query = database.table("OOLINE")
      .index("00")
      .matching(OOLINE_expression)
      .selection("OBORNO"
        , "OBPONR"
        , "OBPOSX"
        , "OBCUNO"
        , "OBITNO"
        , "OBADID"
        , "OBFACI"
        , "OBWHLO"
        , "OBORQT"
        , "OBLNAM"
        , "OBORST"
        , "OBROUT"
        , "OBCHID"
        , "OBALQT")
      .build()
    DBContainer OOLINE_request = OOLINE_query.getContainer()
    OOLINE_request.set("OBCONO", currentCompany)
    OOLINE_request.set("OBORNO", inORNO)
    OOLINE_request.set("OBPONR", inPONR)
    OOLINE_request.set("OBPOSX", inPOSX)

    //nb keys for OOLINE read all
    int nbk = inPONR == 0 ? 2 : 4

    if (!OOLINE_query.readAll(OOLINE_request, nbk, performOOLINEJob)) {
      // TODO WTF
    }
  }

  /**
   * Perform treatment per OOLINE
   */
  Closure<?> performOOLINEJob = { DBContainer OOLINE_result ->
    String orno = OOLINE_result.get("OBORNO")
    int ponr = OOLINE_result.get("OBPONR") as Integer
    int posx = OOLINE_result.get("OBPOSX") as Integer
    String cuno = OOLINE_result.get("OBCUNO")
    String itno = OOLINE_result.get("OBITNO")
    String faci = OOLINE_result.get("OBFACI")
    double orqt = OOLINE_result.get("OBORQT") as Double
    double alqt = OOLINE_result.get("OBALQT") as Double
    String orst = OOLINE_result.get("OBORST")
    String chid = OOLINE_result.get("OBCHID")

    logger.debug("performOOLINEJob orno=${orno} ponr=${ponr} posx=${posx}")

    documents = new LinkedHashMap<String, String>()
    documents_EXT036 = new LinkedHashMap<String, String>()

    datasLINE = [
      "ORNO"  : ""
      , "PONR": ""
      , "POSX": ""
      , "FACI": ""
      , "WHLO": ""
      , "ITNO": ""
      , "POPN": ""
      , "ORQT": ""
      , "ALQT": ""
      , "ORST": ""
      , "CHID": ""
      , "ZCLI": "0"
      , "DLIX": "0"
      , "CONN": "0"
    ]

    datasLINE["ORNO"] = orno
    datasLINE["PONR"] = ponr as String
    datasLINE["POSX"] = posx as String
    datasLINE["CUNO"] = cuno
    datasLINE["ITNO"] = itno
    datasLINE["FACI"] = faci
    datasLINE["ORQT"] = orqt as String
    datasLINE["ALQT"] = alqt as String
    datasLINE["ORST"] = orst
    datasLINE["CHID"] = chid
    datasLINE["ZCLI"] = ""

    // Read delivery lines
    long dlix = 0
    long conn = 0
    DBAction query_MHDISL = database.table("MHDISL").index("10").selection("URDLIX").build()
    DBContainer MHDISL_request = query_MHDISL.getContainer()
    MHDISL_request.set("URCONO", currentCompany)
    MHDISL_request.set("URRORC", 3)
    MHDISL_request.set("URRIDN", orno)
    MHDISL_request.set("URRIDL", ponr)
    MHDISL_request.set("URRIDX", posx)

    Closure<?> MHDISL_reader = { DBContainer MHDISL_result ->
      dlix = MHDISL_result.get("URDLIX") as Long
    }

    if(!query_MHDISL.readAll(MHDISL_request, 4, 1, MHDISL_reader)){
    }
    if(dlix != 0) {
      DBAction query_MHDISH = database.table("MHDISH").index("00").selection("OQCONN").build()
      DBContainer MHDISH_request = query_MHDISH.getContainer()
      MHDISH_request.set("OQCONO", currentCompany)
      MHDISH_request.set("OQINOU",  1)
      MHDISH_request.set("OQDLIX",  dlix)
      if(query_MHDISH.read(MHDISH_request)) {
        conn = MHDISH_request.get("OQCONN") as Long
      }
    }
    datasLINE["DLIX"] = dlix as String
    datasLINE["CONN"] = conn as String

    getLineCriterias()

    getLineEXT030()

    getLineEXT036()

    mngEXT036()

  }

  /**
   *
   */
  public void mngEXT036(){
    String orno = datasLINE["ORNO"]
    int ponr = datasLINE["PONR"] as Integer
    int posx = datasLINE["POSX"] as Integer
    String cuno = datasLINE["CUNO"] as String
    String itno = datasLINE["ITNO"]
    String faci = datasLINE["FACI"]
    double orqt = datasLINE["ORQT"] as Double
    double alqt = datasLINE["ALQT"] as Double
    String orst = datasLINE["ORST"]
    String chid = datasLINE["CHID"]
    int zcli = datasLINE["ZCLI"] as Integer
    long dlix = datasLINE["DLIX"] as Long
    long conn = datasLINE["CONN"] as Long
    logger.debug("mngEXT036 orno=${orno} ponr=${ponr} posx=${posx}")

    String cscd = datasORDER["CSCD"]
    String uca4 = datasORDER["UCA4"]

    String hazi =  datasITEM["HAZI"]
    String hie5 =  datasITEM["HIE5"]
    String cfi4 =  datasITEM["CFI4"]
    String suno =  datasITEM["SUNO"]
    String prod =  datasITEM["PROD"]
    String sig6 =  datasITEM["SIG6"]
    double grwe =  datasITEM["GRWE"] as Double
    double newe =  datasITEM["NEWE"] as Double
    double ztgr =  datasITEM["ZTGR"] as Double
    double ztnw =  datasITEM["ZTNW"] as Double
    String csno =  datasITEM["CSNO"]
    String orco =  datasITEM["ORCO"]
    String zali =  datasITEM["ZALI"]
    String zalc =  datasITEM["ZALC"]
    String zsan =  datasITEM["ZSAN"]
    String zcap =  datasITEM["ZCAP"]
    String zca1 =  datasITEM["ZCA1"]
    String zca2 =  datasITEM["ZCA2"]
    String zca3 =  datasITEM["ZCA3"]
    String zca4 =  datasITEM["ZCA4"]
    String zca5 =  datasITEM["ZCA5"]
    String zca6 =  datasITEM["ZCA6"]
    String zca7 =  datasITEM["ZCA7"]
    String zca8 =  datasITEM["ZCA8"]
    String zori =  datasITEM["ZORI"]
    String zphy =  datasITEM["ZPHY"]
    int zagr =  datasITEM["ZAGR"] as Integer
    String znag =  datasITEM["ZNAG"]
    double zqco  =  datasITEM["ZQCO"] as Double


    for (key in documents_EXT036.keySet()) {
      String value = documents_EXT036.get(key)
      String[] vt = value.split("#")
      int tcli = vt[0] as Integer
      String stat = vt[1]
      logger.debug("Log mngEXT036 - ${key} " + tcli + " " + stat)
      if (!documents.containsKey(key) && stat != "90"){
        logger.debug("deactivate " + key)
        updateEXT036(orno, ponr, posx, tcli, "90", null)
      } else if (documents.containsKey(key)){
        logger.debug("reactivate " + key)
        String zsty = documents.get(key)
        updateEXT036(orno, ponr, posx, tcli, "20", zsty)
      }
    }
    for (key in documents.keySet()) {
      if (!documents_EXT036.containsKey(key)){
        zcli++
        logger.debug("create " + key)
        String[] ks = key.split("#")
        double zcid = ks[0] as Double
        String zcty = ks[1]
        String zcod = ks[2]
        String doid = ks.length > 3 ? ks[3] : ""
        String ads1 = ks.length > 4 ? ks[4] : ""
        String zsty = documents.get(key)


        DBAction EXT036_query = database.table("EXT036").index("00").build()

        DBContainer EXT036_request = EXT036_query.getContainer()
        EXT036_request.set("EXCONO", currentCompany)
        EXT036_request.set("EXORNO", orno)
        EXT036_request.set("EXPONR", ponr)
        EXT036_request.set("EXPOSX", posx)
        EXT036_request.set("EXZCLI", zcli)
        if (!EXT036_query.read(EXT036_request)) {
          EXT036_request.set("EXORST", orst)
          EXT036_request.set("EXSTAT", "20")
          EXT036_request.set("EXCONN", conn)
          EXT036_request.set("EXDLIX", dlix)
          EXT036_request.set("EXUCA4", uca4)
          EXT036_request.set("EXCUNO", cuno)
          EXT036_request.set("EXITNO", itno)
          EXT036_request.set("EXZAGR", zagr)
          EXT036_request.set("EXZNAG", znag)
          EXT036_request.set("EXORQT", orqt)
          EXT036_request.set("EXALQT", alqt)
          EXT036_request.set("EXZQCO", zqco)
          EXT036_request.set("EXZTGR", ztgr)
          EXT036_request.set("EXZTNW", ztnw)
          EXT036_request.set("EXZCID", zcid)
          EXT036_request.set("EXZCOD", zcod)
          EXT036_request.set("EXZCTY", zcty)
          EXT036_request.set("EXDOID", doid)
          EXT036_request.set("EXADS1", ads1)
          EXT036_request.set("EXZSTY", zsty)
          LocalDateTime timeOfCreation = LocalDateTime.now()
          EXT036_request.setInt("EXRGDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
          EXT036_request.setInt("EXRGTM", timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")) as Integer)
          EXT036_request.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
          EXT036_request.setInt("EXCHNO", 1)
          EXT036_request.set("EXCHID", chid)
          EXT036_query.insert(EXT036_request)
        }

      }
    }
  }
  public void updateEXT036(String orno, int ponr, int posx, int zcli, String stat, String zsty) {
    String cuno = datasLINE["CUNO"] as String
    String itno = datasLINE["ITNO"]
    String faci = datasLINE["FACI"]
    double orqt = datasLINE["ORQT"] as Double
    double alqt = datasLINE["ALQT"] as Double
    String orst = datasLINE["ORST"]
    String chid = datasLINE["CHID"]

    long dlix = datasLINE["DLIX"] as Long
    long conn = datasLINE["CONN"] as Long
    logger.debug("mngEXT036 orno=${orno} ponr=${ponr} posx=${posx}")

    String cscd = datasORDER["CSCD"]
    String uca4 = datasORDER["UCA4"]

    String hazi =  datasITEM["HAZI"]
    String hie5 =  datasITEM["HIE5"]
    String cfi4 =  datasITEM["CFI4"]
    String suno =  datasITEM["SUNO"]
    String prod =  datasITEM["PROD"]
    String sig6 =  datasITEM["SIG6"]
    double grwe =  datasITEM["GRWE"] as Double
    double newe =  datasITEM["NEWE"] as Double
    double ztgr =  datasITEM["ZTGR"] as Double
    double ztnw =  datasITEM["ZTNW"] as Double
    String csno =  datasITEM["CSNO"]
    String orco =  datasITEM["ORCO"]
    String zali =  datasITEM["ZALI"]
    String zalc =  datasITEM["ZALC"]
    String zsan =  datasITEM["ZSAN"]
    String zcap =  datasITEM["ZCAP"]
    String zca1 =  datasITEM["ZCA1"]
    String zca2 =  datasITEM["ZCA2"]
    String zca3 =  datasITEM["ZCA3"]
    String zca4 =  datasITEM["ZCA4"]
    String zca5 =  datasITEM["ZCA5"]
    String zca6 =  datasITEM["ZCA6"]
    String zca7 =  datasITEM["ZCA7"]
    String zca8 =  datasITEM["ZCA8"]
    String zori =  datasITEM["ZORI"]
    String zphy =  datasITEM["ZPHY"]
    int zagr =  datasITEM["ZAGR"] as Integer
    String znag =  datasITEM["ZNAG"]
    double zqco  =  datasITEM["ZQCO"] as Double

    DBAction query = database.table("EXT036").index("00").build()
    DBContainer EXT036_request = query.getContainer()
    EXT036_request.set("EXCONO", currentCompany)
    EXT036_request.set("EXORNO", orno)
    EXT036_request.set("EXPONR", ponr)
    EXT036_request.set("EXPOSX", posx)
    EXT036_request.set("EXZCLI", zcli)

    Closure<?> updateCallBack = { LockedResult lockedResult ->
      LocalDateTime timeOfCreation = LocalDateTime.now()
      int changeNumber = lockedResult.get("EXCHNO") as Integer

      lockedResult.set("EXORST", orst)
      lockedResult.set("EXCONN", conn)
      lockedResult.set("EXDLIX", dlix)
      lockedResult.set("EXUCA4", uca4)
      lockedResult.set("EXCUNO", cuno)
      lockedResult.set("EXITNO", itno)
      lockedResult.set("EXZAGR", zagr)
      lockedResult.set("EXZNAG", znag)
      lockedResult.set("EXORQT", orqt)
      lockedResult.set("EXALQT", alqt)
      lockedResult.set("EXZQCO", zqco)
      lockedResult.set("EXZTGR", ztgr)
      lockedResult.set("EXZTNW", ztnw)
      if (zsty != null)
        lockedResult.set("EXZSTY", zsty)
	  if (orst == "99") {
		lockedResult.set("EXSTAT", "90")
	  }else{
		lockedResult.set("EXSTAT", stat)
	  }
      lockedResult.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      lockedResult.setInt("EXCHNO", changeNumber + 1)
      lockedResult.set("EXCHID", program.getUser())
      lockedResult.update()
    }


    if(!query.readLock(EXT036_request, updateCallBack)){}
  }

  /**
   *
   */
  public void getLineEXT036(){
    String orno = datasLINE["ORNO"]
    int ponr = datasLINE["PONR"] as Integer
    int posx = datasLINE["POSX"] as Integer
    int constraintLine = 0

    // Retrieve next constraint line available
    DBAction EXT036_query = database.table("EXT036")
      .index("00")
      .selection(
        "EXORNO"
        , "EXPONR"
        , "EXPOSX"
        , "EXZCLI"
        , "EXZCID"
        , "EXZCTY"
        , "EXZCOD"
        , "EXDOID"
        , "EXADS1"
        , "EXSTAT"
      )
      .build()
    DBContainer EXT036_request = EXT036_query.getContainer()
    EXT036_request.set("EXCONO", currentCompany)
    EXT036_request.set("EXORNO", orno)
    EXT036_request.set("EXPONR", ponr)
    EXT036_request.set("EXPOSX", posx)

    Closure<?> EXT036_reader = { DBContainer EXT036_result ->
      constraintLine = EXT036_result.get("EXZCLI") as Integer
      String zcid = EXT036_result.get("EXZCID") as String
      String zcty = EXT036_result.get("EXZCTY") as String
      String zcod = EXT036_result.get("EXZCOD") as String
      String doid = EXT036_result.get("EXDOID") as String
      String ads1 = EXT036_result.get("EXADS1") as String
      String key = zcid.trim() + "#" + zcty.trim() + "#" + zcod.trim() + "#" + doid.trim() + "#" + ads1.trim()
      String value = EXT036_result.get("EXZCLI") as String
      value += "#" +  EXT036_result.get("EXSTAT") as String
      logger.debug("Add document_EXT036 key=${key}")
      documents_EXT036.put(key, value)
    }
    if (!EXT036_query.readAll(EXT036_request, 4, EXT036_reader)) {
    }
    logger.debug("last ZCLI ${constraintLine}")
    datasLINE["ZCLI"] = constraintLine as String
  }


  /**
   * Query on EXT030 (Constraint matrix)
   */
  public void getLineEXT030() {
    String orno = datasLINE["ORNO"]
    int ponr = datasLINE["PONR"] as Integer
    int posx = datasLINE["POSX"] as Integer

    logger.debug("getLineEXT030 orno=${orno} ponr=${ponr} posx=${posx}")

    String cuno = datasORDER["CUNO"]
    String cscd = datasORDER["CSCD"]

    String hazi =  datasITEM["HAZI"]
    String hie5 =  datasITEM["HIE5"]
    String cfi4 =  datasITEM["CFI4"]
    String suno =  datasITEM["SUNO"]
    String prod =  datasITEM["PROD"]
    String sig6 =  datasITEM["SIG6"]
    String grwe =  datasITEM["GRWE"]
    String newe =  datasITEM["NEWE"]
    String ztgr =  datasITEM["ZTGR"]
    String ztnw =  datasITEM["ZTNW"]
    String csno =  datasITEM["CSNO"]
    String orco =  datasITEM["ORCO"]
    String zali =  datasITEM["ZALI"]
    String zalc =  datasITEM["ZALC"]
    String zsan =  datasITEM["ZSAN"]
    String zcap =  datasITEM["ZCAP"]
    String zca1 =  datasITEM["ZCA1"]
    String zca2 =  datasITEM["ZCA2"]
    String zca3 =  datasITEM["ZCA3"]
    String zca4 =  datasITEM["ZCA4"]
    String zca5 =  datasITEM["ZCA5"]
    String zca6 =  datasITEM["ZCA6"]
    String zca7 =  datasITEM["ZCA7"]
    String zca8 =  datasITEM["ZCA8"]
    String zori =  datasITEM["ZORI"]
    String zphy =  datasITEM["ZPHY"]
    String zagr =  datasITEM["ZAGR"]
    String znag =  datasITEM["ZNAG"]

    ExpressionFactory expression_EXT030 = database.getExpressionFactory("EXT030")
    expression_EXT030 = (expression_EXT030.eq("EXCUNO", cuno)).or(expression_EXT030.eq("EXCUNO", ""))

    expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXCSCD", cscd)).or(expression_EXT030.eq("EXCSCD", "")))

    expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXHAZI", hazi as String)).or(expression_EXT030.eq("EXHAZI", "2")))

    if (hie5 != "") {
      expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXHIE0", hie5)).or(expression_EXT030.eq("EXHIE0", hie5.substring(0, 2) + "*")).or(expression_EXT030.eq("EXHIE0", hie5.substring(0, 4) + "*")).or(expression_EXT030.eq("EXHIE0", hie5.substring(0, 7) + "*")).or(expression_EXT030.eq("EXHIE0", hie5.substring(0, 9) + "*")).or(expression_EXT030.eq("EXHIE0", hie5.substring(0, 11) + "*")).or(expression_EXT030.eq("EXHIE0", "")))
    } else {
      expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXHIE0", hie5)).or(expression_EXT030.eq("EXHIE0", "")))
    }

    expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXCFI4", cfi4)).or(expression_EXT030.eq("EXCFI4", "")))


    expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXPOPN", sig6)).or(expression_EXT030.eq("EXPOPN", "")))

    if (csno != "") {
      expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXCSNO", csno)).or(expression_EXT030.eq("EXCSNO", csno.substring(0, 1) + "*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0, 2) + "*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0, 3) + "*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0, 4) + "*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0, 5) + "*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0, 6) + "*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0, 7) + "*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0, 8) + "*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0, 9) + "*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0, 10) + "*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0, 11) + "*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0, 12) + "*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0, 13) + "*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0, 14) + "*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0, 15) + "*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0, 16) + "*")).or(expression_EXT030.eq("EXCSNO", "")))
    } else {
      expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXCSNO", csno)).or(expression_EXT030.eq("EXCSNO", "")))
    }

    expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXORCO", orco)).or(expression_EXT030.eq("EXORCO", "")))

    expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXZALC", zalc as String)).or(expression_EXT030.eq("EXZALC", "2")))

    expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXZSAN", zsan as String)).or(expression_EXT030.eq("EXZSAN", "2")))

    expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXZCAP", zca1)).or(expression_EXT030.eq("EXZCAP", "")))

    expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXZCAS", zca1)).or(expression_EXT030.eq("EXZCAS", zca2)).or(expression_EXT030.eq("EXZCAS", zca3)).or(expression_EXT030.eq("EXZCAS", zca4)).or(expression_EXT030.eq("EXZCAS", zca5)).or(expression_EXT030.eq("EXZCAS", zca6)).or(expression_EXT030.eq("EXZCAS", zca7)).or(expression_EXT030.eq("EXZCAS", zca8)).or(expression_EXT030.eq("EXZCAS", "")))

    if (znag != "") {
      expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXZNAG", znag)).or(expression_EXT030.eq("EXZNAG", znag.substring(0, 4) + "*")).or(expression_EXT030.eq("EXZNAG", "")))
    } else {
      expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXZNAG", znag)).or(expression_EXT030.eq("EXZNAG", "")))
    }

    expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXZALI", zali as String)).or(expression_EXT030.eq("EXZALI", "2")))

    expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXZORI", zori as String)).or(expression_EXT030.eq("EXZORI", "2")))

    expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXZPHY", zphy as String)).or(expression_EXT030.eq("EXZPHY", "2")))

    logger.debug("ORCO 2 ${orco} " + (orco == "FR"))
    if (orco == "FR"){
      expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXZOHF", "0")).or(expression_EXT030.eq("EXZOHF", "2")))
    } else {
      expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXZOHF", "1")).or(expression_EXT030.eq("EXZOHF", "2")))
    }

    DBAction EXT030_query = database.table("EXT030").index("20").matching(expression_EXT030).selection("EXZCID", "EXZCOD").build()
    DBContainer EXT030 = EXT030_query.getContainer()
    EXT030.set("EXCONO", currentCompany)
    EXT030.set("EXSTAT", "20")

    if(!EXT030_query.readAll(EXT030, 2, EXT030_reader)){
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

  /**
   *
   */
  private void getLineCriterias() {
    getOrderDatas()
    getItemDatas()
  }

  /**
   * Get Item Informations and load them into itemDatas map object
   */
  public void getItemDatas() {
    //GetItem informations
    String itno = datasLINE["ITNO"] as String
    String faci = datasLINE["FACI"] as String

    String titno = datasITEM == null ? "" : datasITEM["ITNO"]

    if (titno == itno) {
      return
    }
    logger.debug("getItemDatas itno=${itno}")
    datasITEM = [
      "HAZI"  : ""
      , "HIE5": ""
      , "CFI4": ""
      , "SUNO": ""
      , "PROD": ""
      , "SIG6": ""
      , "GRWE": ""
      , "NEWE": ""
      , "ZTGR": ""
      , "ZTNW": ""
      , "CSNO": ""
      , "ORCO": ""
      , "ZALI": ""
      , "ZALC": ""
      , "ZSAN": ""
      , "ZCAP": ""
      , "ZCA1": ""
      , "ZCA2": ""
      , "ZCA3": ""
      , "ZCA4": ""
      , "ZCA5": ""
      , "ZCA6": ""
      , "ZCA7": ""
      , "ZCA8": ""
      , "ZORI": ""
      , "ZPHY": ""
      , "ZAGR": ""
      , "ZNAG": ""
      , "ZQCO": ""
      , "ZSTY": ""
    ]

    double alqt = datasLINE["ALQT"] as Double
    double orqt = datasLINE["ORQT"] as Double

    String hazi = ""
    String hie5 = ""
    String cfi4 = ""
    String suno = ""
    String prod = ""
    String sig6 = ""
    double grwe = 0
    double newe = 0
    double ztgr = 0
    double ztnw = 0
    String csno = ""
    String orco = ""
    int zali = 0
    int zalc = 0
    int zsan = 0
    String zcap = ""
    String zca1 = ""
    String zca2 = ""
    String zca3 = ""
    String zca4 = ""
    String zca5 = ""
    String zca6 = ""
    String zca7 = ""
    String zca8 = ""
    int zori = 0
    int zphy = 0
    int zagr = 0
    String znag = ""

    //Get infos from MITMAS
    DBAction MITMAS_query = database.table("MITMAS").index("00").selection("MMHAZI", "MMHIE5", "MMCFI4", "MMSUNO", "MMPROD", "MMITGR", "MMGRWE", "MMNEWE").build()
    DBContainer MITMAS = MITMAS_query.getContainer()
    MITMAS.set("MMCONO", currentCompany)
    MITMAS.set("MMITNO", itno)
    if (MITMAS_query.read(MITMAS)) {
      hazi = MITMAS.get("MMHAZI")
      hie5 = MITMAS.get("MMHIE5")
      cfi4 = MITMAS.get("MMCFI4")
      suno = MITMAS.get("MMSUNO")
      prod = MITMAS.get("MMPROD")
      grwe = MITMAS.get("MMGRWE") as Double
      newe = MITMAS.get("MMNEWE") as Double
      if (grwe != 0 && orqt != 0) {
        ztgr = grwe * orqt
      }
      if (newe != 0 && orqt != 0) {
        ztnw = newe * orqt
      }
    }
    //Get infos from MITPOP
    ExpressionFactory expression_MITPOP = database.getExpressionFactory("MITPOP")
    expression_MITPOP = expression_MITPOP.eq("MPREMK", "SIGMA6")
    DBAction MITPOP_query = database.table("MITPOP")
      .index("00")
      .matching(expression_MITPOP)
      .selection("MPPOPN").build()

    DBContainer MITPOP_request = MITPOP_query.getContainer()
    MITPOP_request.set("MPCONO", currentCompany)
    MITPOP_request.set("MPALWT", 1)
    MITPOP_request.set("MPALWQ", "")
    MITPOP_request.set("MPITNO", itno)

    Closure<?> MITPOP_reader = { DBContainer MITPOP_result ->
      //logger.debug("found MITPOP")
      sig6 = MITPOP_result.getString("MPPOPN").trim()
    }

    if (!MITPOP_query.readAll(MITPOP_request, 4, MITPOP_reader)) {
    }

    //Get infos from MITFAC
    DBAction MITFAC_query = database.table("MITFAC")
      .index("00")
      .selection("M9CSNO"
        , "M9ORCO")
      .build()
    DBContainer MITFAC = MITFAC_query.getContainer()
    MITFAC.set("M9CONO", currentCompany)
    MITFAC.set("M9FACI", faci)
    MITFAC.set("M9ITNO", itno)
    logger.debug("FACI ${faci}")
    if (MITFAC_query.read(MITFAC)) {
      csno = MITFAC.get("M9CSNO")
      orco = MITFAC.getString("M9ORCO").trim()
      logger.debug("ORCO ${orco}")
    }

    //Get infos from EXT032
    DBAction EXT032_query = database
      .table("EXT032")
      .index("00")
      .selection("EXZALC"
        , "EXZSAN"
        , "EXZCA1"
        , "EXZCA1"
        , "EXZCA2"
        , "EXZCA3"
        , "EXZCA4"
        , "EXZCA5"
        , "EXZCA6"
        , "EXZCA7"
        , "EXZCA8"
        , "EXZORI"
        , "EXZPHY"
        , "EXZAGR"
        , "EXZALI"
      ).build()

    DBContainer EXT032_request = EXT032_query.getContainer()
    EXT032_request.set("EXCONO", currentCompany)
    EXT032_request.set("EXPOPN", sig6)
    EXT032_request.set("EXSUNO", suno)
    EXT032_request.set("EXORCO", orco)
    if (EXT032_query.read(EXT032_request)) {
      zalc = EXT032_request.get("EXZALC") as Integer
      zsan = EXT032_request.get("EXZSAN") as Integer
      zcap = EXT032_request.get("EXZCA1")
      zca1 = EXT032_request.get("EXZCA1")
      zca2 = EXT032_request.get("EXZCA2")
      zca3 = EXT032_request.get("EXZCA3")
      zca4 = EXT032_request.get("EXZCA4")
      zca5 = EXT032_request.get("EXZCA5")
      zca6 = EXT032_request.get("EXZCA6")
      zca7 = EXT032_request.get("EXZCA7")
      zca8 = EXT032_request.get("EXZCA8")
      zori = EXT032_request.get("EXZORI") as Integer
      zphy = EXT032_request.get("EXZPHY") as Integer
      zagr = EXT032_request.get("EXZAGR") as Integer
      zali = EXT032_request.get("EXZALI") as Integer
    }

    DBAction CUGEX1_CIDMAS_query = database.table("CUGEX1").index("00").selection("F1A030").build()
    DBContainer CUGEX1_CIDMAS = CUGEX1_CIDMAS_query.getContainer()
    CUGEX1_CIDMAS.set("F1CONO", currentCompany)
    CUGEX1_CIDMAS.set("F1FILE", "CIDMAS")
    if (prod.trim() != "") {
      CUGEX1_CIDMAS.set("F1PK01", prod)
    } else {
      CUGEX1_CIDMAS.set("F1PK01", suno)
    }
    CUGEX1_CIDMAS.set("F1PK02", "")
    CUGEX1_CIDMAS.set("F1PK03", "")
    CUGEX1_CIDMAS.set("F1PK04", "")
    CUGEX1_CIDMAS.set("F1PK05", "")
    CUGEX1_CIDMAS.set("F1PK06", "")
    CUGEX1_CIDMAS.set("F1PK07", "")
    CUGEX1_CIDMAS.set("F1PK08", "")
    if (CUGEX1_CIDMAS_query.read(CUGEX1_CIDMAS)) {
      znag = CUGEX1_CIDMAS.get("F1A030")
    }
    double cofa = 0
    double zqco = 0
    DBAction query_MITAUN = database.table("MITAUN").index("00").selection("MUCOFA").build()
    DBContainer MITAUN = query_MITAUN.getContainer()
    MITAUN.set("MUCONO", currentCompany)
    MITAUN.set("MUITNO", itno)
    MITAUN.set("MUAUTP", 1)
    MITAUN.set("MUALUN", "COL")
    if (query_MITAUN.read(MITAUN)) {
      cofa = MITAUN.get("MUCOFA") as Double
      if (orqt != 0 && cofa != 0)
        zqco = orqt / cofa
    }

    //Store datas
    datasITEM["HAZI"] = hazi
    datasITEM["HIE5"] = hie5
    datasITEM["CFI4"] = cfi4
    datasITEM["SUNO"] = suno
    datasITEM["PROD"] = prod
    datasITEM["SIG6"] = sig6
    datasITEM["GRWE"] = grwe as String
    datasITEM["NEWE"] = newe as String
    datasITEM["ZTGR"] = ztgr as String
    datasITEM["ZTNW"] = ztnw as String
    datasITEM["ZQCO"] = zqco as String
    datasITEM["CSNO"] = csno
    datasITEM["ORCO"] = orco
    datasITEM["ZALI"] = zali as String
    datasITEM["ZALC"] = zalc as String
    datasITEM["ZSAN"] = zsan as String
    datasITEM["ZCAP"] = zcap
    datasITEM["ZCA1"] = zca1
    datasITEM["ZCA2"] = zca2
    datasITEM["ZCA3"] = zca3
    datasITEM["ZCA4"] = zca4
    datasITEM["ZCA5"] = zca5
    datasITEM["ZCA6"] = zca6
    datasITEM["ZCA7"] = zca7
    datasITEM["ZCA8"] = zca8
    datasITEM["ZORI"] = zori as String
    datasITEM["ZPHY"] = zphy as String
    datasITEM["ZAGR"] = zagr as String
    datasITEM["ZNAG"] = znag
  }

  /**
   * Get Order Informations and load them into orderDatas map object
   */
  public void getOrderDatas() {

    datasORDER = [
      "ORNO"  : ""
      , "CUNO": ""
      , "CSCD": ""
      , "UCA4": ""
    ]

    //Get country code from OOHEAD
    String orno = datasLINE["ORNO"] as String
    String torno = datasORDER == null ? "" : datasORDER["ORNO"]

    if (orno == torno) {
      return
    }
    logger.debug("getOrderDatas orno=${orno}")

    DBAction OOHEAD_query = database.table("OOHEAD")
      .index("00")
      .selection("OAADID"
        , "OACUNO"
        , "OAUCA4"
      )
      .build()
    DBContainer OOHEAD_request = OOHEAD_query.getContainer()
    OOHEAD_request.set("OACONO", currentCompany)
    OOHEAD_request.set("OAORNO", orno)
    if (OOHEAD_query.read(OOHEAD_request)) {
      datasORDER["UCA4"] = OOHEAD_request["OAUCA4"] as String
      datasORDER["CUNO"] = OOHEAD_request["OACUNO"] as String
      if (OOHEAD_request.getString("OAADID") != "") {
        DBAction OCUSAD_query = database.table("OCUSAD")
          .index("00")
          .selection("OPCSCD")
          .build()
        DBContainer OCUSAD_request = OCUSAD_query.getContainer()
        OCUSAD_request.set("OPCONO", currentCompany)
        OCUSAD_request.set("OPCUNO", datasORDER["CUNO"])
        OCUSAD_request.set("OPADRT", 1)
        OCUSAD_request.set("OPADID", OOHEAD_request.get("OAADID"))
        if (OCUSAD_query.read(OCUSAD_request)) {
          datasORDER["CSCD"] = OCUSAD_request.getString("OPCSCD")
        }
      } else {
        DBAction OCUSMA_query = database.table("OCUSMA")
          .index("00")
          .selection("OKCSCD").build()
        DBContainer OCUSMA_request = OCUSMA_query.getContainer()
        OCUSMA_request.set("OKCONO", currentCompany)
        OCUSMA_request.set("OKCUNO", datasORDER["CUNO"])
        if (OCUSMA_query.read(OCUSMA_request)) {
          datasORDER["CSCD"] = OCUSMA_request.getString("OKCSCD")
        }
      }
    }
  }
  /**
   * Read EXT030 constraint
   */
  Closure<?> EXT030_reader = { DBContainer EXT030_result ->
    boolean constraintFound = true
    String zcid = EXT030_result.get("EXZCID")
    String zcod = EXT030_result.get("EXZCOD")
    String zcty = ""
    String zsty = ""

    DBAction EXT034_query = database
      .table("EXT034")
      .index("00")
      .selection("EXZCTY"
        , "EXZSTY")
      .build()
    DBContainer EXT034_request = EXT034_query.getContainer()
    EXT034_request.set("EXCONO", currentCompany)
    EXT034_request.set("EXZCOD", zcod)
    if(EXT034_query.read(EXT034_request)){
      zcty = EXT034_request.get("EXZCTY")
      zsty = EXT034_request.get("EXZSTY") as String
    }
    logger.debug("EXT030_reader zcid=${zcid} zcty=${zcty} zcod=${zcod}")
    queryEXT035(zcid, zcty,zcod, zsty)

  }
  /**
   * Query on EXT035 (Documentation) and insert found documents in EXT036
   * @return
   */
  public void queryEXT035(String zcid, String zcty, String zcod, zsty) {
    //Document search: write in EXT036 for each document found
    DBAction EXT035_query = database.table("EXT035").index("00").selection("EXZCOD").build()
    DBContainer EXT035_request = EXT035_query.getContainer()
    EXT035_request.set("EXCONO", currentCompany)
    EXT035_request.set("EXZCOD", zcod)
    EXT035_request.set("EXCSCD", datasORDER["CSCD"])
    EXT035_request.set("EXCUNO", datasORDER["CUNO"])


    String doid = ""
    String ads1 = ""

    Closure<?> EXT035_reader = { DBContainer EXT035_result ->
      doid = EXT035_result.get("EXDOID") as String
      DBAction MPDDOC_query = database.table("MPDDOC").index("00").selection("DOADS1").build()
      DBContainer MPDDOC = MPDDOC_query.getContainer()
      MPDDOC.set("DOCONO", currentCompany)
      MPDDOC.set("DODOID", doid)
      if(MPDDOC_query.read(MPDDOC)){
        ads1 = MPDDOC.get("DOADS1")
      }
      String key = zcid.trim() + "#" + zcty.trim() + "#" + zcod.trim() + "#" + doid.trim() + "#" + ads1.trim()
      String value = zsty
      logger.debug("Add document key=${key}")
      documents.put(key, value)
    }

    if(!EXT035_query.readAll(EXT035_request, 4, EXT035_reader)){
      //logger.debug("-----------------------------------EXT035 not found with ZCOD/CSCD/CUNO-----------------------------------")
      EXT035_request.set("EXCSCD", datasORDER["CSCD"])
      EXT035_request.set("EXCUNO", "")
      if(!EXT035_query.readAll(EXT035_request, 4, EXT035_reader)){
        //logger.debug("-----------------------------------EXT035 not found with ZCOD/CSCD-----------------------------------")
        EXT035_request.set("EXCSCD", "")
        EXT035_request.set("EXCUNO", "")
        if(!EXT035_query.readAll(EXT035_request, 4, EXT035_reader)){
          String key = zcid.trim() + "#" + zcty.trim() + "#" + zcod.trim() + "#" + doid.trim() + "#" + ads1.trim()
          String value = zsty
          logger.debug("Add document key=${key}")
          documents.put(key, value)
        }
      }
    }
  }

}
