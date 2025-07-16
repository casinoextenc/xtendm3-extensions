/**
 * README
 * This extension is used by
 *
 * Name : EXT820MI.SubmitBatch
 * Description : Batch submission
 * Date         Changed By  Description
 * 20211129     RENARN      INTX99 Batch submission
 */
import com.sun.org.apache.xerces.internal.impl.dtd.models.CMUniOp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class SubmitBatch extends ExtendM3Transaction {
  private final MIAPI mi
  private final MICallerAPI miCaller
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program
  private String referenceId
  private String jobId
  private String data = ""
  private String description
  private Integer currentCompany
  private String tx30 = ""
  private String tx60 = ""
  private String xcat = ""
  private String scty = ""
  private String xnow = ""
  private String xtod = ""
  private String xnmo = ""
  private String xntu = ""
  private String xnwe = ""
  private String xnth = ""
  private String xnfr = ""
  private String xnsa = ""
  private String xnsu = ""
  private String xemo = ""
  private String xetu = ""
  private String xewe = ""
  private String xeth = ""
  private String xefr = ""
  private String xesa = ""
  private String xesu = ""
  private String xemt = ""
  private String xrdy = ""
  private String xjdt = ""
  private String xjtm = ""
  private String xsti = ""
  private String fdat = ""
  private String tdat = ""
  private String jsca = ""
  private String petp = ""
  private String tizo = ""
  private boolean foundEXT820
  private String p001 = ""
  private String p002 = ""
  private String p003 = ""
  private String p004 = ""
  private String p005 = ""
  private String p006 = ""
  private String p007 = ""
  private String p008 = ""
  private String p009 = ""
  private String p010 = ""

  public SubmitBatch(MIAPI mi, MICallerAPI miCaller, DatabaseAPI database, LoggerAPI logger, ProgramAPI program) {
    this.mi = mi
    this.miCaller = miCaller
    this.database = database
    this.logger = logger
    this.program = program
  }

  public void main() {
    currentCompany
    if (mi.in.get("CONO") == null) {
      currentCompany = (Integer)program.getLDAZD().CONO
    } else {
      currentCompany = mi.in.get("CONO")
    }
    logger.debug("Début EXT820")
    if(mi.in.get("JOID") != null)
      jobId = mi.in.get("JOID")
    if(mi.in.get("P001") != null)
      p001 = mi.in.get("P001")
    if(mi.in.get("P002") != null)
      p002 = mi.in.get("P002")
    if(mi.in.get("P003") != null)
      p003 = mi.in.get("P003")
    if(mi.in.get("P004") != null)
      p004 = mi.in.get("P004")
    if(mi.in.get("P005") != null)
      p005 = mi.in.get("P005")
    if(mi.in.get("P006") != null)
      p006 = mi.in.get("P006")
    if(mi.in.get("P007") != null)
      p007 = mi.in.get("P007")
    if(mi.in.get("P008") != null)
      p008 = mi.in.get("P008")
    if(mi.in.get("P009") != null)
      p009 = mi.in.get("P009")
    if(mi.in.get("P010") != null)
      p010 = mi.in.get("P010")

    if(p001.trim() != "") {
      data = data + p001.trim() + ";"
    }else{
      data = data +";"
    }
    if(p002.trim() != "") {
      data = data + p002.trim() + ";"
    }else{
      data = data +";"
    }
    if(p003.trim() != "") {
      data = data + p003.trim() + ";"
    }else{
      data = data +";"
    }
    if(p004.trim() != "") {
      data = data + p004.trim() + ";"
    }else{
      data = data +";"
    }
    if(p005.trim() != "") {
      data = data + p005.trim() + ";"
    }else{
      data = data +";"
    }
    if(p006.trim() != "") {
      data = data + p006.trim() + ";"
    }else{
      data = data +";"
    }
    if(p007.trim() != "") {
      data = data + p007.trim() + ";"
    }else{
      data = data +";"
    }
    if(p008.trim() != "") {
      data = data + p008.trim() + ";"
    }else{
      data = data +";"
    }
    if(p009.trim() != "") {
      data = data + p009.trim() + ";"
    }else{
      data = data +";"
    }
    if(p010.trim() != "") {
      data = data + p010.trim() + ";"
    }else{
      data = data +";"
    }

    referenceId = UUID.randomUUID().toString()
    setupData(referenceId, data)
    // Get schedule settings per extension
    getEXT820()
    // If EXT820 settings not found, default settings are applied
    if(!foundEXT820){
      tx30 = "Job XtendM3 soumis"
      tx60 = "Job XtendM3 soumis"
      xcat = "010"
      scty = "1"
      xnow = "1"
    }
    logger.debug("foundEXT820 = " + foundEXT820)
    callSHS010MI()
    mi.outData.put("UUID", referenceId)
    mi.write()
  }
  /*
   *  SHS010MI is a special extension which has a metohdt for executing Batch extension named SchedXM3Job - without calling it - the Batch extension won't be executed.
   *  It has several mandatory parametres to call the extension point of it:
   *  - TX30 - description for running an extension (ex. which transaction is being executed - here EXT919MI)
   *  - XCAT - job schedule category, depends on the SHS010 category data
   *  - SCTY - job schedule type
   *  - XNOW - the time of executing batch. XNOW refers to immediate execution. There are another similar parameters for execution at spcific day, a consistently recurring day,  *           every month or even chosen date.
   *  - JOB  - name of Batch extension which should be executed
   *  - UUID - UUID number inputed by user
   */
  void callSHS010MI() {
    logger.debug("Calling SHS010MI tx30/tx60/xcat/scty/xnow" + tx30 + "/" + tx60 + "/" + xcat + "/" + scty + "/" + xnow)
    Closure<?> callback = {Map<String, String> result ->
      logger.debug("Result is: ${result}")
    }
    def params = [
      "JOB": jobId,
      "TX30": tx30,
      "TX60": tx60,
      "XCAT": xcat,
      "SCTY": scty,
      "XNOW": xnow,
      "XTOD": xtod,
      "XNMO": xnmo,
      "XNTU": xntu,
      "XNWE": xnwe,
      "XNTH": xnth,
      "XNFR": xnfr,
      "XNSA": xnsa,
      "XNSU": xnsu,
      "XEMO": xemo,
      "XETU": xetu,
      "XEWE": xewe,
      "XETH": xeth,
      "XEFR": xefr,
      "XESA": xesa,
      "XESU": xesu,
      "XEMT": xemt,
      "XRDY": xrdy,
      "XJDT": xjdt,
      "XJTM": xjtm,
      "XSTI": xsti,
      "FDAT": fdat,
      "TDAT": tdat,
      "JSCA": jsca,
      "PETP": petp,
      "TIZO": tizo,
      "UUID": referenceId]
    miCaller.call("SHS010MI", "SchedXM3Job", params, callback)
    // This method is calling SHS010MI to execute with inputed parameters. More info about MICaller API is available in the doc
  }
  // Setup data
  public void setupData(String referenceId, String data){
    LocalDateTime timeOfCreation = LocalDateTime.now()
    def query = database.table("EXTJOB").index("00").selection("EXRFID", "EXDATA").build()
    def container = query.createContainer()
    container.set("EXRFID", referenceId)
    container.set("EXJOID", jobId)
    container.set("EXDATA", data)
    container.setInt("EXRGDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
    container.setInt("EXRGTM", timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")) as Integer)
    container.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
    container.setInt("EXCHNO", 1)
    container.set("EXCHID", program.getUser())
    query.insert(container)
  }
  // Get schedule settings per extension
  public void getEXT820(){
    logger.debug("EXT820 Recherche - CONO = " + currentCompany)
    logger.debug("EXT820 Recherche - JOID  = " + mi.in.get("JOID"))
    logger.debug("EXT820 Recherche - USID = " + program.getUser())
    DBAction query = database.table("EXT820").index("00").selection("EXTX30","EXTX60","EXXCAT","EXSCTY","EXXNOW","EXXTOD","EXXNMO","EXXNTU","EXXNWE","EXXNTH","EXXNFR","EXXNSA","EXXNSU","EXXEMO","EXXETU","EXXEWE","EXXETH","EXXEFR","EXXESA","EXXESU","EXXEMT","EXXRDY","EXXJDT","EXXJTM","EXXSTI","EXFDAT","EXTDAT","EXJSCA","EXPETP","EXTIZO").build()
    DBContainer EXT820 = query.getContainer()
    EXT820.set("EXCONO", currentCompany)
    EXT820.set("EXJOID", mi.in.get("JOID"))
    if(mi.in.get("USID") != null){
      EXT820.set("EXUSID", mi.in.get("USID"))
    } else {
      EXT820.set("EXUSID", program.getUser())
    }
    if(mi.in.get("XVSN") != null){
      EXT820.set("EXXVSN", mi.in.get("XVSN"))
    } else {
      EXT820.set("EXXVSN", 0)
    }
    if(!query.readAll(EXT820, 4, outData)){
      EXT820.set("EXCONO", currentCompany)
      EXT820.set("EXJOID", mi.in.get("JOID"))
      EXT820.set("EXUSID", "")
      if(!query.readAll(EXT820, 3, outData)){
        EXT820.set("EXCONO", currentCompany)
        EXT820.set("EXJOID", "")
        EXT820.set("EXUSID", "")
        if(!query.readAll(EXT820, 3, outData)){
        }
      }
    }
  }
  // Retrieve EXT820
  Closure<?> outData = { DBContainer EXT820 ->
    foundEXT820 = true
    tx30 = EXT820.get("EXTX30")
    tx60 = EXT820.get("EXTX60")
    xcat = EXT820.get("EXXCAT")
    scty = EXT820.get("EXSCTY")
    xnow = EXT820.get("EXXNOW")
    xtod = EXT820.get("EXXTOD")
    xnmo = EXT820.get("EXXNMO")
    xntu = EXT820.get("EXXNTU")
    xnwe = EXT820.get("EXXNWE")
    xnth = EXT820.get("EXXNTH")
    xnfr = EXT820.get("EXXNFR")
    xnsa = EXT820.get("EXXNSA")
    xnsu = EXT820.get("EXXNSU")
    xemo = EXT820.get("EXXEMO")
    xetu = EXT820.get("EXXETU")
    xewe = EXT820.get("EXXEWE")
    xeth = EXT820.get("EXXETH")
    xefr = EXT820.get("EXXEFR")
    xesa = EXT820.get("EXXESA")
    xesu = EXT820.get("EXXESU")
    xemt = EXT820.get("EXXEMT")
    if(EXT820.get("EXXRDY") != 0)
      xrdy = EXT820.get("EXXRDY")
    if(EXT820.get("EXXJDT") != 0)
      xjdt = EXT820.get("EXXJDT")
    if(EXT820.get("EXXJTM") != 0) {
      xjtm = EXT820.get("EXXJTM")
      if(xjtm.length() == 5)
        xjtm = "0" + EXT820.get("EXXJTM")
      if(xjtm.length() == 4)
        xjtm = "00" + EXT820.get("EXXJTM")
      if(xjtm.length() == 3)
        xjtm = "000" + EXT820.get("EXXJTM")
      if(xjtm.length() == 2)
        xjtm = "0000" + EXT820.get("EXXJTM")
      if(xjtm.length() == 1)
        xjtm = "00000" + EXT820.get("EXXJTM")
    }
    logger.debug("xjtm = " + xjtm)
    xsti = EXT820.get("EXXSTI")
    if(EXT820.get("EXFDAT") != 0)
      fdat = EXT820.get("EXFDAT")
    if(EXT820.get("EXTDAT") != 0)
      tdat = EXT820.get("EXTDAT")
    jsca = EXT820.get("EXJSCA")
    if(EXT820.get("EXPETP") != 0)
      petp = EXT820.get("EXPETP")
    tizo = EXT820.get("EXTIZO")
  }
}
