/**
 * README
 * This extension is used by Mashup
 *
 * Name : EXT820MI.AddSchedSetting
 * Description : Add records to the EXT820 table.
 * Date         Changed By   Description
 * 20211129     RENARN       INTX99 Batch submission
 * 20220106     RENARN       Auto increment of XSVN added
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class AddSchedSetting extends ExtendM3Transaction {
  private final MIAPI mi;
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final MICallerAPI miCaller;
  private final ProgramAPI program;
  private final UtilityAPI utility;
  private final TextFilesAPI textFiles
  private final ExtensionAPI extension
  private String currentDate
  private String scty = "0"
  private String fdat = "0"
  private String tdat = "0"
  private String xemt = "0"
  private String petp = "0"
  private String xnow = "0"
  private String xtod = "0"
  private String xnmo = "0"
  private String xntu = "0"
  private String xnwe = "0"
  private String xnth = "0"
  private String xnfr = "0"
  private String xnsa = "0"
  private String xnsu = "0"
  private String xemo = "0"
  private String xetu = "0"
  private String xewe = "0"
  private String xeth = "0"
  private String xefr = "0"
  private String xesa = "0"
  private String xesu = "0"
  private String xsti = "0"
  private String xjdt = ""
  private String xjtm = ""
  private String jsca = ""
  private Integer multiSubmission
  private Integer singleSubmission
  public boolean selectionExist
  public Integer xvsn = 0

  public AddSchedSetting(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program,UtilityAPI utility, TextFilesAPI textFiles, MICallerAPI miCaller, ExtensionAPI extension) {
    this.mi = mi;
    this.database = database
    this.logger = logger
    this.program = program
    this.utility = utility;
    this.textFiles = textFiles
    this.miCaller = miCaller
    this.extension = extension
  }

  public void main() {
    Integer currentCompany
    if (mi.in.get("CONO") == null) {
      currentCompany = (Integer)program.getLDAZD().CONO
    } else {
      currentCompany = mi.in.get("CONO")
    }

    // Get current date
    LocalDateTime timeOfCreation = LocalDateTime.now()
    currentDate = timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer

    // Check job schedule category
    if(mi.in.get("XCAT") != null){
      DBAction query = database.table("CSYTAB").index("00").build()
      DBContainer CSYTAB = query.getContainer()
      CSYTAB.set("CTCONO",currentCompany)
      CSYTAB.set("CTSTCO",  "SHSC")
      CSYTAB.set("CTSTKY", mi.in.get("XCAT"))
      if (!query.read(CSYTAB)) {
        mi.error("Catégorie programme travail " + mi.in.get("XCAT") + " n'existe pas")
        return
      }
    }
    // Check job schedule type
    if(mi.in.get("SCTY") != null){
      scty = mi.in.get("SCTY") as Integer
      if(mi.in.get("SCTY") != 0 && mi.in.get("SCTY") != 1 && mi.in.get("SCTY") != 2 && mi.in.get("SCTY") != 3 && mi.in.get("SCTY") != 4) {
        mi.error("Type programme travail " + scty + " est invalide")
        return
      }
    }
    // Check now
    if(mi.in.get("XNOW") != null){
      xnow = mi.in.get("XNOW")
      if(mi.in.get("XNOW") != "0" && mi.in.get("XNOW") != "1") {
        mi.error("Maintenant " + xnow + " est invalide")
        return
      }
    }
    // Check today
    if(mi.in.get("XTOD") != null){
      xtod = mi.in.get("XTOD")
      if(mi.in.get("XTOD") != "0" && mi.in.get("XTOD") != "1") {
        mi.error("Aujourd'hui " + xtod + " est invalide")
        return
      }
    }
    // Check next Monday
    if(mi.in.get("XNMO") != null){
      xnmo = mi.in.get("XNMO")
      if(mi.in.get("XNMO") != "0" && mi.in.get("XNMO") != "1") {
        mi.error("Lundi prochain " + xnmo + " est invalide")
        return
      }
    }
    // Check next Tuesday
    if(mi.in.get("XNTU") != null){
      xntu = mi.in.get("XNTU")
      if(mi.in.get("XNTU") != "0" && mi.in.get("XNTU") != "1") {
        mi.error("Mardi prochain " + xntu + " est invalide")
        return
      }
    }
    // Check next Wednesday
    if(mi.in.get("XNWE") != null){
      xnwe = mi.in.get("XNWE")
      if(mi.in.get("XNWE") != "0" && mi.in.get("XNWE") != "1") {
        mi.error("Mercredi prochain " + xnwe + " est invalide")
        return
      }
    }
    // Check next Thursday
    if(mi.in.get("XNTH") != null){
      xnth = mi.in.get("XNTH")
      if(mi.in.get("XNTH") != "0" && mi.in.get("XNTH") != "1") {
        mi.error("Jeudi prochain " + xnth + " est invalide")
        return
      }
    }
    // Check next Friday
    if(mi.in.get("XNFR") != null){
      xnfr = mi.in.get("XNFR")
      if(mi.in.get("XNFR") != "0" && mi.in.get("XNFR") != "1") {
        mi.error("Vendredi prochain " + xnfr + " est invalide")
        return
      }
    }
    // Check next Saturday
    if(mi.in.get("XNSA") != null){
      xnsa = mi.in.get("XNSA")
      if(mi.in.get("XNSA") != "0" && mi.in.get("XNSA") != "1") {
        mi.error("Samedi prochain " + xnsa + " est invalide")
        return
      }
    }
    // Check next Sunday
    if(mi.in.get("XNSU") != null){
      xnsu = mi.in.get("XNSU")
      if(mi.in.get("XNSU") != "0" && mi.in.get("XNSU") != "1") {
        mi.error("Dimanche prochain " + xnsu + " est invalide")
        return
      }
    }
    // Check every Monday
    if(mi.in.get("XEMO") != null){
      xemo = mi.in.get("XEMO")
      if(mi.in.get("XEMO") != "0" && mi.in.get("XEMO") != "1") {
        mi.error("Chaque lundi " + xemo + " est invalide")
        return
      }
    }
    // Check every Tuesday
    if(mi.in.get("XETU") != null){
      xetu = mi.in.get("XETU")
      if(mi.in.get("XETU") != "0" && mi.in.get("XETU") != "1") {
        mi.error("Chaque mardi " + xetu + " est invalide")
        return
      }
    }
    // Check every Wednesday
    if(mi.in.get("XEWE") != null){
      xewe = mi.in.get("XEWE")
      if(mi.in.get("XEWE") != "0" && mi.in.get("XEWE") != "1") {
        mi.error("Chaque mercredi " + xewe + " est invalide")
        return
      }
    }
    // Check every Thursday
    if(mi.in.get("XETH") != null){
      xeth = mi.in.get("XETH")
      if(mi.in.get("XETH") != "0" && mi.in.get("XETH") != "1") {
        mi.error("Chaque jeudi " + xeth + " est invalide")
        return
      }
    }
    // Check every Friday
    if(mi.in.get("XEFR") != null){
      xefr = mi.in.get("XEFR")
      if(mi.in.get("XEFR") != "0" && mi.in.get("XEFR") != "1") {
        mi.error("Chaque vendredi " + xefr + " est invalide")
        return
      }
    }
    // Check every Saturday
    if(mi.in.get("XESA") != null){
      xesa = mi.in.get("XESA")
      if(mi.in.get("XESA") != "0" && mi.in.get("XESA") != "1") {
        mi.error("Chaque samedi " + xesa + " est invalide")
        return
      }
    }
    // Check every Sunday
    if(mi.in.get("XESU") != null){
      xesu = mi.in.get("XESU")
      if(mi.in.get("XESU") != "0" && mi.in.get("XESU") != "1") {
        mi.error("Chaque dimanche " + xesu + " est invalide")
        return
      }
    }
    // Check every month
    if(mi.in.get("XEMT") != null){
      xemt = mi.in.get("XEMT")
      if(mi.in.get("XEMT") != "0" && mi.in.get("XEMT") != "1") {
        mi.error("Chaque mois " + xemt + " est invalide")
        return
      }
    }
    // Check on day
    Integer xrdy = mi.in.get("XRDY")
    if (xemt == "1") {
      if (xrdy >= 1 &&
        xrdy <= 31 ||
        xrdy == 98 ||
        xrdy == 99) {
      } else {
        mi.error("Par jour " + (xrdy as String) + " est invalide");
        return;
      }
    }
    // Check specific date
    if(mi.in.get("XJDT") != null){
      xjdt = mi.in.get("XJDT");
      logger.debug("xjdt = " + xjdt)
      if (!utility.call("DateUtil", "isDateValid", xjdt, "yyyyMMdd")) {
        mi.error("Date spécifique " + xjdt + " est invalide");
        return;
      }
      if ((xjdt as Integer) <= (currentDate as Integer)) {
        mi.error("Date spécifique doit être supérieure à la date du jour");
        return;
      }
    }
    // Check time
    if(mi.in.get("XJTM") != null && mi.in.get("XJTM") != 0){
      xjtm = mi.in.get("XJTM") as Integer
      if(scty.trim() == "4") {
        mi.error("Heure " + xjtm + " est invalide");
        return;
      }
    }
    // Check recovery action
    if(mi.in.get("XSTI") != null){
      xsti = mi.in.get("XSTI")
      if(mi.in.get("XSTI") != "0" && mi.in.get("XSTI") != "1") {
        mi.error("Action reprise " + xsti + " est invalide")
        return
      }
    }
    // Check from date
    if(mi.in.get("FDAT") != null && mi.in.get("FDAT") != 0){
      fdat = mi.in.get("FDAT");
      if(scty.trim() == "4") {
        mi.error("Date de début " + fdat + " est invalide");
        return;
      }
      if (!utility.call("DateUtil", "isDateValid", fdat, "yyyyMMdd")) {
        mi.error("Date de début " + fdat + " est invalide");
        return;
      }
    }
    // Check to date
    if(mi.in.get("TDAT") != null && mi.in.get("TDAT") != 0){
      tdat = mi.in.get("TDAT");
      if(scty.trim() == "4") {
        mi.error("Date de fin " + tdat + " est invalide");
        return;
      }
      if (!utility.call("DateUtil", "isDateValid", tdat, "yyyyMMdd")) {
        mi.error("Date de fin " + tdat + " est invalide");
        return;
      }
    }
    if((fdat as Integer) != 0 && (tdat as Integer) != 0 && (fdat as Integer) > (tdat as Integer)) {
      mi.error("Date de début doit être inférieure à date de fin");
      return;
    }
    if(mi.in.get("JSCA") != null)
      jsca = mi.in.get("JSCA")
    // At least one selection must be made
    if(xnow == "0" && xtod == "0" && xnmo == "0" && xntu == "0" && xnwe == "0" && xnth == "0" && xnfr == "0" && xnsa == "0" && xnsu == "0" && xemo == "0" && xetu == "0" && xewe == "0" && xeth == "0" && xefr == "0" && xesa == "0" && xesu == "0" && xemt == "0" && xjdt.trim() == ""){
      mi.error("Au moins une sélection doit être faite")
      return
    } else {
      selectionExist = true
    }
    // Every mon-sun is allowed to be multi submission,
    // all others are single submission...
    multiSubmission = 0;
    singleSubmission = 0;
    if (xemo == "1") {
      multiSubmission++;
    }
    if (xetu == "1") {
      multiSubmission++;
    }
    if (xewe == "1") {
      multiSubmission++;
    }
    if (xeth == "1") {
      multiSubmission++;
    }
    if (xefr == "1") {
      multiSubmission++;
    }
    if (xesa == "1") {
      multiSubmission++;
    }
    if (xesu == "1") {
      multiSubmission++;
    }
    if (xnow == "1") {
      singleSubmission++;
    }
    if (xtod == "1") {
      singleSubmission++;
    }
    if (xnmo == "1") {
      singleSubmission++;
    }
    if (xntu == "1") {
      singleSubmission++;
    }
    if (xnwe == "1") {
      singleSubmission++;
    }
    if (xnth == "1") {
      singleSubmission++;
    }
    if (xnfr == "1") {
      singleSubmission++;
    }
    if (xnsa == "1") {
      singleSubmission++;
    }
    if (xnsu == "1") {
      singleSubmission++;
    }
    if (xjdt.trim() != "") {
      singleSubmission++;
    }
    if (xemt == "1") {
      singleSubmission++;
    }
    if (singleSubmission > 1 || singleSubmission != 0 && multiSubmission != 0) {
      mi.error("Soumissions multiples autorisées uniquement du lundi au dimanche")
      return
    }
    // Only one selection should be made
    if (jsca.trim() != "" && selectionExist){
      mi.error("Une seule sélection doit être faite")
      return
    }
    // Check calendar
    if(mi.in.get("JSCA") != null){
      DBAction query = database.table("CSHCAL").index("00").build()
      DBContainer CSHCAL = query.getContainer()
      CSHCAL.set("LKCONO", currentCompany)
      CSHCAL.set("LKJSCA", mi.in.get("JSCA"))
      if (!query.readAll(CSHCAL, 2, outData_CSHCAL)) {
        mi.error("Calendrier " + mi.in.get("JSCA") + " n'existe pas")
        return
      } else {
        // Future date must exist
        ExpressionFactory expression = database.getExpressionFactory("CSHCAD")
        expression = expression.gt("LKSGDT", currentDate)
        DBAction query2 = database.table("CSHCAD").index("00").build()
        DBContainer CSHCAD = query2.getContainer()
        CSHCAD.set("LKCONO", currentCompany)
        CSHCAD.set("LKJSCA", mi.in.get("JSCA"))
        if (!query2.readAll(CSHCAD, 2, outData_CSHCAD)) {
          mi.error("Calendrier " + mi.in.get("JSCA") + " n'existe pas")
          return
        }
      }
    }
    // Check period type
    if(mi.in.get("PETP") != null){
      petp = mi.in.get("PETP") as Integer
      if(mi.in.get("PETP") != 0 && mi.in.get("PETP") != 1) {
        mi.error("Type de période " + petp + " est invalide")
        return
      }
    }
    // Check time zone
    if(mi.in.get("TIZO") != null){
      DBAction query = database.table("CITZON").index("00").build()
      DBContainer CITZON = query.getContainer()
      CITZON.set("TZCONO", currentCompany)
      CITZON.set("TZTIZO", mi.in.get("TIZO"))
      if (!query.readAll(CITZON, 2, outData_CITZON)) {
        mi.error("Zone heure " + mi.in.get("TIZO") + " n'existe pas")
        return
      }
    }
    // Retrieve last version
    DBAction query_1 = database.table("EXT820").index("00").selection("EXXVSN").reverse().build()
    DBContainer EXT820_1 = query_1.getContainer()
    EXT820_1.set("EXCONO", currentCompany)
    EXT820_1.set("EXJOID",  mi.in.get("JOID"))
    EXT820_1.set("EXUSID",  mi.in.get("USID"))
    if (!query_1.readAll(EXT820_1, 3, outData_EXT820)) {
    }
    // Increment last version
    xvsn++
    DBAction query = database.table("EXT820").index("00").build()
    DBContainer EXT820 = query.getContainer()
    EXT820.set("EXCONO", currentCompany)
    EXT820.set("EXJOID",  mi.in.get("JOID"))
    EXT820.set("EXUSID",  mi.in.get("USID"))
    EXT820.set("EXXVSN",  xvsn)
    if (!query.read(EXT820)) {
      if (mi.in.get("TX30") != null)
        EXT820.set("EXTX30", mi.in.get("TX30"))
      if (mi.in.get("TX60") != null)
        EXT820.set("EXTX60", mi.in.get("TX60"))
      if (mi.in.get("XCAT") != null)
        EXT820.set("EXXCAT", mi.in.get("XCAT"))
      if (mi.in.get("SCTY") != null)
        EXT820.set("EXSCTY", mi.in.get("SCTY"))
      if (mi.in.get("XNOW") != null)
        EXT820.set("EXXNOW", mi.in.get("XNOW"))
      if (mi.in.get("XTOD") != null)
        EXT820.set("EXXTOD", mi.in.get("XTOD"))
      if (mi.in.get("XNMO") != null)
        EXT820.set("EXXNMO", mi.in.get("XNMO"))
      if (mi.in.get("XNTU") != null)
        EXT820.set("EXXNTU", mi.in.get("XNTU"))
      if (mi.in.get("XNWE") != null)
        EXT820.set("EXXNWE", mi.in.get("XNWE"))
      if (mi.in.get("XNTH") != null)
        EXT820.set("EXXNTH", mi.in.get("XNTH"))
      if (mi.in.get("XNFR") != null)
        EXT820.set("EXXNFR", mi.in.get("XNFR"))
      if (mi.in.get("XNSA") != null)
        EXT820.set("EXXNSA", mi.in.get("XNSA"))
      if (mi.in.get("XNSU") != null)
        EXT820.set("EXXNSU", mi.in.get("XNSU"))
      if (mi.in.get("XEMO") != null)
        EXT820.set("EXXEMO", mi.in.get("XEMO"))
      if (mi.in.get("XETU") != null)
        EXT820.set("EXXETU", mi.in.get("XETU"))
      if (mi.in.get("XEWE") != null)
        EXT820.set("EXXEWE", mi.in.get("XEWE"))
      if (mi.in.get("XETH") != null)
        EXT820.set("EXXETH", mi.in.get("XETH"))
      if (mi.in.get("XEFR") != null)
        EXT820.set("EXXEFR", mi.in.get("XEFR"))
      if (mi.in.get("XESA") != null)
        EXT820.set("EXXESA", mi.in.get("XESA"))
      if (mi.in.get("XESU") != null)
        EXT820.set("EXXESU", mi.in.get("XESU"))
      if (mi.in.get("XEMT") != null)
        EXT820.set("EXXEMT", mi.in.get("XEMT"))
      if (mi.in.get("XRDY") != null)
        EXT820.set("EXXRDY", mi.in.get("XRDY"))
      if (mi.in.get("XJDT") != null)
        EXT820.set("EXXJDT", mi.in.get("XJDT") as Integer)
      if (mi.in.get("XJTM") != null)
        EXT820.set("EXXJTM", mi.in.get("XJTM"))
      if (mi.in.get("XSTI") != null)
        EXT820.set("EXXSTI", mi.in.get("XSTI"))
      if (mi.in.get("FDAT") != null)
        EXT820.set("EXFDAT", mi.in.get("FDAT") as Integer)
      if (mi.in.get("TDAT") != null)
        EXT820.set("EXTDAT", mi.in.get("TDAT") as Integer)
      if (mi.in.get("JSCA") != null)
        EXT820.set("EXJSCA", mi.in.get("JSCA"))
      if (mi.in.get("PETP") != null)
        EXT820.set("EXPETP", mi.in.get("PETP"))
      if (mi.in.get("TIZO") != null)
        EXT820.set("EXTIZO", mi.in.get("TIZO"))
      EXT820.setInt("EXRGDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      EXT820.setInt("EXRGTM", timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")) as Integer)
      EXT820.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      EXT820.setInt("EXCHNO", 1)
      EXT820.set("EXCHID", program.getUser())
      query.insert(EXT820)
    } else {
      mi.error("L'enregistrement existe déjà")
      return
    }
  }
  Closure<?> outData_CSHCAL = { DBContainer CSHCAL ->
  }
  Closure<?> outData_CSHCAD = { DBContainer CSHCAD ->
  }
  Closure<?> outData_CITZON = { DBContainer CITZON ->
  }
  Closure<?> outData_EXT820 = { DBContainer EXT820 ->
    if(xvsn == 0)
      xvsn = EXT820.get("EXXVSN")
  }
}
