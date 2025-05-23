/**
 * Name : EXT850MI.UpdOustInvAmnt
 *
 * Description :
 * This API method to Update update oustanding invoice amount
 *
 *
 * Date         Changed By    Description
 * 20230119     NALLANIC      FIN3001 - Creation
 * 20240701     PBEAUDOUIN    For Validation Xtend
 */
import java.time.LocalDate
import java.time.format.DateTimeFormatter
class UpdOustInvAmnt extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program
  private final UtilityAPI utility
  private final MICallerAPI miCaller
  private int currentCompany
  private String nbr
  private Integer line

  public UpdOustInvAmnt(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, MICallerAPI miCaller, UtilityAPI utility) {
    this.mi = mi
    this.database = database
    this.logger = logger
    this.program = program
    this.miCaller = miCaller
    this.utility = utility
  }

  public void main() {

    if (mi.in.get("CONO") == null) {
      currentCompany = (Integer) program.getLDAZD().CONO
    } else {
      currentCompany = mi.in.get("CONO")
    }

    //Get mi inputs
    String divi = (String) (mi.in.get("DIVI") != null ? mi.in.get("DIVI") : "")
    String cuno = (String) (mi.in.get("CUNO") != null ? mi.in.get("CUNO") : "")
    String inputOina = (String) (mi.in.get("OINA") != null ? mi.in.get("OINA") : "")
    String upda = (String) (mi.in.get("UPDA") != null ? mi.in.get("UPDA") : "")

    //Read the payer of the customer
    DBAction queryOcusma = database.table("OCUSMA")
      .index("00")
      .selection(
        "OKCONO",
        "OKCUNO",
        "OKPYNO",
        "OKCUCD"
      )
      .build()
    DBContainer containerOcusma = queryOcusma.getContainer()
    containerOcusma.set("OKCONO", currentCompany)
    containerOcusma.set("OKCUNO", cuno)

    //Record exists
    if (!queryOcusma.read(containerOcusma)) {
      mi.error("Le code Client " + cuno + " n'existe pas")
      return
    }

    String pyno = containerOcusma.get("OKPYNO")
    String cucd = containerOcusma.get("OKCUCD")

    if (pyno.trim().equals("")) {
      pyno = cuno
    }

    //Read the current oustanding invoice amount
    DBAction queryCcucrl = database.table("CCUCRL")
      .index("00")
      .selection(
        "CCCONO",
        "CCDIVI",
        "CCPYNO",
        "CCOINA"
      )
      .build()
    DBContainer containerCcucrl = queryCcucrl.getContainer()
    containerCcucrl.set("CCCONO", currentCompany)
    containerCcucrl.set("CCDIVI", "")
    containerCcucrl.set("CCPYNO", pyno)

    //Record exists
    if (!queryCcucrl.read(containerCcucrl)) {
      mi.error("La limite de crédit du payeur " + pyno.trim() + " n'existe pas")
      return
    }

    String currentOina = containerCcucrl.get("CCOINA")
    Double doubleCurrentOina = 0
    Double doubleInputOina = 0

    try {
      doubleCurrentOina = Double.parseDouble(currentOina)
    } catch (NumberFormatException e) {
    }

    try {
      doubleInputOina = Double.parseDouble(inputOina)
    } catch (NumberFormatException e) {
    }

    java.text.DecimalFormat df2 = new java.text.DecimalFormat("0.##")
    String pyam = df2.format(doubleInputOina - doubleCurrentOina)

    LocalDate today = LocalDate.now()
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
    String dateDay = today.format(formatter)

    if (!upda.equals("0") && !upda.equals("1")) {
      mi.error("Le mode de mise à jour peut prendre la valeur 1 ou 0")
      return
    }

    executeCRS165MIRtvNextNumber(divi, "ZC", "C")
    String nbnrEc = nbr.trim()
    String key1

    if (upda.equals("1")) {
      key1 = dateDay.substring(2, 8) + "_" + nbnrEc
    }
    if (upda.equals("0")) {
      key1 = dateDay
    }

    //Read the current interface head
    DBAction queryFglinh = database.table("FGLINH")
      .index("00")
      .selection(
        "FHCONO",
        "FHDIVI",
        "FHKEY1"
      )
      .build()
    DBContainer containerFglinh = queryFglinh.getContainer()
    containerFglinh.set("FHCONO", currentCompany)
    containerFglinh.set("FHDIVI", divi)
    containerFglinh.set("FHKEY1", key1)

    String sCurrentCompany = Integer.toString(currentCompany)

    if (!queryFglinh.read(containerFglinh)) {

      if (upda.equals("1")) {
        executeGLS840MIAddBatchHead(sCurrentCompany, divi, key1, "PAY_ENCOURS", "PAY_ENCOURS_" + pyno)
      }
      if (upda.equals("0")) {
        executeGLS840MIAddBatchHead(sCurrentCompany, divi, key1, "PAY_ENCOURS", "PAY_ENCOURS_" + dateDay)
      }

    }

    //Read the current interface line
    DBAction fglinlQuery = database.table("FGLINL")
      .index("00")
      .selection("FLLINE")
      .reverse()
      .build()

    DBContainer fglinlRequest = fglinlQuery.getContainer()
    fglinlRequest.set("FLCONO", currentCompany)
    fglinlRequest.set("FLDIVI", divi)
    fglinlRequest.set("FLKEY1", key1)

    //Record exists
    if (!fglinlQuery.readAll(fglinlRequest, 3, 1, fglinlReader)) {
      line = 1
    }

    String maxLine = Integer.toString(line)

    for (int i = 0; nbnrEc.length() < 9; i++) nbnrEc = nbnrEc + " "
    String maxLineParm = maxLine
    for (int i = 0; maxLineParm.length() < 8; i++) maxLineParm = maxLineParm + " "
    for (int i = 0; divi.length() < 3; i++) divi = divi + " "
    for (int i = 0; pyno.length() < 10; i++) pyno = pyno + " "
    executeCRS165MIRtvNextNumber(divi, "52", "2")
    String nbnr52 = nbr.trim()
    for (int i = 0; nbnr52.length() < 15; i++) nbnr52 = nbnr52 + " "
    for (int i = 0; pyam.length() < 17; i++) pyam = " " + pyam
    pyam = pyam.replace(".", ",")
    for (int i = 0; cucd.length() < 3; i++) cucd = cucd + " "

    String sParm = "I1" + nbnrEc + maxLineParm + divi + pyno + nbnr52 + pyam + cucd + dateDay + dateDay
    executeGLS840MIAddBatchLine(sCurrentCompany, divi, key1, maxLine, sParm)
    if (upda.equals("1")) {
      executeGLS840MIUpdBatch(sCurrentCompany, divi, key1)
    }

  }

  // Retrieve FGLINL
  Closure<?> fglinlReader = { DBContainer fglinlResult ->
    line = fglinlResult.get("FLLINE")
    line++
  }

  // Execute GLS840MI.AddBatchHead
  private executeGLS840MIAddBatchHead(String CONO, String DIVI, String KEY1, String INTN, String DESC) {
    Map<String, String> parameters = ["CONO": CONO, "DIVI": DIVI, "KEY1": KEY1, "INTN": INTN, "DESC": DESC]
    Closure<?> handler = { Map<String, String> response ->
      if (response.error != null) {
        return mi.error("Failed GLS840MI.AddBatchHead: " + response.errorMessage)
      }
    }
    miCaller.call("GLS840MI", "AddBatchHead", parameters, handler)
  }

  // Execute GLS840MI.AddBatchLine
  private executeGLS840MIAddBatchLine(String pCono, String pDivi, String pKey1, String pLine, String pParm) {
    LinkedHashMap<String, String> parameters = ["CONO": pCono, "DIVI": pDivi, "KEY1": pKey1, "LINE": pLine, "PARM": pParm]
    Closure<?> handler = { Map<String, String> response ->
      if (response.error != null) {
        return mi.error("Failed GLS840MI.AddBatchLine: " + response.errorMessage)
      }
    }
    miCaller.call("GLS840MI", "AddBatchLine", parameters, handler)
  }

  // Execute GLS840MI.UpdBatch
  private executeGLS840MIUpdBatch(String pCono, String pDivi, String pKey1) {
    LinkedHashMap<String, String> parameters = ["CONO": pCono, "DIVI": pDivi, "KEY1": pKey1]
    Closure<?> handler = { Map<String, String> response ->
      if (response.error != null) {
        return mi.error("Failed GLS840MI.UpdBatch: " + response.errorMessage)
      }
    }
    miCaller.call("GLS840MI", "UpdBatch", parameters, handler)
  }

  // Execute CRS165MI.RtvNextNumber
  private executeCRS165MIRtvNextNumber(String pDivi, String pNbty, String pNbid) {
    LinkedHashMap<String, String> parameters = ["DIVI": pDivi, "NBTY": pNbty, "NBID": pNbid]
    Closure<?> handler = { Map<String, String> response ->
      nbr = response.NBNR.trim()
      if (response.error != null) {
        return mi.error("Failed CRS165MI.RtvNextNumber: " + response.errorMessage)
      }
    }
    miCaller.call("CRS165MI", "RtvNextNumber", parameters, handler)
  }
}
