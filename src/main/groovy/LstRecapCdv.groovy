/**
 * Name : EXT012MI.LstRecapCdv
 *
 * Description :
 * This API method to List Recap Cdv APP02
 *
 *
 * Date         Changed By    Description
 * 20240208     MLECLERCQ       Creation
 * 20240530     MLECLERCQ       Retrieve DLDG from EXT012
 */

public class LstRecapCdv extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final MICallerAPI miCaller
  private final ProgramAPI program
  private final UtilityAPI utility

  private Integer currentCompany
  private String ornoInput
  private String currentLine
  private String currentDWDZ
  private String currentItno
  private String currentCuno
  private String currentSupplier
  private String currentSupplierName
  private String currentAssort
  private Integer nbMaxRecord = 10000

  public LstRecapCdv(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program,UtilityAPI utility,MICallerAPI miCaller) {
    this.mi = mi
    this.database = database
    this.logger = logger
    this.program = program
    this.utility = utility
    this.miCaller = miCaller
  }

  public void main() {
    currentCompany = (Integer) program.getLDAZD().CONO
    int maxReturnedRecords = 9999

    ornoInput = (mi.in.get("ORNO")).toString()

    if (ornoInput == null || ornoInput == "") {
      mi.error("N° de commande obligatoire.")
      return
    }

    DBAction rechercheOOLINE = database.table("OOLINE").index("00").selection("OBPONR","OBORNO","OBITNO","OBCUNO","OBDWDZ").build()
    DBContainer oolineContainer = rechercheOOLINE.createContainer()
    oolineContainer.set("OBCONO", currentCompany)
    oolineContainer.set("OBORNO", ornoInput)

    if(rechercheOOLINE.readAll(oolineContainer,2,nbMaxRecord,closureOOLINE)){

    }
  }
  /**
   * Retrieve OOLINE
   */
  Closure<?> closureOOLINE = { DBContainer oolineResult ->
    currentCuno = oolineResult.get("OBCUNO")
    currentLine = oolineResult.get("OBPONR")
    currentDWDZ = oolineResult.get("OBDWDZ")
    currentItno = oolineResult.get("OBITNO")

    logger.debug("OOlineResult PONR = ${currentLine}")

    Map<String, String> params = [
      "RIDN": ornoInput,
      "ORCA": "311",
      "ORC2": "250",
      "SUCL": "200",
      "PONR": currentLine
    ]

    miCaller.call("EXT012MI","SelSupplyChain", params, closureEXT012)
  }

  /**
   * Retrieve EXT012
   */
  Closure<?> closureEXT012 = {
    Map<String,String> response ->

      currentSupplier = response.get("SUNO")

      logger.debug("SelSupplyChain response SUNO = ${currentSupplier} and PONR = ${currentLine}")

      DBAction rechercheCIDMAS = database.table("CIDMAS").index("00").selection("IDSUNM").build()
      DBContainer cidmasContainer = rechercheCIDMAS.createContainer()
      cidmasContainer.set("IDCONO",currentCompany)
      cidmasContainer.set("IDSUNO", currentSupplier)

      if(rechercheCIDMAS.read(cidmasContainer)) {
        currentSupplierName = cidmasContainer.get("IDSUNM").toString()
      }

      logger.debug("currentDWDZ : ${currentDWDZ} and PONR = ${currentLine}")
      ExpressionFactory expression = database.getExpressionFactory("EXT010")
      expression = expression.eq("EXSULE", currentSupplier)
      expression = expression.and(expression.le("EXFVDT", currentDWDZ))
      expression = expression.and(expression.ge("EXLVDT", currentDWDZ))
      DBAction rechercheEXT010 = database.table("EXT010").index("02").matching(expression).selection("EXASGD","EXSULE","EXITNO","EXFVDT","EXLVDT").build()
      DBContainer ext010Container = rechercheEXT010.createContainer()


      ext010Container.set("EXCONO",currentCompany)
      ext010Container.set("EXCUNO", currentCuno)
      ext010Container.set("EXITNO", currentItno)

      if(rechercheEXT010.readAll(ext010Container,3,nbMaxRecord, closureEXT010)){
      }else{

        logger.debug("No record found in EXT010 for PONR: ${currentLine}")
        mi.outData.put("ORNO", ornoInput)
        mi.outData.put("PONR", currentLine)
        mi.outData.put("ITNO", currentItno)
        mi.outData.put("ASGD", "")
        mi.outData.put("SUNO", currentSupplier)
        mi.outData.put("SUNM", currentSupplierName)
        mi.outData.put("DWDZ", currentDWDZ)
        mi.outData.put("CUNO", currentCuno)
        mi.write()
      }
  }

  /**
   * Retrieve EXT010
   */
  Closure<?> closureEXT010 = { DBContainer resultEXT010 ->
    currentAssort = resultEXT010.get("EXASGD").toString().trim()

    logger.debug("Record found in EXT010, ASCD is : ${currentAssort} for line ${currentLine}, EXITNO:" +resultEXT010.get("EXITNO")+", EXFVDT:" +resultEXT010.get("EXFVDT") + ",EXLVDT:" + resultEXT010.get("EXLVDT") +", search EXT012")

    DBAction rechercheEXT012 = database.table("EXT012").selection("EXDLGD").build()
    DBContainer ext012Container = rechercheEXT012.createContainer()

    int drgd = Integer.parseInt(currentDWDZ)

    ext012Container.set("EXCONO", currentCompany)
    ext012Container.set("EXCUNO", currentCuno)
    ext012Container.set("EXSUNO", currentSupplier)
    ext012Container.set("EXASGD", currentAssort)
    ext012Container.set("EXDRGD", drgd)

    if(rechercheEXT012.readAll(ext012Container,5,1, { DBContainer resultEXT012 ->
      String currentDLGD = resultEXT012.get("EXDLGD")

      logger.debug("in EXT012, line is : ${currentLine}")

      mi.outData.put("ORNO", ornoInput)
      mi.outData.put("PONR", currentLine)
      mi.outData.put("ITNO", currentItno)
      mi.outData.put("ASGD", currentAssort)
      mi.outData.put("SUNO", currentSupplier)
      mi.outData.put("SUNM", currentSupplierName)
      mi.outData.put("DWDZ", currentDWDZ)
      mi.outData.put("CUNO", currentCuno)
      mi.outData.put("DLGD", currentDLGD)

      mi.write()

    })){

    }else{
      logger.debug("No record found in EXT012 for line ${currentLine}")

      mi.outData.put("ORNO", ornoInput)
      mi.outData.put("PONR", currentLine)
      mi.outData.put("ITNO", currentItno)
      mi.outData.put("ASGD", currentAssort)
      mi.outData.put("SUNO", currentSupplier)
      mi.outData.put("SUNM", currentSupplierName)
      mi.outData.put("DWDZ", currentDWDZ)
      mi.outData.put("CUNO", currentCuno)
      mi.outData.put("DLGD", "")

      mi.write()
    }
  }
}
