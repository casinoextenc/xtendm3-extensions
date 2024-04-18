public class LstRecapCdv extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final MICallerAPI miCaller
  private final ProgramAPI program
  private final UtilityAPI utility

  private Integer currentCompany
  private String orno_input
  private String currentLine
  private String currentDWDZ
  private String currentItno
  private String currentCuno
  private String currentSupplier
  private String currentSupplierName





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

    orno_input = (mi.in.get("ORNO")).toString()

    if (orno_input == null || orno_input == "") {
      mi.error("N° de commande obligatoire.")
      return
    }

    DBAction rechercheOOLINE = database.table("OOLINE").index("00").selection("OBPONR","OBORNO","OBITNO","OBCUNO","OBDWDZ").build()
    DBContainer oolineContainer = rechercheOOLINE.createContainer()
    oolineContainer.set("OBCONO", currentCompany)
    oolineContainer.set("OBORNO", orno_input)

    if(rechercheOOLINE.readAll(oolineContainer,2,9999,closureOOLINE)){

    }


  }

  Closure<?> closureOOLINE = { DBContainer oolineResult ->
    currentCuno = oolineResult.get("OBCUNO")
    currentLine = oolineResult.get("OBPONR")
    currentDWDZ = oolineResult.get("OBDWDZ")
    currentItno = oolineResult.get("OBITNO")

    Map<String, String> params = [
      "RIDN": orno_input,
      "ORCA": "311",
      "ORC2": "250",
      "SUCL": "200",
      "PONR": currentLine
    ]

    miCaller.call("EXT012MI","SelSupplyChain", params, closureEXT012)
  }

  Closure<?> closureEXT012 = {
    Map<String,String> response ->

      currentSupplier = response.get("SUNO")

      DBAction rechercheCIDMAS = database.table("CIDMAS").index("00").selection("IDSUNM").build()
      DBContainer cidmasContainer = rechercheCIDMAS.createContainer()
      cidmasContainer.set("IDCONO",currentCompany)
      cidmasContainer.set("IDSUNO", currentSupplier)

      if(rechercheCIDMAS.read(cidmasContainer)) {
        currentSupplierName = cidmasContainer.get("IDSUNM").toString()
      }



      DBAction rechercheEXT010 = database.table("EXT010").index("02").selection("EXASGD","EXSULE").build()
      DBContainer ext010Container = rechercheEXT010.createContainer()
      ext010Container.set("EXCONO",currentCompany)
      ext010Container.set("EXCUNO", currentCuno)
      ext010Container.set("EXITNO", currentItno)
      ext010Container.set("EXSULE", currentSupplier)

      if(rechercheEXT010.readAll(ext010Container,3,9999, closureEXT010)){
      }else{

        logger.debug("No record found in EXT010")

        mi.outData.put("ORNO", orno_input)
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

  Closure<?> closureEXT010 = { DBContainer resultEXT010 ->
    String currentAssort = resultEXT010.get("EXASGD")

    logger.debug("Record found in EXT010")

      mi.outData.put("ORNO", orno_input)
      mi.outData.put("PONR", currentLine)
      mi.outData.put("ITNO", currentItno)
      mi.outData.put("ASGD", currentAssort)
      mi.outData.put("SUNO", currentSupplier)
      mi.outData.put("SUNM", currentSupplierName)
      mi.outData.put("DWDZ", currentDWDZ)
      mi.outData.put("CUNO", currentCuno)

      mi.write()



  }
}
