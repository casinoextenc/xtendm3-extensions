public class OIS101SupplyPath extends ExtendM3Trigger {
  private final InteractiveAPI interactive;
  private final ProgramAPI program;
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final UtilityAPI utility
  private final MIAPI mi
  private final MICallerAPI miCaller
  private String ortp
  private String cuno
  private String modl
  private String orqa
  private String whlo
  private String itno
  private String ltyp
  private String sigma6
  private String sigma9
  private String errorApi
  private Integer currentCompany

  private final String[] runOnlyForUsers = ["FLEBARS", "PBEAUDOIN", "MLAFON", "CDECORNOY"]
  // Leave the array empty if it should be run for everyone, otherwise add authorized usernames

  public OIS101SupplyPath(InteractiveAPI interactive, ProgramAPI program, DatabaseAPI database, LoggerAPI logger, UtilityAPI utility, MICallerAPI miCaller) {
    this.interactive = interactive;
    this.database = database
    this.logger = logger
    this.program = program
    this.utility = utility
    this.miCaller = miCaller
  }


  void main() {
    logger.debug("Debut OIS101.SupplyPath ##PB##")
    if (!shouldRun()) return
    sigma6 = interactive.display.fields.WBITNO
    String orno = interactive.display.fields.OAORNO
    orqa = interactive.display.fields.WBORQA
    currentCompany = (Integer) program.getLDAZD().CONO
    logger.debug("WBITNO = " + sigma6 + " WBORNO = " + orno + " CurrentCompany = " + currentCompany)
    if (sigma6.length() == 6) {
      ExpressionFactory expression = database.getExpressionFactory("MITPOP")
      expression = expression.ge("MPREMK", "SIGMA6")
      DBAction queryMITPOP = database.table("MITPOP").index("10").matching(expression).build()
      DBContainer ContainerMITPOP = queryMITPOP.getContainer()
      ContainerMITPOP.set("MPCONO", currentCompany)
      ContainerMITPOP.setInt("MPALWT", 1)
      ContainerMITPOP.set("MPALWQ", "")
      ContainerMITPOP.set("MPPOPN", sigma6)
      if (!queryMITPOP.readAll(ContainerMITPOP, 4, MITPOPData)) {
        return
      }

      DBAction OOHEAD_query = database.table("OOHEAD").index("00").selection(
        "OAOBLC",
        "OAORTP",
        "OACUNO",
        "OAMODL",
        "OAWHLO").build()

      DBContainer OOHEAD_request = OOHEAD_query.getContainer()
      OOHEAD_request.set("OACONO", currentCompany)
      OOHEAD_request.set("OAORNO", orno)
      if (OOHEAD_query.read(OOHEAD_request)) {
        ortp = OOHEAD_request.get("OAORTP") as String
        cuno = OOHEAD_request.get("OACUNO") as String
        modl = OOHEAD_request.get("OAMODL") as String
        whlo = OOHEAD_request.get("OAWHLO") as String

        DBAction queryCUGEX100 = database.table("CUGEX1").index("00").selection("F1CONO",
          "F1FILE",
          "F1PK01",
          "F1PK02",
          "F1PK03",
          "F1PK04",
          "F1PK05",
          "F1PK06",
          "F1PK07",
          "F1PK08",
          "F1A830"
        ).build()


        DBContainer containerCUGEX1 = queryCUGEX100.getContainer()
        containerCUGEX1.set("F1CONO", currentCompany)
        containerCUGEX1.set("F1FILE", "OOTYPE")
        containerCUGEX1.set("F1PK01", ortp)

        if (queryCUGEX100.read(containerCUGEX1)) {
          boolean isCHB4 = containerCUGEX1.get("F1CHB4").toString().trim()
          logger.debug("cugex1 chb4 = " + isCHB4)
          if (isCHB4) {
            itno = ""
            whlo = ""
            ltyp = ""
            executeEXT011GetSupplyPath(cuno, ortp, modl, sigma6, whlo, orqa, "0", "1")
            if (errorApi != "" ){
              interactive.showCustomError("EXT011.GetSupplyPath", "Erreur : " + errorApi)
            }
           
            if (itno != ""){
            interactive.display.fields.WBITNO = itno
            interactive.display.fields.OBWHLO = whlo
            interactive.display.fields.OBLTYP = ltyp
            logger.debug("AfterExecute WBITNO = " + itno + " WBWHLO = " + whlo + " WBltyp = " + ltyp)
          }
          }


        }
      }

    }

  }

  private boolean shouldRun() {
    if (runOnlyForUsers.length != 0) {
      String currentUser = program.LDAZD.get("RESP").toString().trim()
      boolean authorizedToRun = runOnlyForUsers.contains(currentUser)
      logger.debug("User {$currentUser} authorization check result was ${authorizedToRun}")
      return authorizedToRun
    } else {
      return true
    }
  }
  Closure<?> MITPOPData = { DBContainer ContainerMITPOP ->
    sigma9 = ContainerMITPOP.get("MPITNO")
    logger.debug("Cosure MITPOP Sigma9 = " + sigma9)
  }


  // Execute EXT011MI.GetSupplyPath
  private executeEXT011GetSupplyPath(String CUNO, String ORTP, String MODL, String POPN, String WHLO, String ORQA, String FLG1, String FLAG) {
    def parameters = ["CUNO": CUNO, "ORTP": ORTP, "MODL": MODL, "POPN": POPN, "WHLO": WHLO, "ORQA": ORQA, "FLG1": FLG1, "FLAG": FLAG]
    errorApi=""
    Closure<?> handler = { Map<String, String> response ->
    logger.debug("Closure executeMI")
        if (response.error != null) {
           logger.debug("Closure executeMI error"+ response.errorMessage )
           errorApi ="EXT011.GetSupplyPath : " + response.errorMessage
           
      } else {
        logger.debug("Closure executeMI pas error")
        itno = response.ITNO.trim()
        whlo = response.WHLO.trim()
        ltyp = response.LTYP.trim()
      }
    }
    miCaller.call("EXT011MI", "GetSupplyPath", parameters, handler)
  }


}

