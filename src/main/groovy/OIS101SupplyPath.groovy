/**
 *
 */
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
  private String alun
  private String whlo
  private String itno
  private String ltyp
  private String sigma6
  private String sigma9
  private String errorApi
  private Integer currentCompany

  private final String[] runOnlyForUsers = []
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
    currentCompany = (Integer) program.getLDAZD().CONO
    if (!shouldRun()) return
    sigma6 = interactive.display.fields.WBITNO
    String orno = interactive.display.fields.OAORNO
    orqa = interactive.display.fields.WBORQA

    alun = interactive.display.fields.WBALUN
    alun = alun == null ? "COL" : alun
    alun = alun.length() == 0 ? "COL" : alun

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
          "F1CHB4"
        ).build()


        DBContainer containerCUGEX1 = queryCUGEX100.getContainer()
        containerCUGEX1.set("F1CONO", currentCompany)
        containerCUGEX1.set("F1FILE", "OOTYPE")
        containerCUGEX1.set("F1PK01", ortp)

        if (queryCUGEX100.read(containerCUGEX1)) {
          boolean isCHB4 = "1".equals(containerCUGEX1.get("F1CHB4").toString().trim())
          if (isCHB4) {
            itno = ""
            whlo = ""
            ltyp = ""
            executeEXT011GetSupplyPath(cuno, ortp, modl, sigma6, whlo, orqa, "0", "1", alun)
            if (errorApi != "") {
              interactive.showCustomError("EXT011.GetSupplyPath", "Erreur : " + errorApi)
            }

            if (itno != "") {
              interactive.display.fields.WBITNO = itno
              interactive.display.fields.OBWHLO = whlo
              interactive.display.fields.OBLTYP = ltyp
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
      return authorizedToRun
    } else {
      return true
    }
  }
  Closure<?> MITPOPData = { DBContainer ContainerMITPOP ->
    sigma9 = ContainerMITPOP.get("MPITNO")
  }


  /**
   * Call EXT011MI.GetSupplyPath
   * @param pCuno
   * @param pOrtp
   * @param pModl
   * @param pPopn
   * @param pWhlo
   * @param pOrqa
   * @param pFlg1
   * @param pFlag
   * @param pAlun
   * @return
   */
  private executeEXT011GetSupplyPath(String pCuno, String pOrtp, String pModl, String pPopn, String pWhlo, String pOrqa, String pFlg1, String pFlag, String pAlun) {
    Map<String, String> parameters = [
      "CUNO": pCuno,
      "ORTP": pOrtp,
      "MODL": pModl,
      "POPN": pPopn,
      "WHLO": pWhlo,
      "ORQA": pOrqa,
      "FLG1": pFlg1,
      "FLAG": pFlag,
      "ALUN": pAlun
    ]
    errorApi = ""
    logger.debug("CUNO: ${pCuno}, ORTP: ${pOrtp}, MODL: ${pModl}, POPN: ${pPopn}, WHLO: ${pWhlo}, ORQA: ${pOrqa}, FLG1: ${pFlg1}, FLAG: ${pFlag}")
    Closure<?> handler = { Map<String, String> response ->
      if (response.error != null) {
        errorApi = "EXT011.GetSupplyPath : " + response.errorMessage

      } else {
        itno = response.ITNO.trim()
        whlo = response.WHLO.trim()
        ltyp = response.LTYP.trim()
      }
    }
    miCaller.call("EXT011MI", "GetSupplyPath", parameters, handler)
  }


}

