/**
 * README
 * This extension is used by MEC
 *
 * Name : EXT012MI.LstOrderLines
 * Description : List order lines
 * Date         Changed By    Description
 * 20230822     FLEBARS       LOG1203 Creation
 */
public class LstOrderLines extends ExtendM3Transaction {
  private final MIAPI mi
  private final LoggerAPI logger
  private final ProgramAPI program
  private final DatabaseAPI database
  private final SessionAPI session
  private final TransactionAPI transaction
  private final MICallerAPI miCaller
  private final UtilityAPI utility

  private int currentCompany
  private Integer nbMaxRecord = 10000

  public LstOrderLines(LoggerAPI logger, MIAPI mi, DatabaseAPI database, ProgramAPI program, MICallerAPI miCaller, UtilityAPI utility) {
    this.logger = logger
    this.mi = mi
    this.database = database
    this.program = program
    this.miCaller = miCaller
    this.utility = utility
  }

  public void main() {
    logger.debug("main start")
    currentCompany = (Integer)program.getLDAZD().CONO
    String orno = (String)(mi.in.get("ORNO") != null ? mi.in.get("ORNO") : "")
    String asgd = (String)(mi.in.get("ASGD") != null ? mi.in.get("ASGD") : "")
    String suno = (String)(mi.in.get("SUNO") != null ? mi.in.get("SUNO") : "")

    ExpressionFactory oolineExpression = database.getExpressionFactory("OOLINE")
    oolineExpression = oolineExpression.lt("OBORST", "66")
    if (asgd.length() > 0)
      oolineExpression = oolineExpression.and(oolineExpression.eq("OBUCA0", asgd))

    DBAction oolineQuery = database.table("OOLINE")
      .matching(oolineExpression)
      .selection(
        "OBPONR"
        ,"OBRORC"
        ,"OBRORN"
        ,"OBRORL"
        ,"OBRORX"
        ,"OBUCA0"
      )
      .index("00").build()

    DBContainer oolineRequest = oolineQuery.getContainer()
    oolineRequest.set("OBCONO", currentCompany)
    oolineRequest.set("OBORNO", orno)

    boolean rt = false
    Closure<?> oolineReader = { DBContainer oolineResult ->
      logger.debug("oolineResult ")
      int rorc = oolineResult.get("OBRORC") as Integer
      int ponr = oolineResult.get("OBPONR") as Integer
      int posx = oolineResult.get("OBPOSX") as Integer
      String xxasgd = oolineResult.get("OBUCA0") as String
      if (rorc == 0) {
        mi.outData.put("PONR",  ponr + "")
        mi.outData.put("POSX",  posx + "")
        mi.outData.put("ASGD",  xxasgd)
        mi.write()
        rt = true
      } else {
        getPurchaseLines(orno, ponr, posx, suno, xxasgd)
      }
    }

    if (!oolineQuery.readAll(oolineRequest, 2, nbMaxRecord, oolineReader)) {
    }
    if (!rt) {
      mi.error("Ligne d'ordre d'achat oa :${orno} assortiment:${asgd} fournisseur:${suno} n'existe pas")
      return
    }
  }
  /**
   *
   * @param orno
   * @param ponr
   * @param posx
   * @param suno
   */
  public boolean getPurchaseLines(String orno, int ponr, int posx, String suno, String asgd) {
    boolean rt = false

    DBAction mpoplpQuery = database.table("MPOPLP")
      .selection(
        "POCONO"
        ,"PORORN"
        ,"PORORL"
        ,"PORORX"
        ,"POPLPN"
        ,"POPLPS"
        ,"POPLP2"
        ,"POSUNO"
      )
      .index("90").build()

    DBContainer mpoplpRequest = mpoplpQuery.getContainer()
    mpoplpRequest.set("POCONO", currentCompany)
    mpoplpRequest.set("PORORN", orno)
    mpoplpRequest.set("PORORL", ponr)
    mpoplpRequest.set("PORORX", posx)
    mpoplpRequest.set("PORORC", 3)

    Closure<?> mpoplpReader = { DBContainer mpoplpResult ->
      logger.debug("mpoplpResult ")
      String xxsuno = mpoplpResult.get("POSUNO") as String
      if (suno.length() == 0 || xxsuno.trim().equals(suno)) {
        mi.outData.put("PONR",  ponr + "")
        mi.outData.put("POSX",  posx + "")
        mi.outData.put("ORCA",  "250")
        mi.outData.put("RIDN",  mpoplpResult.get("POPLPN") as String)
        mi.outData.put("RIDL",  mpoplpResult.get("POPLPS") as String)
        mi.outData.put("RIDX",  mpoplpResult.get("POPLP2") as String)
        mi.outData.put("SUNO",  mpoplpResult.get("POSUNO") as String)
        mi.outData.put("ASGD",  asgd)
        mi.write()
        rt = true
      }
    }

    if (!mpoplpQuery.readAll(mpoplpRequest, 5, nbMaxRecord, mpoplpReader)) {
    }

    DBAction mplineQuery = database.table("MPLINE")
      .selection(
        "IBCONO"
        ,"IBRORN"
        ,"IBRORL"
        ,"IBRORX"
        ,"IBPUNO"
        ,"IBSUNO"
        ,"IBPNLI"
        ,"IBPNLS"
      )
      .index("20").build()

    DBContainer mplineRequest = mplineQuery.getContainer()
    mplineRequest.set("IBCONO", currentCompany)
    mplineRequest.set("IBRORC", 3)
    mplineRequest.set("IBRORN", orno)
    mplineRequest.set("IBRORL", ponr)
    mplineRequest.set("IBRORX", posx)

    Closure<?> mplineReader = { DBContainer mplineResult ->
      logger.debug("mplineResult ")
      String xxsuno = mplineResult.get("IBSUNO") as String
      if (suno.length() == 0 || xxsuno.trim().equals(suno)) {
        mi.outData.put("PONR",  ponr + "")
        mi.outData.put("POSX",  posx + "")
        mi.outData.put("ORCA",  "251")
        mi.outData.put("RIDN",  mplineResult.get("IBPUNO") as String)
        mi.outData.put("RIDL",  mplineResult.get("IBPNLI") as String)
        mi.outData.put("RIDX",  mplineResult.get("IBPNLS") as String)
        mi.outData.put("SUNO",  mplineResult.get("IBSUNO") as String)
        mi.outData.put("ASGD",  asgd)
        mi.write()
        rt = true
      }
    }

    if (!mplineQuery.readAll(mplineRequest, 5, nbMaxRecord, mplineReader)) {
    }
    return rt
  }
}
