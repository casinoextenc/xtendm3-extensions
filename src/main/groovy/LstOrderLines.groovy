/**
 * README
 * This extension is used by MEC
 *
 * Name : EXT099MI.GetPOLineSIGMA6
 * Description : INT-LOG1203 find OOLINE by POPN and CUNO
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

  public LstOrderLines(LoggerAPI logger, MIAPI mi, DatabaseAPI database, ProgramAPI program, MICallerAPI miCaller, UtilityAPI utility) {
    this.logger = logger
    this.mi = mi
    this.database = database
    this.program = program
    this.miCaller = miCaller
    this.utility = utility
  }

  /**
   * 
   */
  public void main() {
    logger.debug("main start")
    currentCompany = (Integer)program.getLDAZD().CONO
    String orno = (String)(mi.in.get("ORNO") != null ? mi.in.get("ORNO") : "")
    String asgd = (String)(mi.in.get("ASGD") != null ? mi.in.get("ASGD") : "")
    String suno = (String)(mi.in.get("SUNO") != null ? mi.in.get("SUNO") : "")

    ExpressionFactory OOLINE_expression = database.getExpressionFactory("OOLINE")
    OOLINE_expression = OOLINE_expression.lt("OBORST", "66")
    if (asgd.length() > 0)
      OOLINE_expression = OOLINE_expression.and(OOLINE_expression.eq("OBUCA0", asgd))

    DBAction OOLINE_query = database.table("OOLINE")
        .matching(OOLINE_expression)
        .selection(
        "OBPONR"
        ,"OBRORC"
        ,"OBRORN"
        ,"OBRORL"
        ,"OBRORX"
        ,"OBUCA0"
        )
        .index("00").build()

    DBContainer OOLINE_request = OOLINE_query.getContainer()
    OOLINE_request.set("OBCONO", currentCompany)
    OOLINE_request.set("OBORNO", orno)

    boolean rt = false
    Closure<?> OOLINE_reader = { DBContainer OOLINE_result ->
      logger.debug("OOLINE_result ")
      int rorc = OOLINE_result.get("OBRORC") as Integer
      int ponr = OOLINE_result.get("OBPONR") as Integer
      int posx = OOLINE_result.get("OBPOSX") as Integer
      String xxasgd = OOLINE_result.get("OBUCA0") as String
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

    if (!OOLINE_query.readAll(OOLINE_request, 2, OOLINE_reader)) {
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

    DBAction MPOPLP_query = database.table("MPOPLP")
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

    DBContainer MPOPLP_request = MPOPLP_query.getContainer()
    MPOPLP_request.set("POCONO", currentCompany)
    MPOPLP_request.set("PORORN", orno)
    MPOPLP_request.set("PORORL", ponr)
    MPOPLP_request.set("PORORX", posx)
    MPOPLP_request.set("PORORC", 3)

    Closure<?> MPOPLP_reader = { DBContainer MPOPLP_result ->
      logger.debug("MPOPLP_result ")
      String xxsuno = MPOPLP_result.get("POSUNO") as String
      if (suno.length() == 0 || xxsuno.trim().equals(suno)) {
        mi.outData.put("PONR",  ponr + "")
        mi.outData.put("POSX",  posx + "")
        mi.outData.put("ORCA",  "250")
        mi.outData.put("RIDN",  MPOPLP_result.get("POPLPN") as String)
        mi.outData.put("RIDL",  MPOPLP_result.get("POPLPS") as String)
        mi.outData.put("RIDX",  MPOPLP_result.get("POPLP2") as String)
        mi.outData.put("SUNO",  MPOPLP_result.get("POSUNO") as String)
        mi.outData.put("ASGD",  asgd)
        mi.write()
        rt = true
      }
    }

    if (!MPOPLP_query.readAll(MPOPLP_request, 5, MPOPLP_reader)) {
    }

    DBAction MPLINE_query = database.table("MPLINE")
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

    DBContainer MPLINE_request = MPLINE_query.getContainer()
    MPLINE_request.set("IBCONO", currentCompany)
    MPLINE_request.set("IBRORC", 3)
    MPLINE_request.set("IBRORN", orno)
    MPLINE_request.set("IBRORL", ponr)
    MPLINE_request.set("IBRORX", posx)

    Closure<?> MPLINE_reader = { DBContainer MPLINE_result ->
      logger.debug("MPLINE_result ")
      String xxsuno = MPLINE_result.get("IBSUNO") as String
      if (suno.length() == 0 || xxsuno.trim().equals(suno)) {
        mi.outData.put("PONR",  ponr + "")
        mi.outData.put("POSX",  posx + "")
        mi.outData.put("ORCA",  "251")
        mi.outData.put("RIDN",  MPLINE_result.get("IBPUNO") as String)
        mi.outData.put("RIDL",  MPLINE_result.get("IBPNLI") as String)
        mi.outData.put("RIDX",  MPLINE_result.get("IBPNLS") as String)
        mi.outData.put("SUNO",  MPLINE_result.get("IBSUNO") as String)
        mi.outData.put("ASGD",  asgd)
        mi.write()
        rt = true
      }
    }

    if (!MPLINE_query.readAll(MPLINE_request, 5, MPLINE_reader)) {
    }
    return rt
  }
}