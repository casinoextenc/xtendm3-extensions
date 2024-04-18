/**
 * README
 * This extension is used by MEC
 *
 * Name : EXT099MI.GetPOLineSIGMA6
 * Description : INT-LOG1203 find MPLINE by POPN and CUNO
 * Date         Changed By    Description
 * 20230822     FLEBARS       LOG1203 Creation
 */
public class GetPOLineSIGMA6 extends ExtendM3Transaction {
  private final MIAPI mi
  private final LoggerAPI logger
  private final ProgramAPI program
  private final DatabaseAPI database
  private final SessionAPI session
  private final TransactionAPI transaction
  private final MICallerAPI miCaller
  private final UtilityAPI utility

  private int currentCompany

  public GetPOLineSIGMA6(LoggerAPI logger, MIAPI mi, DatabaseAPI database, ProgramAPI program, MICallerAPI miCaller, UtilityAPI utility) {
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
    String puno = (String)(mi.in.get("PUNO") != null ? mi.in.get("PUNO") : "")
    String popn = (String)(mi.in.get("POPN") != null ? mi.in.get("POPN") : "")
    String cuno = (String)(mi.in.get("CUNO") != null ? mi.in.get("CUNO") : "")
    String suno = (String)(mi.in.get("SUNO") != null ? mi.in.get("SUNO") : "")
    String dlqt = (String)(mi.in.get("DLQT") != null ? mi.in.get("DLQT") : "")

    ExpressionFactory MPLINE_expression = database.getExpressionFactory("MPLINE")
    MPLINE_expression = MPLINE_expression.like("IBITNO", popn + "%")
    MPLINE_expression = MPLINE_expression.and(MPLINE_expression.gt("IBPUSL", "15"))
    MPLINE_expression = MPLINE_expression.and(MPLINE_expression.lt("IBPUSL", "40"))
    if (cuno.length() == 0) {
      MPLINE_expression = MPLINE_expression.and(MPLINE_expression.eq("IBRORC", "0"))
    } else {
      MPLINE_expression = MPLINE_expression.and(MPLINE_expression.eq("IBRORC", "3"))
    }


    DBAction MPLINE_query = database.table("MPLINE")
        .matching(MPLINE_expression)
        .selection(
        "IBPUNO"
        ,"IBPNLI"
        ,"IBPNLS"
        ,"IBPUSL"
        ,"IBPUST"
        ,"IBITNO"
        ,"IBRORC"
        ,"IBRORN"
        ,"IBRORL"
        ,"IBRORX"
        ,"IBORQA"
        )
        .index("00").build()
    DBContainer MPLINE_request = MPLINE_query.getContainer()
    MPLINE_request.set("IBCONO", currentCompany)
    MPLINE_request.set("IBPUNO", puno)

    boolean rt = (cuno.length() == 0)
    Closure<?> MPLINE_reader = { DBContainer MPLINE_result ->
      logger.debug("MPLINE_result ")
      
      if (!rt) {
        String orno = MPLINE_result.get("IBRORN") as String
        rt = (cuno.equals(getCustomerFromOrder(orno)))
      }
      logger.debug("MPLINE_result ${rt}")
      if (rt) {
        mi.outData.put("PUNO",  MPLINE_result.get("IBPUNO") as String)
        mi.outData.put("PNLI",  MPLINE_result.get("IBPNLI") as String)
        mi.outData.put("PNLS",  MPLINE_result.get("IBPNLS") as String)
        mi.outData.put("ITNO",  MPLINE_result.get("IBITNO") as String)
        mi.outData.put("ORQA",  MPLINE_result.get("IBORQA") as String)
        mi.write()
      }
    }

    if (!MPLINE_query.readAll(MPLINE_request, 2, MPLINE_reader)) {
    }
    if (!rt) {
      mi.error("Ligne d'ordre d'achat oA:${puno} sigma6:${popn} client:${cuno} n'existe pas")
      return
    }
  }

  /**
   * 
   * @param orno
   * @return customer number for order
   */
  public String getCustomerFromOrder(String orno) {
    //Check if record exists
    DBAction queryOOHEAD = database.table("OOHEAD")
        .index("00")
        .selection(
        "OACONO"
        ,"OAORNO"
        ,"OACUNO"
        )
        .build()

    DBContainer containerOOHEAD = queryOOHEAD.getContainer()
    containerOOHEAD.set("OACONO", currentCompany)
    containerOOHEAD.set("OAORNO", orno)

    // not Record exists
    if (queryOOHEAD.read(containerOOHEAD)) {
      String cuno = containerOOHEAD.get("OACUNO") as String
      logger.debug("CUNO ${cuno} r" )
      return cuno.trim()
    }
    return null
  }
}