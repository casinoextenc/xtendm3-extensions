/****************************************************************************************
 Extension Name: EXT099MI.GetPOLineSIGMA6
 Type: ExtendM3Transaction
 Script Author: FLEBARS
 Date: 2023-08-22
 Description:
 * Find MPLINE by POPN and CUNO

 Revision History:
 Name         Date         Version   Description of Changes
 FLEBARS      2023-08-22   1.0       REF001 - Reference data interfaces Creation
 ARENARD      2025-04-22   1.1       Code has been checked
 ******************************************************************************************/

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
  private Integer nbMaxRecord = 10000

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

    ExpressionFactory mplineExpression = database.getExpressionFactory("MPLINE")
    mplineExpression = mplineExpression.like("IBITNO", popn + "%")
    mplineExpression = mplineExpression.and(mplineExpression.gt("IBPUSL", "15"))
    mplineExpression = mplineExpression.and(mplineExpression.lt("IBPUSL", "40"))
    if (cuno.length() == 0) {
      mplineExpression = mplineExpression.and(mplineExpression.eq("IBRORC", "0"))
    } else {
      mplineExpression = mplineExpression.and(mplineExpression.eq("IBRORC", "3"))
    }


    DBAction mplineQuery = database.table("MPLINE")
      .matching(mplineExpression)
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
    DBContainer mplineRequest = mplineQuery.getContainer()
    mplineRequest.set("IBCONO", currentCompany)
    mplineRequest.set("IBPUNO", puno)

    boolean rt = (cuno.length() == 0)
    Closure<?> mplineReader = { DBContainer mplineResult ->
      logger.debug("mplineResult ")

      if (!rt) {
        String orno = mplineResult.get("IBRORN") as String
        rt = (cuno.equals(getCustomerFromOrder(orno)))
      }
      logger.debug("mplineResult ${rt}")
      if (rt) {
        mi.outData.put("PUNO",  mplineResult.get("IBPUNO") as String)
        mi.outData.put("PNLI",  mplineResult.get("IBPNLI") as String)
        mi.outData.put("PNLS",  mplineResult.get("IBPNLS") as String)
        mi.outData.put("ITNO",  mplineResult.get("IBITNO") as String)
        mi.outData.put("ORQA",  mplineResult.get("IBORQA") as String)
        mi.write()
      }
    }

    if (!mplineQuery.readAll(mplineRequest, 2, nbMaxRecord, mplineReader)) {
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
