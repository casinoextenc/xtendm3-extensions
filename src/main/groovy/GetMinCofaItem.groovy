/**
 * Name: EXT099MI.GetPOLineSIGMA6
 * Migration projet GIT
 * old file = EXT099MI_GetPOLineSIGMA6.groovy
 */

/**
 * README
 * This extension is used by MEC
 *
 * Name : EXT099MI.GetMinCofaItem
 * Description : INT-COM04 find ITNO (SIGMA9) avec le plus petit COFA by POPN (SIGMA6)
 * Date         Changed By    Description
 * 20231219    PBEAUDOUIN     COM04 Creation

 */
public class GetMinCofaItem extends ExtendM3Transaction {
  private final MIAPI mi
  private final LoggerAPI logger
  private final ProgramAPI program
  private final DatabaseAPI database
  private final SessionAPI session
  private final TransactionAPI transaction
  private final MICallerAPI miCaller
  private final UtilityAPI utility

  private int currentCompany

  public GetMinCofaItem(LoggerAPI logger, MIAPI mi, DatabaseAPI database, ProgramAPI program, MICallerAPI miCaller, UtilityAPI utility) {
    this.logger = logger
    this.mi = mi
    this.database = database
    this.program = program
    this.miCaller = miCaller
    this.utility = utility
  }

  public void main() {
    logger.debug("main start")
    currentCompany = (Integer) program.getLDAZD().CONO
    String popn = (String) (mi.in.get("POPN") != null ? mi.in.get("POPN") : "")

    ExpressionFactory MITPOP_expression = database.getExpressionFactory("MITPOP")
    MITPOP_expression = MITPOP_expression.eq("MPREMK", "SIGMA6")


    DBAction MITPOP_query = database.table("MITPOP")
      .matching(MITPOP_expression)
      .selection(
        "MPITNO"
      )
      .index("10").build()
    DBContainer MITPOP_request = MITPOP_query.getContainer()
    MITPOP_request.set("MPCONO", currentCompany)
    MITPOP_request.set("MPALWT", 1)
    MITPOP_request.set("MPALWQ", "")
    MITPOP_request.set("MPPOPN", popn)


    boolean rt = false
    double currentCofa = 0
    String currentItno = ""

    Closure<?> MITPOP_reader = { DBContainer MITPOP_result ->
      rt = true
      double cofa = getCofa(MITPOP_result.get("MPITNO") as String)
      if (cofa < currentCofa || currentCofa == 0) {
        currentItno = MITPOP_result.get("MPITNO") as String
        currentCofa = cofa
      }
    }

    if (!MITPOP_query.readAll(MITPOP_request, 4, MITPOP_reader)) {
    }
    if (rt) {
      mi.outData.put("ITNO", currentItno)
      mi.outData.put("COFA", String.valueOf(currentCofa ))
      mi.write()
    }


    if (!rt) {
      mi.error("Sigma 6 ${popn}  n'existe pas")
      return
    }
  }

  public double getCofa(String itno) {
    String popn = (String) (mi.in.get("POPN") != null ? mi.in.get("POPN") : "")

    ExpressionFactory MITAUN_expression = database.getExpressionFactory("MITAUN")
    MITAUN_expression = MITAUN_expression.eq("MUAUS2", "1")


    DBAction MITAUN_query = database.table("MITAUN")
      .matching(MITAUN_expression)
      .selection(
        "MUCOFA"
      )
      .index("40").build()
    DBContainer MITAUN_request = MITAUN_query.getContainer()
    MITAUN_request.set("MUCONO", currentCompany)
    MITAUN_request.set("MUITNO", itno)
    MITAUN_request.set("MUAUTP", 1)
    MITAUN_request.set("MUDMCF", 1)
    double cofa = 0
    Closure<?> MITAUN_reader = { DBContainer MITAUN_result ->

      cofa = MITAUN_result.get("MUCOFA") as Double

    }
    if (!MITAUN_query.readAll(MITAUN_request, 4, MITAUN_reader)) {

    }

    return cofa
  }

}

