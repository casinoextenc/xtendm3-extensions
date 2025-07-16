/****************************************************************************************
 Extension Name: EXT099MI.GetMinCofaItem
 Type: ExtendM3Transaction
 Script Author: PBEAUDOUIN
 Date: 2023-12-19
 Description:
 * Find ITNO (SIGMA9) avec le plus petit COFA by POPN (SIGMA6)

 Revision History:
 Name         Date         Version   Description of Changes
 PBEAUDOUIN   2023-12-19   1.0       REF001 - Reference data interfaces Creation
 ARENARD      2025-04-22   1.1       Code has been checked
 ******************************************************************************************/

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
  private Integer nbMaxRecord = 10000

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

    ExpressionFactory mitpopExpression = database.getExpressionFactory("MITPOP")
    mitpopExpression = mitpopExpression.eq("MPREMK", "SIGMA6")


    DBAction mitpopQuery = database.table("MITPOP")
      .matching(mitpopExpression)
      .selection(
        "MPITNO"
      )
      .index("10").build()
    DBContainer mitpopRequest = mitpopQuery.getContainer()
    mitpopRequest.set("MPCONO", currentCompany)
    mitpopRequest.set("MPALWT", 1)
    mitpopRequest.set("MPALWQ", "")
    mitpopRequest.set("MPPOPN", popn)


    boolean rt = false
    double currentCofa = 0
    String currentItno = ""

    Closure<?> mitpopReader = { DBContainer mitpopResult ->
      rt = true
      double cofa = getCofa(mitpopResult.get("MPITNO") as String)
      if (cofa < currentCofa || currentCofa == 0) {
        currentItno = mitpopResult.get("MPITNO") as String
        currentCofa = cofa
      }
    }

    if (!mitpopQuery.readAll(mitpopRequest, 4, nbMaxRecord, mitpopReader)) {
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

  /**
   * Get the COFA of an item
   * @param itno the item number
   * @return the COFA of the item
   */
  public double getCofa(String itno) {
    String popn = (String) (mi.in.get("POPN") != null ? mi.in.get("POPN") : "")

    ExpressionFactory mitaunExpression = database.getExpressionFactory("MITAUN")
    mitaunExpression = mitaunExpression.eq("MUAUS2", "1")


    DBAction mitaunQuery = database.table("MITAUN")
      .matching(mitaunExpression)
      .selection(
        "MUCOFA"
      )
      .index("40").build()
    DBContainer mitaunRequest = mitaunQuery.getContainer()
    mitaunRequest.set("MUCONO", currentCompany)
    mitaunRequest.set("MUITNO", itno)
    mitaunRequest.set("MUAUTP", 1)
    mitaunRequest.set("MUDMCF", 1)
    double cofa = 0
    Closure<?> mitaunReader = { DBContainer mitaunResult ->

      cofa = mitaunResult.get("MUCOFA") as Double

    }
    if (!mitaunQuery.readAll(mitaunRequest, 4, nbMaxRecord, mitaunReader)) {

    }

    return cofa
  }

}

